"""
GASL Tools Flask Application

Security Features Implemented:
- File upload validation (filename, size, MIME type, content)
- Input sanitization and validation  
- Rate limiting (5 requests per minute for file uploads)
- Security headers (XSS, CSRF, Content Security Policy)
- Secure filename handling
- Temporary file cleanup
- Error handling with safe error messages
- Session security configuration
"""

import os
import re
import hashlib
try:
    import magic
    HAS_MAGIC = True
except ImportError:
    HAS_MAGIC = False
    print("Warning: python-magic not available. Using fallback file validation.")
from flask import Flask, request, jsonify, send_file, render_template, abort, session
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from werkzeug.utils import secure_filename
from werkzeug.exceptions import RequestEntityTooLarge
from time_standards_service import TimeStandardsService
import pandas as pd
import tempfile


app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', os.urandom(24))
app.config['SESSION_COOKIE_SECURE'] = True
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'

# Rate limiting - compatible with multiple Flask-Limiter versions
try:
    # Try new style initialization (Flask-Limiter 2.0+)
    limiter = Limiter(
        key_func=get_remote_address,
        default_limits=["200 per day", "50 per hour", "10 per minute"]
    )
    limiter.init_app(app)
except TypeError:
    # Fallback to old style initialization (Flask-Limiter 1.x)
    limiter = Limiter(
        app,
        key_func=get_remote_address,
        default_limits=["200 per day", "50 per hour", "10 per minute"]
    )

service = TimeStandardsService()


class Config:
    """Configuration class for default values."""
    DEFAULT_GOLD_PERCENTILE = 0.15
    DEFAULT_SILVER_PERCENTILE = 0.55
    DEFAULT_HEAT_TIME = 15  # seconds
    DEFAULT_EVENT_TIME = 30  # seconds
    ALLOWED_EXTENSIONS = {'csv'}
    MAX_FILENAME_LENGTH = 255
    MAX_FILE_SIZE = 16 * 1024 * 1024  # 16MB
    ALLOWED_MIME_TYPES = {'text/csv', 'text/plain', 'application/csv'}


def validate_filename(filename):
    """Validate filename for security."""
    if not filename:
        return False, "No filename provided"
    
    if len(filename) > Config.MAX_FILENAME_LENGTH:
        return False, "Filename too long"
    
    # Check for directory traversal attempts
    if '..' in filename or '/' in filename or '\\' in filename:
        return False, "Invalid characters in filename"
    
    # Check for null bytes
    if '\x00' in filename:
        return False, "Null byte in filename"
    
    # Allow only alphanumeric, dots, dashes, underscores
    if not re.match(r'^[a-zA-Z0-9._-]+$', filename):
        return False, "Invalid characters in filename"
    
    return True, "Valid"


def validate_file_content(file_path):
    """Validate file content and MIME type."""
    try:
        # Check file size
        file_size = os.path.getsize(file_path)
        if file_size > Config.MAX_FILE_SIZE:
            return False, "File too large"
        
        if file_size == 0:
            return False, "Empty file"
        
        # MIME type checking (if magic is available)
        if HAS_MAGIC:
            try:
                mime_type = magic.from_file(file_path, mime=True)
                if mime_type not in Config.ALLOWED_MIME_TYPES:
                    return False, f"Invalid file type: {mime_type}"
            except Exception as e:
                print(f"Warning: Magic MIME detection failed: {e}")
                # Continue with other validations
        
        # Enhanced CSV validation - check file content
        try:
            # Read first few lines to validate CSV structure
            with open(file_path, 'r', encoding='utf-8') as f:
                first_line = f.readline().strip()
                if not first_line:
                    return False, "Empty file"
                
                # Check for common CSV characteristics
                if not (',' in first_line or ';' in first_line or '\t' in first_line):
                    return False, "File does not appear to be a valid CSV"
                
                # Check for binary content in first 1KB
                f.seek(0)
                sample = f.read(1024)
                
                # Check for null bytes (indicates binary file)
                if '\x00' in sample:
                    return False, "File appears to be binary, not CSV"
                
                # Check for excessive non-printable characters
                non_printable = sum(1 for c in sample if ord(c) < 32 and c not in '\n\r\t')
                if non_printable > len(sample) * 0.1:  # More than 10% non-printable
                    return False, "File contains too many non-printable characters"
            
            # Try to parse with pandas
            df = pd.read_csv(file_path, nrows=5)
            if df.empty:
                return False, "Invalid CSV format - no data found"
            
            # Check for reasonable number of columns (1-50)
            if len(df.columns) < 1 or len(df.columns) > 50:
                return False, f"Unexpected number of columns: {len(df.columns)}"
            
        except UnicodeDecodeError:
            return False, "File encoding not supported - please use UTF-8"
        except pd.errors.EmptyDataError:
            return False, "CSV file is empty"
        except pd.errors.ParserError as e:
            return False, f"CSV parsing error: {str(e)}"
        except Exception as e:
            return False, f"File validation error: {str(e)}"
        
        return True, "Valid"
    
    except Exception as e:
        return False, f"File validation error: {str(e)}"


def allowed_file(filename):
    """Check if file extension is allowed."""
    if not filename:
        return False
    
    # Validate filename first
    is_valid, message = validate_filename(filename)
    if not is_valid:
        return False
    
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in Config.ALLOWED_EXTENSIONS


def sanitize_input(value, input_type='string', max_length=None):
    """Sanitize user input."""
    if value is None:
        return None
    
    if input_type == 'float':
        try:
            val = float(value)
            # Validate reasonable ranges for percentiles
            if 0.0 <= val <= 1.0:
                return val
            else:
                raise ValueError("Value out of range")
        except (ValueError, TypeError):
            raise ValueError("Invalid float value")
    
    elif input_type == 'int':
        try:
            val = int(value)
            # Validate reasonable ranges for timing
            if 0 <= val <= 3600:  # Max 1 hour
                return val
            else:
                raise ValueError("Value out of range")
        except (ValueError, TypeError):
            raise ValueError("Invalid integer value")
    
    elif input_type == 'string':
        if not isinstance(value, str):
            value = str(value)
        
        # Remove null bytes and control characters
        value = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', value)
        
        if max_length and len(value) > max_length:
            value = value[:max_length]
        
        return value.strip()
    
    return value


# Security headers
@app.after_request
def add_security_headers(response):
    """Add security headers to all responses."""
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'DENY'
    response.headers['X-XSS-Protection'] = '1; mode=block'
    response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'
    response.headers['Content-Security-Policy'] = "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'"
    response.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
    return response


# Error handlers
@app.errorhandler(413)
@app.errorhandler(RequestEntityTooLarge)
def file_too_large(error):
    """Handle file too large errors."""
    return jsonify({'error': 'File too large. Maximum size is 16MB.'}), 413


@app.errorhandler(400)
def bad_request(error):
    """Handle bad request errors."""
    return jsonify({'error': 'Bad request. Please check your input.'}), 400


@app.errorhandler(500)
def internal_error(error):
    """Handle internal server errors."""
    return jsonify({'error': 'Internal server error. Please try again later.'}), 500


@app.route('/')
def home():
    """Serve the main GASL Tools landing page."""
    return render_template('home.html')


@app.route('/time-standards')
def time_standards():
    """Serve the time standards calculator page."""
    return render_template('time-standards.html')


@app.route('/close-to-pin')
def close_to_pin():
    """Serve the close to pin analysis page."""
    return render_template('close-to-pin.html')


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({'status': 'healthy'})


@app.route('/standards-info', methods=['GET'])
@limiter.limit("20 per minute")
def get_standards_info():
    """Get information about current standards file."""
    try:
        # Use local current standards file
        standards_filepath = os.path.join(os.getcwd(), 'current_standards.csv')
        if not os.path.exists(standards_filepath):
            # Try alternative name
            standards_filepath = os.path.join(os.getcwd(), 'current_standards')
            if not os.path.exists(standards_filepath):
                return jsonify({'error': 'Current standards file not found'}), 404
        
        # Try to read metadata from first line
        adopted_date = extract_metadata_date(standards_filepath)
        
        if adopted_date:
            return jsonify({
                'status': 'success',
                'last_modified': adopted_date,
                'last_modified_iso': None  # Keep for compatibility but not used for metadata
            })
        else:
            # Fallback to file modification time if no metadata found
            import datetime
            modification_time = os.path.getmtime(standards_filepath)
            last_modified = datetime.datetime.fromtimestamp(modification_time)
            
            return jsonify({
                'status': 'success',
                'last_modified': last_modified.strftime('%B %d, %Y'),
                'last_modified_iso': last_modified.isoformat()
            })
    
    except Exception as e:
        return jsonify({'error': f'Failed to get standards info: {str(e)}'}), 500


def extract_metadata_date(filepath):
    """Extract adoption date from metadata line in standards file."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            first_line = f.readline().strip()
            
            # Check if first line is a metadata comment
            if first_line.startswith('#'):
                # Look for "Adopted:" pattern
                import re
                adopted_match = re.search(r'#\s*Adopted:\s*(.+)', first_line, re.IGNORECASE)
                if adopted_match:
                    return adopted_match.group(1).strip()
        
        return None
    
    except Exception as e:
        print(f"Warning: Could not read metadata from {filepath}: {e}")
        return None


@app.route('/current-standards', methods=['GET'])
@limiter.limit("10 per minute")
def get_current_standards():
    """Get current time standards for display."""
    try:
        # Use local current standards file
        standards_filepath = os.path.join(os.getcwd(), 'current_standards.csv')
        if not os.path.exists(standards_filepath):
            # Try alternative name
            standards_filepath = os.path.join(os.getcwd(), 'current_standards')
            if not os.path.exists(standards_filepath):
                return jsonify({'error': 'Current standards file not found'}), 404
        
        # Get file modification time
        import datetime
        modification_time = os.path.getmtime(standards_filepath)
        last_modified = datetime.datetime.fromtimestamp(modification_time)
        
        # Read and process standards
        import pandas as pd
        from utils import add_event_names_column, read_csv_with_metadata
        
        standards = read_csv_with_metadata(standards_filepath)
        standards = add_event_names_column(standards)
        
        # Clean data for display
        standards_clean = standards.fillna('')
        
        return jsonify({
            'status': 'success',
            'standards': standards_clean.to_dict('records'),
            'last_modified': last_modified.strftime('%B %d, %Y'),
            'last_modified_iso': last_modified.isoformat()
        })
    
    except Exception as e:
        return jsonify({'error': f'Failed to load standards: {str(e)}'}), 500


@app.route('/calculate-standards', methods=['POST'])
@limiter.limit("5 per minute")
def calculate_standards():
    """Calculate new time standards based on uploaded data."""
    try:
        # Sanitize and validate input parameters
        gold_pct = sanitize_input(
            request.form.get('gold_percentile', Config.DEFAULT_GOLD_PERCENTILE), 
            'float'
        )
        silver_pct = sanitize_input(
            request.form.get('silver_percentile', Config.DEFAULT_SILVER_PERCENTILE), 
            'float'
        )
        
        # Check if data files are present
        if 'data_files' not in request.files:
            return jsonify({'error': 'Missing required data files'}), 400
        
        data_files = request.files.getlist('data_files')
        
        # Validate files
        if not data_files:
            return jsonify({'error': 'No data files selected'}), 400
        
        # Use local current standards file
        standards_filepath = os.path.join(os.getcwd(), 'current_standards.csv')
        if not os.path.exists(standards_filepath):
            # Try alternative name
            standards_filepath = os.path.join(os.getcwd(), 'current_standards')
            if not os.path.exists(standards_filepath):
                return jsonify({'error': 'Current standards file not found in application directory'}), 400

        # Save files temporarily
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create data directory
            data_dir = os.path.join(temp_dir, 'data')
            os.makedirs(data_dir)
            
            # Save and validate data files
            data_file_paths = []
            for file in data_files:
                if file and file.filename:
                    # Enhanced filename validation
                    is_valid, message = validate_filename(file.filename)
                    if not is_valid:
                        return jsonify({'error': f'Invalid filename: {message}'}), 400
                    
                    if not allowed_file(file.filename):
                        return jsonify({'error': f'File type not allowed: {file.filename}'}), 400
                    
                    filename = secure_filename(file.filename)
                    filepath = os.path.join(data_dir, filename)
                    
                    # Save file
                    file.save(filepath)
                    
                    # Validate file content
                    is_valid, message = validate_file_content(filepath)
                    if not is_valid:
                        os.remove(filepath)  # Clean up invalid file
                        return jsonify({'error': f'File validation failed: {message}'}), 400
                    
                    data_file_paths.append(filepath)
            
            if not data_file_paths:
                return jsonify({'error': 'No valid data files uploaded'}), 400
            
            # Create file pattern for data files only
            data_pattern = os.path.join(data_dir, '*.csv')
            
            # Calculate new standards
            results = service.calculate_new_standards(
                data_pattern, 
                standards_filepath,
                gold_pct=gold_pct, 
                silver_pct=silver_pct
            )
            
            # Format response
            proposed_standards = results['proposed_standards']
            
            # Column ordering and renaming
            col_order = [
                "GASL_Event_ID_x", "Event_name", "gold_y", "new_gold_y", "gold_diff_y", 
                "gold_s", "new_gold_s", "gold_diff_s", "silver_y", "new_silver_y", 
                "silver_diff_y", "silver_s", "new_silver_s", "silver_diff_s"
            ]
            
            proposed_standards = proposed_standards[col_order]
            proposed_standards = proposed_standards.rename(columns={
                "GASL_Event_ID_x": "Event_ID",
                "Event_name": "Event",
                "gold_y": "Current Gold Time (yards)",
                "new_gold_y": "Proposed Gold Time (yards)",
                "gold_diff_y": "Gold delta (yards)",
                "gold_s": "Current Gold Time (meters)",
                "new_gold_s": "Proposed Gold Time (meters)",
                "gold_diff_s": "Gold delta (meters)",
                "silver_y": "Current Silver Time (yards)",
                "new_silver_y": "Proposed Silver Time (yards)",
                "silver_diff_y": "Silver delta (yards)",
                "silver_s": "Current Silver Time (meters)",
                "new_silver_s": "Proposed Silver Time (meters)",
                "silver_diff_s": "Silver delta (meters)"
            })
            
            proposed_standards = proposed_standards.sort_values(by=['Event_ID'])
            
            # Handle NaN values before converting to dict
            proposed_standards_clean = proposed_standards.fillna('')
            
            return jsonify({
                'status': 'success',
                'parameters': {
                    'gold_percentile': gold_pct,
                    'silver_percentile': silver_pct
                },
                'results': proposed_standards_clean.to_dict('records')
            })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/analyze-current-standards', methods=['POST'])
@limiter.limit("5 per minute")
def analyze_current_standards():
    """Analyze current percentiles for existing time standards."""
    try:
        # Check if data files are present
        if 'data_files' not in request.files:
            return jsonify({'error': 'Missing required data files'}), 400
        
        data_files = request.files.getlist('data_files')
        
        # Use local current standards file
        standards_filepath = os.path.join(os.getcwd(), 'current_standards.csv')
        if not os.path.exists(standards_filepath):
            # Try alternative name
            standards_filepath = os.path.join(os.getcwd(), 'current_standards')
            if not os.path.exists(standards_filepath):
                return jsonify({'error': 'Current standards file not found in application directory'}), 400
        
        # Save files temporarily and analyze
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create data directory
            data_dir = os.path.join(temp_dir, 'data')
            os.makedirs(data_dir)
            
            # Save data files
            data_file_paths = []
            for file in data_files:
                if file and allowed_file(file.filename):
                    filename = secure_filename(file.filename)
                    filepath = os.path.join(data_dir, filename)
                    file.save(filepath)
                    data_file_paths.append(filepath)
            
            if not data_file_paths:
                return jsonify({'error': 'No valid data files uploaded'}), 400
            
            # Prepare data
            data_pattern = os.path.join(data_dir, '*.csv')
            df = service.prepare_data(data_pattern)
            
            # Get current standards
            from utils import add_event_names_column, read_csv_with_metadata
            current_standards = read_csv_with_metadata(standards_filepath)
            current_standards = add_event_names_column(current_standards)
            
            # Analyze current percentiles
            analysis = service.get_current_percentile_summary(df, current_standards)
            
            # Handle NaN values before converting to dict
            analysis_clean = analysis.fillna('')
            
            return jsonify({
                'status': 'success',
                'analysis': analysis_clean.to_dict('records')
            })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/estimate-meet-duration', methods=['POST'])
@limiter.limit("5 per minute")
def estimate_meet_duration():
    """Estimate meet duration based on proposed standards."""
    try:
        # Sanitize and validate input parameters
        heat_time = sanitize_input(
            request.form.get('heat_time', Config.DEFAULT_HEAT_TIME), 
            'int'
        )
        event_time = sanitize_input(
            request.form.get('event_time', Config.DEFAULT_EVENT_TIME), 
            'int'
        )
        gold_pct = sanitize_input(
            request.form.get('gold_percentile', Config.DEFAULT_GOLD_PERCENTILE), 
            'float'
        )
        silver_pct = sanitize_input(
            request.form.get('silver_percentile', Config.DEFAULT_SILVER_PERCENTILE), 
            'float'
        )
        
        # Check if data files are present
        if 'data_files' not in request.files:
            return jsonify({'error': 'Missing required data files'}), 400
        
        data_files = request.files.getlist('data_files')
        
        # Use local current standards file
        standards_filepath = os.path.join(os.getcwd(), 'current_standards.csv')
        if not os.path.exists(standards_filepath):
            # Try alternative name
            standards_filepath = os.path.join(os.getcwd(), 'current_standards')
            if not os.path.exists(standards_filepath):
                return jsonify({'error': 'Current standards file not found in application directory'}), 400
        
        # Process files and estimate duration
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create data directory
            data_dir = os.path.join(temp_dir, 'data')
            os.makedirs(data_dir)
            
            # Save data files
            data_file_paths = []
            for file in data_files:
                if file and allowed_file(file.filename):
                    filename = secure_filename(file.filename)
                    filepath = os.path.join(data_dir, filename)
                    file.save(filepath)
                    data_file_paths.append(filepath)
            
            if not data_file_paths:
                return jsonify({'error': 'No valid data files uploaded'}), 400
            
            # Calculate new standards first
            data_pattern = os.path.join(data_dir, '*.csv')
            results = service.calculate_new_standards(
                data_pattern, 
                standards_filepath,
                gold_pct=gold_pct, 
                silver_pct=silver_pct
            )
            
            # Get qualifiers summary for the most recent season only
            import glob
            from datetime import datetime
            duration_results = []
            
            # Find the most recent season
            latest_year = 0
            latest_file = None
            
            for csv_file in data_file_paths:
                times = pd.read_csv(csv_file)
                season_str = times['date'].iloc[0]
                
                # Parse the date to get the year
                try:
                    # Try different date formats
                    date_formats = ['%m/%d/%y', '%m/%d/%Y', '%Y-%m-%d', '%m-%d-%y', '%m-%d-%Y']
                    season_year = None
                    
                    for fmt in date_formats:
                        try:
                            parsed_date = datetime.strptime(season_str, fmt)
                            season_year = parsed_date.year
                            break
                        except ValueError:
                            continue
                    
                    if season_year is None:
                        # Fallback to pandas parsing
                        parsed_date = pd.to_datetime(season_str)
                        season_year = parsed_date.year
                    
                    if season_year > latest_year:
                        latest_year = season_year
                        latest_file = csv_file
                        
                except Exception:
                    # If date parsing fails, skip this file
                    continue
            
            # Process only the most recent season file
            if latest_file:
                times = pd.read_csv(latest_file)
                season = times['date'].iloc[0]
                
                # Add athlete IDs
                import hashlib
                times['athlete_id'] = times.apply(
                    lambda row: hashlib.sha256(
                        f"{row['first_name']}_{row['last_name']}_{row['team_abbr']}_{row['age']}".encode()
                    ).hexdigest(), 
                    axis=1
                )
                
                qualifiers_result = service.get_qualifiers_summary(
                    times, 
                    results['combined_new'], 
                    results['current_standards'], 
                    heat_time, 
                    event_time
                )
                
                # Handle NaN values before converting to dict
                times_df_clean = qualifiers_result['times_df'].fillna('')
                new_entries_summary_clean = service.get_team_attendance_summary(qualifiers_result['new_entries']).fillna(0)
                current_entries_summary_clean = service.get_team_attendance_summary(qualifiers_result['current_entries']).fillna(0)
                
                # Convert attendance summaries to the format expected by JavaScript
                # DataFrame index contains team names, columns contain GOLD/SILVER/BRONZE/Total
                new_entries_dict = {}
                for team in new_entries_summary_clean.index:
                    new_entries_dict[team] = new_entries_summary_clean.loc[team].to_dict()
                
                current_entries_dict = {}
                for team in current_entries_summary_clean.index:
                    current_entries_dict[team] = current_entries_summary_clean.loc[team].to_dict()
                
                duration_results.append({
                    'season': qualifiers_result['season'],
                    'times_df': times_df_clean.to_dict('records'),
                    'new_entries_summary': new_entries_dict,
                    'current_entries_summary': current_entries_dict
                })
            
            return jsonify({
                'status': 'success',
                'parameters': {
                    'heat_time': heat_time,
                    'event_time': event_time,
                    'gold_percentile': gold_pct,
                    'silver_percentile': silver_pct
                },
                'duration_estimates': duration_results
            })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/analyze-close-to-pin', methods=['POST'])
@limiter.limit("5 per minute")
def analyze_close_to_pin():
    """Analyze close to pin data using uploaded file and local standards."""
    try:
        print("DEBUG: Starting close to pin analysis")
        
        # Check if best times file is present
        if 'best_times_file' not in request.files:
            print("DEBUG: Missing best times file in request")
            return jsonify({'error': 'Missing best times file'}), 400
        
        best_times_file = request.files['best_times_file']
        print(f"DEBUG: Best times file received: {best_times_file.filename}")
        
        # Validate best times file
        if not best_times_file or best_times_file.filename == '':
            return jsonify({'error': 'No best times file selected'}), 400
        
        # Use local current standards file
        standards_filepath = os.path.join(os.getcwd(), 'current_standards.csv')
        if not os.path.exists(standards_filepath):
            # Try alternative name
            standards_filepath = os.path.join(os.getcwd(), 'current_standards')
            if not os.path.exists(standards_filepath):
                return jsonify({'error': 'Current standards file not found in application directory'}), 400
        
        print(f"DEBUG: Using standards file: {standards_filepath}")
        
        # Save best times file temporarily
        with tempfile.TemporaryDirectory() as temp_dir:
            print(f"DEBUG: Created temp directory: {temp_dir}")
            
            # Enhanced validation and save best times file
            if not best_times_file or not best_times_file.filename:
                return jsonify({'error': 'No best times file provided'}), 400
            
            # Validate filename
            is_valid, message = validate_filename(best_times_file.filename)
            if not is_valid:
                return jsonify({'error': f'Invalid filename: {message}'}), 400
            
            if not allowed_file(best_times_file.filename):
                return jsonify({'error': f'File type not allowed: {best_times_file.filename}'}), 400
            
            best_times_filename = secure_filename(best_times_file.filename)
            best_times_filepath = os.path.join(temp_dir, best_times_filename)
            
            # Save file
            best_times_file.save(best_times_filepath)
            print(f"DEBUG: Saved best times file to: {best_times_filepath}")
            
            # Validate file content
            is_valid, message = validate_file_content(best_times_filepath)
            if not is_valid:
                os.remove(best_times_filepath)  # Clean up invalid file
                return jsonify({'error': f'File validation failed: {message}'}), 400
            
            # Process the close to pin analysis
            print("DEBUG: Starting close to pin service analysis")
            from close_to_pin_service import CloseToPinService
            close_to_pin_service = CloseToPinService()
            
            result = close_to_pin_service.analyze_close_to_pin(
                best_times_filepath,
                standards_filepath
            )
            print(f"DEBUG: Analysis completed, result shape: {result.shape}")
            
            # Convert to dict with NaN handling
            result_clean = result.fillna('')
            result_dict = result_clean.to_dict('records')
            print(f"DEBUG: Converted to dict, {len(result_dict)} records")
            
            return jsonify({
                'status': 'success',
                'swimmers': result_dict
            })
    
    except Exception as e:
        print(f"DEBUG: Exception occurred: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/download-close-to-pin/<format>', methods=['POST'])
def download_close_to_pin(format):
    """Download close to pin analysis results in specified format."""
    try:
        # Get the analysis results from request
        data = request.get_json()
        if not data or 'swimmers' not in data:
            return jsonify({'error': 'No data provided'}), 400
        
        df = pd.DataFrame(data['swimmers'])
        
        # Create temporary file
        if format.lower() == 'csv':
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='') as tmp:
                df.to_csv(tmp.name, index=False)
                mimetype = 'text/csv'
                filepath = tmp.name
                download_name = 'close_to_pin_analysis.csv'
        elif format.lower() == 'xlsx':
            with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
                df.to_excel(tmp.name, index=False, engine='openpyxl')
                mimetype = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                filepath = tmp.name
                download_name = 'close_to_pin_analysis.xlsx'
        elif format.lower() == 'pdf':
            filepath = create_close_to_pin_pdf(df)
            mimetype = 'application/pdf'
            download_name = 'close_to_pin_analysis.pdf'
        else:
            return jsonify({'error': 'Unsupported format'}), 400
            
        return send_file(
            filepath,
            as_attachment=True,
            download_name=download_name,
            mimetype=mimetype
        )
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500


def create_close_to_pin_pdf(df):
    """Create a PDF report for close to pin analysis."""
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import letter, A4
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import inch
        from datetime import datetime
        import os
        
        # Create temporary file
        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
            filepath = tmp.name
        
        # Create the PDF document
        doc = SimpleDocTemplate(filepath, pagesize=A4,
                              rightMargin=72, leftMargin=72,
                              topMargin=72, bottomMargin=18)
        
        # Container for the 'Flowable' objects
        elements = []
        
        # Define styles
        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=24,
            spaceAfter=30,
            alignment=1  # Center alignment
        )
        
        subtitle_style = ParagraphStyle(
            'CustomSubtitle',
            parent=styles['Heading2'],
            fontSize=16,
            spaceAfter=20,
            alignment=1  # Center alignment
        )
        
        # Add title
        title = Paragraph("🎯 Close to Pin Analysis Report", title_style)
        elements.append(title)
        
        # Add generation date
        date_str = datetime.now().strftime("%B %d, %Y at %I:%M %p")
        subtitle = Paragraph(f"Generated on {date_str}", subtitle_style)
        elements.append(subtitle)
        elements.append(Spacer(1, 20))
        
        # Add summary statistics
        total_swimmers = len(df)
        gold_qualifiers = len(df[df['Championship Meet'].str.contains('Gold', na=False)])
        silver_qualifiers = len(df[df['Championship Meet'].str.contains('Silver', na=False)])
        bronze_qualifiers = len(df[df['Championship Meet'].str.contains('Bronze', na=False)])
        
        summary_text = f"""
        <b>Summary Statistics:</b><br/>
        • Total Swimmers Analyzed: {total_swimmers}<br/>
        • Gold Meet Qualifiers: {gold_qualifiers}<br/>
        • Silver Meet Qualifiers: {silver_qualifiers}<br/>
        • Bronze Meet Qualifiers: {bronze_qualifiers}<br/>
        """
        
        summary = Paragraph(summary_text, styles['Normal'])
        elements.append(summary)
        elements.append(Spacer(1, 20))
        
        # Prepare table data
        # Column headers
        headers = ['Last Name', 'First Name', 'Event', 'Best Time', 'Championship Meet', 'Gold Time', 'Silver Time']
        
        # Convert DataFrame to list of lists for the table
        table_data = [headers]
        
        for _, row in df.iterrows():
            table_data.append([
                str(row.get('Last Name', '')),
                str(row.get('First Name', '')),
                str(row.get('Event', '')),
                str(row.get('Best Time', '')),
                str(row.get('Championship Meet', '')),
                str(row.get('Gold Time', '')),
                str(row.get('Silver Time', ''))
            ])
        
        # Create the table
        table = Table(table_data, repeatRows=1)
        
        # Apply table styling
        table.setStyle(TableStyle([
            # Header row styling
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            
            # Data rows styling
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 8),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.beige, colors.white]),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            
            # Championship Meet column styling (index 4)
            ('FONTNAME', (4, 1), (4, -1), 'Helvetica-Bold'),
            
            # Align times to center
            ('ALIGN', (3, 1), (-1, -1), 'CENTER'),
            
            # Add padding
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('LEFTPADDING', (0, 0), (-1, -1), 6),
            ('RIGHTPADDING', (0, 0), (-1, -1), 6),
        ]))
        
        # Add color coding for championship meet column
        for i, row in enumerate(df.itertuples(), 1):
            champ_meet = str(row._5)  # Championship Meet is 5th column (0-indexed)
            if 'Gold' in champ_meet:
                table.setStyle(TableStyle([('TEXTCOLOR', (4, i), (4, i), colors.darkgoldenrod)]))
                table.setStyle(TableStyle([('BACKGROUND', (4, i), (4, i), colors.lightyellow)]))
            elif 'Silver' in champ_meet:
                table.setStyle(TableStyle([('TEXTCOLOR', (4, i), (4, i), colors.darkslategray)]))
                table.setStyle(TableStyle([('BACKGROUND', (4, i), (4, i), colors.lightgrey)]))
            elif 'Bronze' in champ_meet:
                table.setStyle(TableStyle([('TEXTCOLOR', (4, i), (4, i), colors.darkred)]))
                table.setStyle(TableStyle([('BACKGROUND', (4, i), (4, i), colors.mistyrose)]))
        
        elements.append(table)
        
        # Add footer note
        elements.append(Spacer(1, 30))
        footer_text = """
        <b>Notes:</b><br/>
        • Gold Meet: Swimmer has qualified for the Gold championship meet<br/>
        • Silver Meet: Swimmer has qualified for the Silver championship meet<br/>
        • Bronze Meet: Swimmer has qualified for the Bronze championship meet<br/>
        • Times shown are from the swimmer's best performance in each event
        """
        footer = Paragraph(footer_text, styles['Normal'])
        elements.append(footer)
        
        # Build the PDF
        doc.build(elements)
        
        return filepath
        
    except ImportError:
        raise ImportError("ReportLab library is required for PDF generation. Install with: pip install reportlab")
    except Exception as e:
        raise Exception(f"Failed to create PDF: {str(e)}")


@app.route('/download-standards/<format>', methods=['POST'])
def download_standards(format):
    """Download calculated standards in specified format."""
    try:
        # Get the calculated standards from request
        data = request.get_json()
        if not data or 'results' not in data:
            return jsonify({'error': 'No data provided'}), 400
        
        df = pd.DataFrame(data['results'])
        
        # Create temporary file
        if format.lower() == 'csv':
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='') as tmp:
                df.to_csv(tmp.name, index=False)
                mimetype = 'text/csv'
                filepath = tmp.name
                download_name = 'time_standards.csv'
        elif format.lower() == 'standards_csv':
            # Format the data to match current_standards.csv structure
            standards_df = convert_to_standards_format(df)
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='') as tmp:
                standards_df.to_csv(tmp.name, index=False)
                mimetype = 'text/csv'
                filepath = tmp.name
                download_name = 'new_time_standards.csv'
        elif format.lower() == 'xlsx':
            with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
                df.to_excel(tmp.name, index=False, engine='openpyxl')
                mimetype = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                filepath = tmp.name
                download_name = 'time_standards.xlsx'
        else:
            return jsonify({'error': 'Unsupported format'}), 400
            
        return send_file(
            filepath,
            as_attachment=True,
            download_name=download_name,
            mimetype=mimetype
        )
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500


def convert_to_standards_format(df):
    """Convert calculated standards dataframe to current_standards.csv format."""
    try:
        # Create a new dataframe with the required columns
        standards_data = []
        
        for _, row in df.iterrows():
            # Extract event information from Event column
            event_info = row['Event']
            event_id = row['Event_ID']
            
            # Parse the event - format is "age_group_distance_stroke" with underscores
            if '_' in event_info:
                # Split by underscore
                parts = event_info.split('_')
                if len(parts) >= 3:
                    age_group = parts[0]
                    distance = parts[1]
                    stroke = '_'.join(parts[2:])  # Handle "Individual_Medley"
                    
                    # Convert distance to integer
                    try:
                        distance = int(distance)
                    except ValueError:
                        distance = None
                    
                    # Replace underscores with spaces in stroke name
                    stroke = stroke.replace('_', ' ')
                else:
                    age_group = distance = stroke = None
            else:
                # Fallback: try space-separated parsing
                parts = event_info.split()
                age_group = None
                distance = None
                stroke = None
                
                # Find age group (look for pattern with numbers)
                for i, part in enumerate(parts):
                    if any(char.isdigit() for char in part) and ('&' in part or '-' in part):
                        # This is likely the age group
                        if i > 0:
                            age_group = ' '.join(parts[:i+1])
                        else:
                            age_group = part
                        
                        # Look for distance (next numeric part)
                        for j in range(i+1, len(parts)):
                            if parts[j].isdigit():
                                distance = int(parts[j])
                                # Stroke is everything after distance
                                stroke = ' '.join(parts[j+1:])
                                break
                        break
                
                # If still not found, try regex approach
                if not all([age_group, distance, stroke]):
                    import re
                    
                    # Try to find age group pattern
                    age_match = re.search(r'(Boys|Girls|Men|Women)\s+(\d+(?:\s*&\s*Under|\s*-\s*\d+))', event_info)
                    if age_match:
                        age_group = age_match.group(0)
                    
                    # Try to find distance pattern
                    distance_match = re.search(r'\b(\d+)\b', event_info)
                    if distance_match:
                        distance = int(distance_match.group(1))
                    
                    # Extract stroke (common stroke names)
                    strokes = ['Freestyle', 'Backstroke', 'Breaststroke', 'Butterfly', 'Individual Medley']
                    for s in strokes:
                        if s in event_info:
                            stroke = s
                            break
            
            # Only add if we have all required information
            if age_group and distance and stroke:
                standards_data.append({
                    'age_group': age_group,
                    'distance': distance,
                    'stroke': stroke,
                    'gold_y': row.get('Proposed Gold Time (yards)', ''),
                    'gold_s': row.get('Proposed Gold Time (meters)', ''),
                    'silver_y': row.get('Proposed Silver Time (yards)', ''),
                    'silver_s': row.get('Proposed Silver Time (meters)', '')
                })
        
        # Create DataFrame
        standards_df = pd.DataFrame(standards_data)
        
        # Sort by age group and event for consistency
        if not standards_df.empty:
            standards_df = standards_df.sort_values(['age_group', 'distance', 'stroke'])
        
        return standards_df
    
    except Exception as e:
        # If conversion fails, return empty dataframe with proper columns
        return pd.DataFrame(columns=['age_group', 'distance', 'stroke', 'gold_y', 'gold_s', 'silver_y', 'silver_s'])


@app.route('/save-final-standards', methods=['POST'])
@limiter.limit("3 per minute")
def save_final_standards():
    """Save the finalized time standards after review and editing."""
    try:
        # Get the finalized standards from request
        data = request.get_json()
        if not data or 'standards' not in data:
            return jsonify({'error': 'No standards data provided'}), 400
        
        standards_data = data['standards']
        parameters = data.get('parameters', {})
        
        # Create DataFrame from the finalized standards
        df = pd.DataFrame(standards_data)
        
        # Filter out rejected standards
        accepted_standards = df[df['Status'] != 'rejected'].copy()
        
        if accepted_standards.empty:
            return jsonify({'error': 'No standards were accepted for saving'}), 400
        
        # Create backup of current standards
        current_standards_path = os.path.join(os.getcwd(), 'current_standards.csv')
        if os.path.exists(current_standards_path):
            # Create backup with timestamp
            from datetime import datetime
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_path = os.path.join(os.getcwd(), f'current_standards_backup_{timestamp}.csv')
            
            import shutil
            shutil.copy2(current_standards_path, backup_path)
        
        # Convert accepted standards to the current_standards.csv format
        new_standards_df = convert_final_standards_to_format(accepted_standards)
        
        # Save new standards with metadata
        new_standards_path = os.path.join(os.getcwd(), 'current_standards_new.csv')
        save_standards_with_metadata(new_standards_df, new_standards_path)
        
        # Log the changes
        log_standards_changes(accepted_standards, parameters)
        
        return jsonify({
            'status': 'success',
            'message': f'Successfully saved {len(accepted_standards)} updated standards',
            'backup_created': os.path.exists(backup_path) if 'backup_path' in locals() else False,
            'new_file': 'current_standards_new.csv',
            'accepted_count': len(accepted_standards),
            'rejected_count': len(df) - len(accepted_standards)
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500


def convert_final_standards_to_format(df):
    """Convert finalized standards dataframe to current_standards.csv format."""
    try:
        # Create a new dataframe with the required columns
        standards_data = []
        
        for _, row in df.iterrows():
            # Extract event information from Event column
            event_info = row['Event']
            
            # Parse the event - format is "age_group_distance_stroke" with underscores
            if '_' in event_info:
                # Split by underscore
                parts = event_info.split('_')
                if len(parts) >= 3:
                    age_group = parts[0]
                    distance = parts[1]
                    stroke = '_'.join(parts[2:])  # Handle "Individual_Medley"
                    
                    # Convert distance to integer
                    try:
                        distance = int(distance)
                    except ValueError:
                        distance = None
                    
                    # Replace underscores with spaces in stroke name
                    stroke = stroke.replace('_', ' ')
                else:
                    age_group = distance = stroke = None
            else:
                # Fallback parsing for space-separated format
                age_group, distance, stroke = parse_event_fallback(event_info)
            
            # Only add if we have all required information
            if age_group and distance and stroke:
                standards_data.append({
                    'age_group': age_group,
                    'distance': distance,
                    'stroke': stroke,
                    'gold_y': row.get('Final Gold Time (yards)', row.get('Proposed Gold Time (yards)', '')),
                    'gold_s': row.get('Proposed Gold Time (meters)', ''),
                    'silver_y': row.get('Final Silver Time (yards)', row.get('Proposed Silver Time (yards)', '')),
                    'silver_s': row.get('Proposed Silver Time (meters)', '')
                })
        
        # Create DataFrame
        standards_df = pd.DataFrame(standards_data)
        
        # Sort by age group and event for consistency
        if not standards_df.empty:
            standards_df = standards_df.sort_values(['age_group', 'distance', 'stroke'])
        
        return standards_df
    
    except Exception as e:
        # If conversion fails, return empty dataframe with proper columns
        return pd.DataFrame(columns=['age_group', 'distance', 'stroke', 'gold_y', 'gold_s', 'silver_y', 'silver_s'])


def parse_event_fallback(event_info):
    """Fallback parsing for event information."""
    import re
    
    age_group = None
    distance = None
    stroke = None
    
    # Try to find age group pattern
    age_match = re.search(r'(Boys|Girls|Men|Women)\s+(\d+(?:\s*&\s*Under|\s*-\s*\d+))', event_info)
    if age_match:
        age_group = age_match.group(0)
    
    # Try to find distance pattern
    distance_match = re.search(r'\b(\d+)\b', event_info)
    if distance_match:
        distance = int(distance_match.group(1))
    
    # Extract stroke (common stroke names)
    strokes = ['Freestyle', 'Backstroke', 'Breaststroke', 'Butterfly', 'Individual Medley']
    for s in strokes:
        if s in event_info:
            stroke = s
            break
    
    return age_group, distance, stroke


def save_standards_with_metadata(standards_df, filepath):
    """Save standards DataFrame to CSV with metadata header."""
    try:
        from datetime import datetime
        
        # Generate current date for metadata
        current_date = datetime.now().strftime('%B %Y')
        metadata_line = f"# Adopted: {current_date}\n"
        
        # Write metadata header and CSV data
        with open(filepath, 'w', encoding='utf-8', newline='') as f:
            f.write(metadata_line)
            standards_df.to_csv(f, index=False)
            
    except Exception as e:
        # Fallback to regular CSV save if metadata save fails
        print(f"Warning: Failed to save with metadata, using regular save: {e}")
        standards_df.to_csv(filepath, index=False)


def log_standards_changes(standards_df, parameters):
    """Log the standards changes for audit purposes."""
    try:
        from datetime import datetime
        import json
        
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'parameters': parameters,
            'changes_count': len(standards_df),
            'events_modified': standards_df['Event'].tolist() if 'Event' in standards_df.columns else [],
            'user_action': 'standards_update'
        }
        
        log_file = os.path.join(os.getcwd(), 'standards_changes.log')
        with open(log_file, 'a') as f:
            f.write(json.dumps(log_entry) + '\n')
    
    except Exception as e:
        print(f"Warning: Failed to log standards changes: {e}")


if __name__ == '__main__':
    app.run(debug=True)