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
from utils import add_event_names_column, convert_time_to_hundredths
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


def create_label_overlay(label_text, page_width, page_height, font_size=None, color=None):
    """
    Create a PDF overlay with the label text at the specified position.
    
    Args:
        label_text (str): Text to display as label
        page_width (float): Width of the page
        page_height (float): Height of the page
        font_size (int, optional): Font size for the label
        color (str, optional): Color name for the label
    
    Returns:
        BytesIO: PDF overlay as bytes
    """
    from io import BytesIO
    from reportlab.pdfgen import canvas
    from reportlab.lib.colors import red, blue, black, green, purple, orange, gray
    
    # Use defaults if not provided
    font_size = font_size or Config.PDF_LABEL_FONT_SIZE_DEFAULT
    color = color or Config.PDF_LABEL_COLOR_DEFAULT
    
    # Fixed settings (not customizable)
    position_top = 20  # Fixed: 20 points from top
    font_name = 'Helvetica-Bold'  # Fixed: Helvetica Bold
    uppercase = True  # Fixed: Always uppercase
    
    # Validate font size
    font_size = max(Config.PDF_LABEL_FONT_SIZE_MIN, min(Config.PDF_LABEL_FONT_SIZE_MAX, font_size))
    
    # Map color names to color objects
    color_map = {
        'red': red,
        'blue': blue,
        'black': black,
        'green': green,
        'purple': purple,
        'orange': orange,
        'gray': gray
    }
    
    color_obj = color_map.get(color, red)
    
    packet = BytesIO()
    can = canvas.Canvas(packet, pagesize=(page_width, page_height))
    
    # Set font, size, and color
    can.setFont(font_name, font_size)
    can.setFillColor(color_obj)
    
    # Process text (always uppercase)
    display_text = label_text.upper()
    
    # Calculate center position for text
    text_width = can.stringWidth(display_text, font_name, font_size)
    x_position = (page_width - text_width) / 2
    y_position = page_height - position_top
    
    # Draw the text
    can.drawString(x_position, y_position, display_text)
    can.save()
    
    packet.seek(0)
    return packet


def create_blank_page(page_width, page_height):
    """
    Create a blank PDF page of the specified dimensions.
    
    Args:
        page_width (float): Width of the page
        page_height (float): Height of the page
    
    Returns:
        BytesIO: Blank PDF page as bytes
    """
    from io import BytesIO
    from reportlab.pdfgen import canvas
    
    packet = BytesIO()
    can = canvas.Canvas(packet, pagesize=(page_width, page_height))
    
    # Draw something invisible to ensure the page is created properly
    can.setFillAlpha(0)  # Make it completely transparent
    can.rect(0, 0, 1, 1, fill=1)  # Draw a tiny invisible rectangle
    can.showPage()  # Ensure the page is finalized
    can.save()
    
    packet.seek(0)
    return packet


def add_labels_to_pdf(input_pdf_file, labels, label_settings=None):
    """
    Add labels to PDF file and return a concatenated PDF with all labeled versions.
    
    Args:
        input_pdf_file: File object of the input PDF
        labels: List of label strings to apply
        label_settings (dict, optional): Label formatting settings
    
    Returns:
        BytesIO: Concatenated PDF with all labeled versions
    """
    from PyPDF2 import PdfReader, PdfWriter
    from io import BytesIO
    import copy
    
    # Use default settings if none provided
    if label_settings is None:
        label_settings = {}
    
    # Read the input PDF once to get basic info
    input_pdf_file.seek(0)
    input_pdf_data = input_pdf_file.read()
    
    # Validate input PDF has pages
    temp_reader = PdfReader(BytesIO(input_pdf_data))
    if len(temp_reader.pages) == 0:
        raise ValueError("Input PDF has no pages")
    
    # Get page dimensions from first page
    first_page_temp = temp_reader.pages[0]
    page_width = float(first_page_temp.mediabox.width)
    page_height = float(first_page_temp.mediabox.height)
    total_pages = len(temp_reader.pages)
    
    # Create a writer for the final concatenated PDF
    final_writer = PdfWriter()
    
    # Process each label
    for label_index, label in enumerate(labels):
        try:
            # Create a fresh reader for each label to avoid page modification issues
            input_buffer = BytesIO(input_pdf_data)
            reader = PdfReader(input_buffer)
            
            # Verify we still have pages
            if len(reader.pages) == 0:
                raise ValueError(f"No pages found in PDF for label {label_index + 1}")
            
            # Create label overlay with custom settings
            overlay_packet = create_label_overlay(
                label, 
                page_width, 
                page_height,
                font_size=label_settings.get('font_size'),
                color=label_settings.get('color')
            )
            overlay_reader = PdfReader(overlay_packet)
            
            # Verify overlay was created successfully
            if len(overlay_reader.pages) == 0:
                raise ValueError(f"Failed to create overlay for label: {label}")
            
            overlay_page = overlay_reader.pages[0]
            
            # Get first page and merge with overlay
            first_page = reader.pages[0]
            first_page.merge_page(overlay_page)
            final_writer.add_page(first_page)
            
            # Add remaining pages unchanged
            for page_num in range(1, len(reader.pages)):
                if page_num < len(reader.pages):  # Safety check
                    final_writer.add_page(reader.pages[page_num])
            
            # Add blank page if this labeled copy has odd number of pages (for double-sided printing)
            current_pages = len(reader.pages)
            
            if Config.PDF_ADD_BLANK_PAGE_FOR_DOUBLE_SIDED and current_pages % 2 == 1:
                # Create and add blank page
                blank_packet = create_blank_page(page_width, page_height)
                blank_reader = PdfReader(blank_packet)
                
                if len(blank_reader.pages) > 0:
                    blank_page = blank_reader.pages[0]
                    final_writer.add_page(blank_page)
                
        except Exception as e:
            raise ValueError(f"Error processing label '{label}': {str(e)}")
    
    # Verify we have pages to write
    if len(final_writer.pages) == 0:
        raise ValueError("No pages to write to output PDF")
    
    # Write the final concatenated PDF
    output_buffer = BytesIO()
    final_writer.write(output_buffer)
    output_buffer.seek(0)
    
    return output_buffer


class Config:
    """Configuration class for default values."""
    DEFAULT_GOLD_PERCENTILE = 0.15
    DEFAULT_SILVER_PERCENTILE = 0.55
    DEFAULT_HEAT_TIME = 15  # seconds
    DEFAULT_EVENT_TIME = 30  # seconds
    ALLOWED_EXTENSIONS = {'csv', 'pdf', 'txt'}
    MAX_FILENAME_LENGTH = 255
    MAX_FILE_SIZE = 16 * 1024 * 1024  # 16MB
    ALLOWED_MIME_TYPES = {'text/csv', 'text/plain', 'application/csv', 'application/pdf'}
    # PDF labeling defaults
    PDF_LABEL_FONT_SIZE_DEFAULT = 10
    PDF_LABEL_FONT_SIZE_MIN = 6
    PDF_LABEL_FONT_SIZE_MAX = 24
    PDF_LABEL_COLOR_DEFAULT = 'red'
    PDF_LABEL_POSITION_TOP_DEFAULT = 20  # points from top
    PDF_LABEL_POSITION_TOP_MIN = 10
    PDF_LABEL_POSITION_TOP_MAX = 100
    PDF_LABEL_AVAILABLE_COLORS = {
        'red': (1, 0, 0),
        'blue': (0, 0, 1),
        'black': (0, 0, 0),
        'green': (0, 0.5, 0),
        'purple': (0.5, 0, 0.5),
        'orange': (1, 0.5, 0),
        'gray': (0.5, 0.5, 0.5)
    }
    PDF_LABEL_AVAILABLE_FONTS = [
        'Helvetica-Bold',
        'Helvetica',
        'Times-Bold',
        'Times-Roman',
        'Courier-Bold',
        'Courier'
    ]
    # Double-sided printing support
    PDF_ADD_BLANK_PAGE_FOR_DOUBLE_SIDED = True


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
    if not re.match(r'^[a-zA-Z0-9._\-\s]+$', filename):
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
        
        # File type specific validation
        file_extension = os.path.splitext(file_path)[1].lower()
        
        if file_extension == '.pdf':
            # PDF validation
            try:
                from PyPDF2 import PdfReader
                reader = PdfReader(file_path)
                
                # Check if PDF is readable and has pages
                if len(reader.pages) == 0:
                    return False, "PDF file has no pages"
                
                # Try to read first page to ensure it's not corrupted
                first_page = reader.pages[0]
                # This will raise an exception if the PDF is corrupted
                _ = first_page.mediabox
                
            except Exception as e:
                return False, f"Invalid PDF file: {str(e)}"
                
        elif file_extension in ['.csv', '.txt']:
            # Enhanced CSV/TXT validation - check file content
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
        else:
            return False, f"Unsupported file type: {file_extension}"
        
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


@app.route('/pdf-labeler')
def pdf_labeler():
    """Serve the PDF labeling page."""
    return render_template('pdf-labeler.html')


@app.route('/swim-event-tracker')
def swim_event_tracker():
    """Serve the swim event tracker page."""
    return render_template('swim-event-tracker.html')


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


@app.route('/export-current-standards-pdf')
@limiter.limit("10 per minute")
def export_current_standards_pdf():
    """Export current standards to PDF."""
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import letter, landscape
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import inch
        from io import BytesIO
        import datetime
        import pandas as pd
        
        # Read current standards
        from utils import read_csv_with_metadata
        df = read_csv_with_metadata('current_standards.csv')
        
        # Get adoption date
        adoption_date = extract_metadata_date('current_standards.csv')
        if not adoption_date:
            import os
            mtime = os.path.getmtime('current_standards.csv')
            adoption_date = datetime.datetime.fromtimestamp(mtime).strftime('%B %Y')
        
        # Create PDF buffer
        buffer = BytesIO()
        
        # Use standard portrait letter size
        doc = SimpleDocTemplate(buffer, pagesize=letter, 
                              rightMargin=0.5*inch, leftMargin=0.5*inch,
                              topMargin=0.75*inch, bottomMargin=0.75*inch)
        
        # Build the document
        elements = []
        styles = getSampleStyleSheet()
        
        # Title
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=16,
            spaceAfter=4,
            alignment=1,  # Center alignment
            textColor=colors.HexColor('#2c3e50')
        )
        title = Paragraph("GASL Time Standards", title_style)
        elements.append(title)
        
        # Subtitle with adoption date
        subtitle_style = ParagraphStyle(
            'CustomSubtitle',
            parent=styles['Normal'],
            fontSize=8,
            spaceAfter=8,
            alignment=1,  # Center alignment
            textColor=colors.HexColor('#7f8c8d')
        )
        subtitle = Paragraph(f"Adopted: {adoption_date}", subtitle_style)
        elements.append(subtitle)
        
        # Function to extract age for sorting
        def extract_age_order(age_group):
            """Extract numeric age or assign order for age groups"""
            age_str = str(age_group).lower()
            
            # Handle specific age ranges
            if '8 & under' in age_str or '8&under' in age_str:
                return 8
            elif '9-10' in age_str:
                return 9
            elif '11-12' in age_str:
                return 11
            elif '13-14' in age_str:
                return 13
            elif '15-18' in age_str:
                return 15
            elif 'women' in age_str or 'men' in age_str:
                return 19  # Adult category comes last
            else:
                # Try to extract first number found
                import re
                numbers = re.findall(r'\d+', age_str)
                if numbers:
                    return int(numbers[0])
                else:
                    return 999  # Unknown ages go to end
        
        # Add age ordering column
        df['age_order'] = df['age_group'].apply(extract_age_order)
        
        # Separate data by gender and sort by age
        girls_data = df[df['age_group'].str.contains('Girls|Women', case=False, na=False)].sort_values(['age_order', 'distance', 'stroke'])
        boys_data = df[df['age_group'].str.contains('Boys|Men', case=False, na=False) & 
                      ~df['age_group'].str.contains('Girls|Women', case=False, na=False)].sort_values(['age_order', 'distance', 'stroke'])
        
        def create_gender_table(data, title, title_color):
            """Create a table for a specific gender group"""
            table_data = []
            
            # Create multi-level headers
            header_row1 = ['Event', '', '', 'Yards', '', 'Meters', '']
            header_row2 = ['Age Group', 'Distance', 'Stroke', 'Gold', 'Silver', 'Gold', 'Silver']
            
            table_data.append(header_row1)
            table_data.append(header_row2)
            
            # Add data rows
            for _, row in data.iterrows():
                def clean_value(val):
                    """Convert value to string and handle NaN/None"""
                    if pd.isna(val) or val is None or str(val).lower() == 'nan':
                        return ''
                    return str(val)
                
                table_data.append([
                    clean_value(row.get('age_group', '')),
                    clean_value(row.get('distance', '')),
                    clean_value(row.get('stroke', '')),
                    clean_value(row.get('gold_y', '')),
                    clean_value(row.get('silver_y', '')),
                    clean_value(row.get('gold_s', '')),
                    clean_value(row.get('silver_s', ''))
                ])
            
            # Create table
            table = Table(table_data, repeatRows=2)
            
            # Table styling
            table.setStyle(TableStyle([
                # Main header row styling (row 0)
                ('BACKGROUND', (0, 0), (2, 0), colors.HexColor('#4b80d6')),  # Age Group, Distance, Stroke
                ('BACKGROUND', (3, 0), (4, 0), colors.HexColor('#4b80d6')),  # Yards columns
                ('BACKGROUND', (5, 0), (6, 0), colors.HexColor('#4b80d6')),  # Meters columns
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
                ('SPAN', (0, 0), (2, 0)),
                ('SPAN', (3, 0), (4, 0)),  # Span "Yards" across two columns
                ('SPAN', (5, 0), (6, 0)),  # Span "Meters" across two columns
                
                # Sub-header row styling (row 1)
                ('BACKGROUND', (0, 1), (2, 1), colors.HexColor('#4b80d6')),  # Age Group, Distance, Stroke
                ('BACKGROUND', (3, 1), (4, 1), colors.HexColor('#4b80d6')),  # Gold, Silver for Yards
                ('BACKGROUND', (5, 1), (6, 1), colors.HexColor('#4b80d6')),  # Gold, Silver for Meters
                ('TEXTCOLOR', (0, 1), (-1, 1), colors.whitesmoke),
                ('FONTNAME', (0, 1), (-1, 1), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 1), (-1, 1), 9),
                ('ALIGN', (0, 1), (-1, 1), 'CENTER'),
                
                # General styling for all cells
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#bdc3c7')),
                
                # Header padding
                ('TOPPADDING', (0, 0), (-1, 1), 6),
                ('BOTTOMPADDING', (0, 0), (-1, 1), 6),
                ('LEFTPADDING', (0, 0), (-1, -1), 4),
                ('RIGHTPADDING', (0, 0), (-1, -1), 4),
                
                # Data rows styling (row 2 and beyond)
                ('FONTNAME', (0, 2), (-1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 2), (-1, -1), 8),
                ('ALIGN', (0, 2), (-1, -1), 'CENTER'),
                ('ROWBACKGROUNDS', (0, 2), (-1, -1), [colors.white, colors.HexColor('#f8f9fa')]),
                ('TOPPADDING', (0, 2), (-1, -1), 3),
                ('BOTTOMPADDING', (0, 2), (-1, -1), 3),
            ]))
            
            return table
        
        # Add Girls/Women section
        girls_title = Paragraph("Girls Events", ParagraphStyle(
            'GirlsTitle',
            parent=styles['Heading2'],
            fontSize=14,
            spaceAfter=10,
            alignment=1,
            textColor=colors.HexColor('#2c3e50')
        ))
        elements.append(girls_title)
        
        girls_table = create_gender_table(girls_data, "Girls", '#2c3e50')
        elements.append(girls_table)
        
        # Add page break
        from reportlab.platypus import PageBreak
        elements.append(PageBreak())
        
        # Add title and subtitle for second page
        elements.append(title)
        elements.append(subtitle)
        
        # Add Boys/Men section
        boys_title = Paragraph("Boys Events", ParagraphStyle(
            'BoysTitle',
            parent=styles['Heading2'],
            fontSize=14,
            spaceAfter=10,
            alignment=1,
            textColor=colors.HexColor('#2c3e50')
        ))
        elements.append(boys_title)
        
        boys_table = create_gender_table(boys_data, "Boys", '#2c3e50')
        elements.append(boys_table)
        
        # Add footer
        elements.append(Spacer(1, 0.3*inch))
        footer_style = ParagraphStyle(
            'Footer',
            parent=styles['Normal'],
            fontSize=8,
            alignment=1,
            textColor=colors.HexColor('#95a5a6')
        )
        footer_text = f"Generated on {datetime.datetime.now().strftime('%B %d, %Y')} • Greater Annapolis Swimming League"
        footer = Paragraph(footer_text, footer_style)
        #elements.append(footer)
        
        # Build PDF
        doc.build(elements)
        
        # Prepare response
        buffer.seek(0)
        
        from flask import make_response
        response = make_response(buffer.getvalue())
        response.headers['Content-Type'] = 'application/pdf'
        response.headers['Content-Disposition'] = f'attachment; filename="GASL_Time_Standards_{adoption_date.replace(" ", "_")}.pdf"'
        
        return response
        
    except Exception as e:
        print(f"Error generating PDF: {e}")
        return jsonify({'error': f'Failed to generate PDF: {str(e)}'}), 500


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


@app.route('/current-meet-duration', methods=['GET'])
def current_meet_duration():
    """Render the current meet duration estimation page."""
    return render_template('current-meet-duration.html')


@app.route('/estimate-current-meet-duration', methods=['POST'])
@limiter.limit("5 per minute")
def estimate_current_meet_duration():
    """Estimate meet duration using current standards."""
    print("DEBUG: Starting estimate_current_meet_duration")
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
            
            # Load current standards using the metadata-aware function
            from utils import read_csv_with_metadata
            current_standards = read_csv_with_metadata(standards_filepath)
            print(f"DEBUG: Current standards columns: {list(current_standards.columns)}")
            
            # Check if current_standards already has Event_name column
            if 'Event_name' not in current_standards.columns:
                # Only add event names if the column doesn't exist
                current_standards = add_event_names_column(current_standards)
            else:
                print("DEBUG: Current standards already has Event_name column")
            
            # Use the same data preparation as the original method
            data_pattern = os.path.join(data_dir, '*.csv')
            print(f"DEBUG: Loading data from pattern: {data_pattern}")
            df = service.prepare_data(data_pattern)
            print(f"DEBUG: Data loaded, shape: {df.shape}")
            
            # Debug: check what columns we have
            available_columns = list(df.columns)
            print(f"DEBUG: Raw columns after prepare_data: {available_columns}")
            
            # Normalize column names like in close_to_pin.py
            if 'AgeGroup' in df.columns and 'Event' in df.columns:
                # Swimtopia format - convert columns
                df = df.assign(
                    distance=df.Event.str.split(' ', n=1, expand=True)[0],
                    stroke=df.Event.str.split(' ', n=1, expand=True)[1],
                    age_group=df.AgeGroup.astype(str)
                )
            elif 'age_group' in df.columns and 'distance' in df.columns and 'stroke' in df.columns:
                # Already has the required columns - no conversion needed
                pass
            else:
                return jsonify({
                    'error': f'Missing required columns. Available columns: {available_columns}. Expected: AgeGroup, Event (or age_group, distance, stroke)'
                }), 400
            
            # Add event names - converted_hundredths should already exist in the source data
            df = add_event_names_column(df)
            
            # Verify that converted_hundredths column exists
            if 'converted_hundredths' not in df.columns:
                return jsonify({'error': 'converted_hundredths column not found in data. Please use gasl_top_times.csv format.'}), 400
            
            # Process all swimmers together (ignore dates - just estimate for current data)
            print(f"DEBUG: Processing all data together, shape: {df.shape}")
            print(f"DEBUG: Columns: {list(df.columns)}")
            
            # Check for team assignments
            team_assignments = None
            if 'team_assignments' in request.form:
                import json
                team_assignments = json.loads(request.form['team_assignments'])
                print(f"DEBUG: Team assignments: {team_assignments}")
            
            # Get duration estimates using current standards for all data
            duration_result = service.get_meet_duration_with_current_standards(
                df, 
                "Current", 
                current_standards, 
                heat_time, 
                event_time,
                team_assignments
            )
            
            # Handle NaN values before converting to dict
            times_df_clean = duration_result['times_df'].fillna('')
            entries_summary_clean = service.get_team_attendance_summary(duration_result['entries']).fillna(0)
            
            # Convert attendance summary to the format expected by JavaScript
            entries_dict = {}
            for team in entries_summary_clean.index:
                entries_dict[team] = entries_summary_clean.loc[team].to_dict()
            
            duration_results = [{
                'season': duration_result['season'],
                'times_df': times_df_clean.to_dict('records'),
                'meet_summary': duration_result['meet_summary'],
                'entries_summary': entries_dict
            }]
            
            return jsonify({
                'status': 'success',
                'parameters': {
                    'heat_time': heat_time,
                    'event_time': event_time
                },
                'duration_estimates': duration_results
            })
    
    except Exception as e:
        print(f"DEBUG: Exception occurred: {str(e)}")
        print(f"DEBUG: Exception type: {type(e)}")
        import traceback
        print(f"DEBUG: Traceback: {traceback.format_exc()}")
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
        
        # Get units parameter from form data (default to 'yards' if not provided)
        units = request.form.get('units', 'yards')
        if units not in ['yards', 'meters']:
            units = 'yards'  # Fallback to yards if invalid value provided
        
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
                standards_filepath,
                units
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


@app.route('/label-pdf', methods=['POST'])
@limiter.limit("3 per minute")
def label_pdf():
    """Add labels to a PDF file and return concatenated result."""
    try:
        # Check if PDF file is present
        if 'pdf_file' not in request.files:
            return jsonify({'error': 'Missing PDF file'}), 400
        
        pdf_file = request.files['pdf_file']
        
        # Validate PDF file
        if not pdf_file or pdf_file.filename == '':
            return jsonify({'error': 'No PDF file selected'}), 400
        
        # Enhanced filename validation
        is_valid, message = validate_filename(pdf_file.filename)
        if not is_valid:
            return jsonify({'error': f'Invalid filename: {message}'}), 400
        
        if not allowed_file(pdf_file.filename):
            return jsonify({'error': f'File type not allowed: {pdf_file.filename}'}), 400
        
        # Get labels from form data
        labels_str = request.form.get('labels', '')
        if not labels_str:
            return jsonify({'error': 'No labels provided'}), 400
        
        # Parse labels (expect comma-separated string or JSON array)
        labels = []
        try:
            import json
            # Try to parse as JSON array first
            labels = json.loads(labels_str)
            if not isinstance(labels, list):
                raise ValueError("Labels must be an array")
        except (json.JSONDecodeError, ValueError):
            # Fallback to comma-separated string
            labels = [label.strip() for label in labels_str.split(',') if label.strip()]
        
        if not labels:
            return jsonify({'error': 'No valid labels provided'}), 400
        
        # Validate and sanitize labels
        sanitized_labels = []
        for label in labels:
            sanitized_label = sanitize_input(label, 'string', max_length=50)
            if sanitized_label:
                sanitized_labels.append(sanitized_label)
        
        if not sanitized_labels:
            return jsonify({'error': 'No valid labels after sanitization'}), 400
        
        # Get and validate label settings
        label_settings = {}
        
        # Font size
        font_size = request.form.get('font_size')
        if font_size:
            try:
                font_size = int(font_size)
                font_size = max(Config.PDF_LABEL_FONT_SIZE_MIN, min(Config.PDF_LABEL_FONT_SIZE_MAX, font_size))
                label_settings['font_size'] = font_size
            except ValueError:
                pass
        
        # Color
        color = request.form.get('color')
        if color and color in Config.PDF_LABEL_AVAILABLE_COLORS:
            label_settings['color'] = color
        
        # Save PDF file temporarily for validation
        with tempfile.TemporaryDirectory() as temp_dir:
            pdf_filename = secure_filename(pdf_file.filename)
            pdf_filepath = os.path.join(temp_dir, pdf_filename)
            
            # Save file
            pdf_file.save(pdf_filepath)
            
            # Validate file content
            is_valid, message = validate_file_content(pdf_filepath)
            if not is_valid:
                return jsonify({'error': f'File validation failed: {message}'}), 400
            
            # Reset file pointer and process PDF
            pdf_file.seek(0)
            
            # Process the PDF with labels
            try:
                labeled_pdf_buffer = add_labels_to_pdf(pdf_file, sanitized_labels, label_settings)
                
                # Generate output filename
                base_name = os.path.splitext(pdf_file.filename)[0]
                output_filename = f"{base_name}_labeled.pdf"
                
                # Return the labeled PDF
                from flask import make_response
                response = make_response(labeled_pdf_buffer.getvalue())
                response.headers['Content-Type'] = 'application/pdf'
                response.headers['Content-Disposition'] = f'attachment; filename="{output_filename}"'
                
                return response
                
            except Exception as e:
                return jsonify({'error': f'PDF processing failed: {str(e)}'}), 500
    
    except Exception as e:
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
                              rightMargin=36, leftMargin=36,
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
        total_events = len(df)
        gold_qualifiers = len(df[df['Championship Meet'].str.contains('Gold', na=False)])
        silver_qualifiers = len(df[df['Championship Meet'].str.contains('Silver', na=False)])
        bronze_qualifiers = len(df[df['Championship Meet'].str.contains('Bronze', na=False)])
        
        summary_text = f"""
        <b>Summary Statistics:</b><br/>
        • Events Analyzed: {total_events}<br/>
        • Gold Meet Qualifiers: {gold_qualifiers}<br/>
        • Silver Meet Qualifiers: {silver_qualifiers}<br/>
        • Bronze Meet Qualifiers: {bronze_qualifiers}<br/>
        """
        
        summary = Paragraph(summary_text, styles['Normal'])
        elements.append(summary)
        elements.append(Spacer(1, 20))
        
        # Prepare table data
        # Column headers - find the actual gold and silver time column names
        gold_col = None
        silver_col = None
        for col in df.columns:
            if 'Gold Time' in col:
                gold_col = col
            elif 'Silver Time' in col:
                silver_col = col
        
        headers = ['Last\nName', 'First\nName', 'Event', 'Best\nTime', 'Championship\nMeet', 
                  'Gold\nTime', 'Silver\nTime']
        
        # Convert DataFrame to list of lists for the table
        table_data = [headers]
        
        for _, row in df.iterrows():
            table_data.append([
                str(row.get('Last Name', '')),
                str(row.get('First Name', '')),
                str(row.get('Event', '')),
                str(row.get('Best Time', '')),
                str(row.get('Championship Meet', '')),
                str(row.get(gold_col, '')) if gold_col else '',
                str(row.get(silver_col, '')) if silver_col else ''
            ])
        
        # Create the table with column widths
        col_widths = [0.8*inch, 0.8*inch, 1.8*inch, 0.8*inch, 1.2*inch, 0.8*inch, 0.8*inch]
        table = Table(table_data, colWidths=col_widths, repeatRows=1)
        
        # Apply table styling
        table.setStyle(TableStyle([
            # Header row styling
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 8),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
            ('VALIGN', (0, 0), (-1, 0), 'MIDDLE'),
            
            # Data rows styling
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 7),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.beige, colors.white]),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            
            # Championship Meet column styling (index 4)
            ('FONTNAME', (4, 1), (4, -1), 'Helvetica-Bold'),
            
            # Align times to center
            ('ALIGN', (3, 1), (-1, -1), 'CENTER'),
            
            # Add padding
            ('TOPPADDING', (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
            ('LEFTPADDING', (0, 0), (-1, -1), 3),
            ('RIGHTPADDING', (0, 0), (-1, -1), 3),
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


@app.route('/generate-swim-event-tracker', methods=['POST'])
@limiter.limit("3 per minute")
def generate_swim_event_tracker():
    """Generate swim event tracker PDF from uploaded CSV/TXT file."""
    try:
        # Check if events file is present
        if 'events_file' not in request.files:
            return jsonify({'error': 'Missing events file'}), 400
        
        events_file = request.files['events_file']
        
        # Validate events file
        if not events_file or events_file.filename == '':
            return jsonify({'error': 'No events file selected'}), 400
        
        # Enhanced filename validation
        is_valid, message = validate_filename(events_file.filename)
        if not is_valid:
            return jsonify({'error': f'Invalid filename: {message}'}), 400
        
        if not allowed_file(events_file.filename):
            return jsonify({'error': f'File type not allowed: {events_file.filename}'}), 400
        
        # Get swim meet name
        swim_meet_name = sanitize_input(
            request.form.get('swim_meet_name', 'Swim Event Race Number Tracker'), 
            'string', 
            max_length=100
        )
        
        # Save events file temporarily for validation
        with tempfile.TemporaryDirectory() as temp_dir:
            events_filename = secure_filename(events_file.filename)
            events_filepath = os.path.join(temp_dir, events_filename)
            
            # Save file
            events_file.save(events_filepath)
            
            # Validate file content
            is_valid, message = validate_file_content(events_filepath)
            if not is_valid:
                return jsonify({'error': f'File validation failed: {message}'}), 400
            
            # Parse events and generate PDF
            try:
                # Parse events from uploaded file
                events = parse_events_from_file(events_filepath)
                
                if not events:
                    return jsonify({'error': 'No valid events found in the file'}), 400
                
                # Create PDF in memory
                pdf_buffer = create_swim_event_tracker_pdf(events, swim_meet_name)
                
                # Generate output filename
                safe_meet_name = re.sub(r'[^\w\s-]', '', swim_meet_name).strip()
                safe_meet_name = re.sub(r'[-\s]+', '_', safe_meet_name)
                output_filename = f"{safe_meet_name}_Event_Tracker.pdf"
                
                # Return the PDF
                from flask import make_response
                response = make_response(pdf_buffer.getvalue())
                response.headers['Content-Type'] = 'application/pdf'
                response.headers['Content-Disposition'] = f'attachment; filename="{output_filename}"'
                
                return response
                
            except Exception as e:
                return jsonify({'error': f'PDF generation failed: {str(e)}'}), 500
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500


def parse_events_from_file(filepath):
    """Parse events from uploaded CSV/TXT file."""
    events = []
    
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            for line in file:
                line = line.strip()
                if not line:
                    continue
                
                # Split by comma and extract the first 3 columns
                parts = line.split(',')
                if len(parts) >= 3:
                    try:
                        event_number = parts[0].strip()
                        event_name = parts[1].strip()
                        num_heats = int(parts[2].strip())
                        events.append((event_number, event_name, num_heats))
                    except (ValueError, IndexError):
                        # Skip invalid lines
                        continue
                        
    except Exception as e:
        raise Exception(f"Error parsing events file: {str(e)}")
    
    return events


def create_swim_event_tracker_pdf(events, swim_meet_name):
    """Create swim event tracker PDF with custom title."""
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.units import inch
    from io import BytesIO
    
    # Create PDF in memory
    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=letter)
    width, height = letter
    
    # Title
    c.setFont("Helvetica-Bold", 16)
    title_text = swim_meet_name
    text_width = c.stringWidth(title_text, "Helvetica-Bold", 16)
    c.drawString((width - text_width)/2, height - 0.5*inch, title_text)
    
    # Find maximum number of heats to determine column layout
    max_heats = max(num_heats for _, _, num_heats in events) if events else 1
    
    # Table setup
    event_col_width = 3*inch
    heat_col_width = 0.8*inch
    start_x = 0.5*inch
    header_y = height - 1.2*inch
    
    # Draw table headers
    c.setFont("Helvetica-Bold", 10)
    c.drawString(start_x, header_y, "Event")
    
    for heat_num in range(1, max_heats + 1):
        x_pos = start_x + event_col_width + (heat_num - 1) * heat_col_width
        c.drawString(x_pos, header_y, f"Heat {heat_num}")
    
    # Draw header underline
    header_line_y = header_y - 3
    c.line(start_x, header_line_y, start_x + event_col_width + max_heats * heat_col_width, header_line_y)
    
    # Starting position for data rows
    y_position = header_y - 0.3*inch
    line_height = 0.25*inch
    
    # Set font for event entries
    c.setFont("Helvetica", 10)
    
    for event_number, event_name, num_heats in events:
        # Check if we need a new page
        if y_position < 1*inch:
            c.showPage()
            # Redraw headers on new page
            y_position = height - 0.5*inch
            c.setFont("Helvetica-Bold", 10)
            c.drawString(start_x, y_position, "Event")
            for heat_num in range(1, max_heats + 1):
                x_pos = start_x + event_col_width + (heat_num - 1) * heat_col_width
                c.drawString(x_pos, y_position, f"Heat {heat_num}")
            header_line_y = y_position - 3
            c.line(start_x, header_line_y, start_x + event_col_width + max_heats * heat_col_width, header_line_y)
            y_position -= 0.3*inch
            c.setFont("Helvetica", 10)
        
        # Draw event number and name
        event_text = f"{event_number}: {event_name}"
        c.drawString(start_x, y_position, event_text)
        
        # Draw underlines only for existing heats
        for heat in range(1, num_heats + 1):
            x_pos = start_x + event_col_width + (heat - 1) * heat_col_width
            underline_width = 0.6*inch
            c.line(x_pos, y_position - 2, x_pos + underline_width, y_position - 2)
        
        # Move to next event row
        y_position -= line_height
    
    c.save()
    buffer.seek(0)
    return buffer


if __name__ == '__main__':
    app.run(debug=True)