# GASL Tools Guidelines

## Commands
- Run scripts: `python <script_name>.py`
- Run close_to_pin.py: `python close_to_pin.py <best_times_file_pattern>`
- Run gasl_time_standards.py: `python gasl_time_standards.py` (interactive prompts will follow)
- Install dependencies: `pip install -r requirements.txt`

## Code Style Guidelines
- **Imports**: Standard library first, then third-party, then local modules; grouped and alphabetized
- **Formatting**: 4-space indentation, use f-strings for string formatting
- **Types**: Duck typing, with clear function documentation
- **Naming**: 
  - snake_case for functions, variables, and files
  - CamelCase for classes
  - Functions that process data should have descriptive names
- **Error Handling**: Use try/except blocks with specific exceptions where possible
- **Comments**: Include docstrings for functions and modules describing purpose and parameters
- **DataFrame Operations**: Prefer vectorized operations over loops for pandas DataFrames

The codebase primarily handles CSV data processing for swim team analysis, focusing on time standards and qualification metrics.