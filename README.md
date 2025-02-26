# GASL Tools

Scripts used for analyzing swim team data from Swimtopia for the Greater Atlanta Swim League.

## Overview

This project provides tools for swim coaches and administrators to:

1. Analyze swimmers' times against championship qualification standards
2. Generate new time standards based on historical data
3. Predict meet durations and attendance

## Setup

1. Install Python 3.6 or higher
2. Clone this repository
3. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

## Available Tools

### Close to Pin Analysis

Compares swimmers' best times against championship qualification standards to determine which championship meet they qualify for.

```
python close_to_pin.py <best_times_file_pattern>
```

The script will:
1. Read best times files from Swimtopia
2. Compare each time to the Gold/Silver standards
3. Generate a CSV showing which championship meet each swimmer qualifies for
4. Show how close they are to the next qualification level

### Time Standards Generator

Calculates proposed time standards based on percentile distributions of historical swim data.

```
python gasl_time_standards.py
```

The script will:
1. Prompt for percentile cutoffs for Gold/Silver qualification
2. Analyze historical swim data
3. Calculate proposed standards 
4. Estimate meet durations and athlete counts per team
5. Output comprehensive CSVs with the new standards

## Input Data

This project expects CSV files with the following naming conventions:
- `gasl*.csv` - Historical swim times from meets
- `current_standards.csv` - Current qualification standards

## Project Structure

- `utils.py` - Shared utility functions
- `constants.py` - Shared constants and configuration
- `close_to_pin.py` - Close-to-pin analysis script
- `gasl_time_standards.py` - Time standards generation script
- `requirements.txt` - Python dependencies

## Contributing

Please maintain the coding style as specified in the CLAUDE.md document.