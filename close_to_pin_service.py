import pandas as pd
from functools import reduce
from datetime import datetime
import math
from utils import read_csv_files, convert_time_to_hundredths, add_event_names_column, read_csv_with_metadata


class CloseToPinService:
    """Service class for close to pin analysis."""

    def __init__(self):
        pass

    def convert_hundredths_to_time_diff(self, hundredths):
        """Convert hundredths to time, returning empty string for positive differences."""
        negative = False
        if hundredths < 0:
            negative = True
            hundredths = abs(hundredths)
        total_seconds = hundredths / 100.0
        minutes = int(total_seconds // 60)
        hours = int(minutes // 60)
        seconds = int(total_seconds % 60)
        hundredths = int((total_seconds - minutes * 60 - seconds) * 100)
        if hours > 0:
            if negative:
                return f"-{hours:02}:{int(minutes % 60)}:{seconds:02}"
            else:
                return f""
        else:
            if negative:
                return f"-{minutes:02}:{seconds:02}.{hundredths:02}"
            else:
                return f""

    def clean_up_events(self, row):
        """Clean up silver diff for qualified swimmers."""
        if row['qualified_for'] == "Gold" or row['qualified_for'] == "Silver":
            return 0
        else:
            return row['silver_diff_hund']

    def determine_next_qualifier(self, row):
        """Determine the next qualification level time difference."""
        if row['qualified_for'] == "Gold":
            return ""
        if row['qualified_for'] == "Silver":
            return row['gold_diff_hund']
        if row['qualified_for'] == "Bronze":
            return row['silver_diff_hund']

    def determine_champ_meet(self, row):
        """Determine which championship meet the swimmer qualifies for."""
        if row['gold_diff_hund'] > 0:
            return "Gold"
        if row['silver_diff_hund'] > 0:
            return f'Silver ({row["gold_diff"]})'
        
        if "men" in row['Event_name'].lower():
            return f'Silver ({row["gold_diff"]})'
        else:
            return f'Bronze ({row["silver_diff"]})'

    def compare_with_standards(self, df, units='yards'):
        """Compare swimmer times with current standards."""
        # Choose the appropriate time standard columns based on units
        gold_col = 'gold_y' if units == 'yards' else 'gold_s'
        silver_col = 'silver_y' if units == 'yards' else 'silver_s'
        
        df['gold_hund'] = df[gold_col].apply(lambda x: convert_time_to_hundredths(x))
        df['silver_hund'] = df[silver_col].apply(lambda x: convert_time_to_hundredths(x))
        
        df['gold_diff_hund'] = df['gold_hund'] - df['ConvertedHundredths']
        df['silver_diff_hund'] = df['silver_hund'] - df['ConvertedHundredths']
        
        df['gold_diff'] = df['gold_diff_hund'].apply(lambda x: self.convert_hundredths_to_time_diff(x))
        df['silver_diff'] = df['silver_diff_hund'].apply(lambda x: self.convert_hundredths_to_time_diff(x))
        
        df['qualified_for'] = df.apply(self.determine_champ_meet, axis=1)
        df['next_qualifier'] = df.apply(self.determine_next_qualifier, axis=1)
        df['silver_diff_hund'] = df.apply(self.clean_up_events, axis=1)
        
        # Drop only the intermediate calculation columns, keep all standard columns
        columns_to_drop = ['gold_hund', 'silver_hund', 'gold_diff_hund', 'silver_diff_hund', 'ConvertedHundredths']
        
        # Only drop columns that exist in the dataframe
        existing_columns_to_drop = [col for col in columns_to_drop if col in df.columns]
        if 'Time' in df.columns:
            existing_columns_to_drop.append('Time')
        
        df = df.drop(existing_columns_to_drop, axis=1)
        
        return df.sort_values(['LastName', 'FirstName'], ascending=[True, True])

    def analyze_close_to_pin(self, best_times_file, current_standards_file, units='yards'):
        """Main analysis method for close to pin analysis."""
        # Read the CSV file
        best_times = pd.read_csv(best_times_file)
        
        # Check if required columns exist
        if 'Event' not in best_times.columns:
            raise ValueError(f"Required column 'Event' not found. Available columns: {best_times.columns.tolist()}")
        
        if 'AgeGroup' not in best_times.columns:
            raise ValueError(f"Required column 'AgeGroup' not found. Available columns: {best_times.columns.tolist()}")
        
        # Create event names from AgeGroup and Event fields
        event_parts = best_times['Event'].str.split(' ', n=1, expand=True)
        best_times = best_times.assign(
            distance=event_parts[0],
            stroke=event_parts[1],
            age_group=best_times['AgeGroup'].astype(str)
        )
        
        # Use our utility to create the Event_name field
        best_times = add_event_names_column(best_times)
        best_times = best_times.drop(['AgeGroup', 'Event', 'Age', 'Date', 'SwimMeet', 'age_group', 'distance', 'stroke'], axis=1)

        # Get the current standards, generate event_names
        current_standards = read_csv_with_metadata(current_standards_file)
        # Use our utility to create event names
        current_standards = add_event_names_column(current_standards)
        
        # Keep both yards and meters columns for comparison
        current_standards = current_standards.drop(['age_group', 'distance', 'stroke'], axis=1)

        best_times_with_standards = pd.merge(best_times, current_standards, on='Event_name')
        compared_times = self.compare_with_standards(best_times_with_standards, units)
        
        # Column order and renaming - use the appropriate columns based on units
        gold_col = 'gold_y' if units == 'yards' else 'gold_s'
        silver_col = 'silver_y' if units == 'yards' else 'silver_s'
        gold_label = f'Gold Time ({"Yards" if units == "yards" else "Meters"})'
        silver_label = f'Silver Time ({"Yards" if units == "yards" else "Meters"})'
        
        col_order = ["LastName", "FirstName", "Event_name", "ConvertedTime", "qualified_for", gold_col, silver_col]
        compared_times = compared_times[col_order]
        compared_times.rename(columns={
            'LastName': 'Last Name', 
            'FirstName': 'First Name', 
            'Event_name': 'Event', 
            'ConvertedTime': 'Best Time', 
            'qualified_for': 'Championship Meet', 
            gold_col: gold_label,
            silver_col: silver_label
        }, inplace=True)

        return compared_times