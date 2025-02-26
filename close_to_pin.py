import pandas as pd
import argparse
from functools import reduce
from datetime import datetime
import math
import glob
from utils import read_csv_files, convert_time_to_hundredths

# Modified version of convert_hundredths_to_time that returns empty string for positive differences
def convert_hundredths_to_time(hundredths):
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

def clean_up_events(row):
    if row['qualified_for'] == "Gold" or row['qualified_for'] == "Silver":
        return 0
    else:
        return row['silver_diff_hund']

def determine_next_qualifier(row):
    if row['qualified_for'] == "Gold":
        return ""
    if row['qualified_for'] == "Silver":
        return row['gold_diff_hund']
    if row['qualified_for'] == "Bronze":
        return row['silver_diff_hund']

def determine_champ_meet(row):
    if row['gold_diff_hund'] > 0:
        return "Gold"
    if row['silver_diff_hund'] > 0:
        return f'Silver ({row["gold_diff"]})'
    
    if "men" in row['Event_name']:
        return f'Silver ({row["gold_diff"]})'
    else:
        return f'Bronze ({row["silver_diff"]})'

def compare_with_standards(df):
    df['gold_hund'] = df['gold_y'].apply(lambda x: convert_time_to_hundredths(x))
    df['silver_hund'] =  df['silver_y'].apply(lambda x: convert_time_to_hundredths(x))
    
    df['gold_diff_hund'] = df['gold_hund'] - df['ConvertedHundredths']

    df['silver_diff_hund'] = df['silver_hund'] - df['ConvertedHundredths']

    df['gold_diff'] = df['gold_diff_hund'].apply(lambda x: convert_hundredths_to_time(x))
    df['silver_diff'] = df['silver_diff_hund'].apply(lambda x: convert_hundredths_to_time(x))

    df['qualified_for'] = df.apply(determine_champ_meet, axis=1)
    df['next_qualifier'] = df.apply(determine_next_qualifier, axis=1)
    df['silver_diff_hund'] = df.apply(clean_up_events, axis=1)

    df = df.drop(['gold_hund', 'silver_hund', 'gold_diff_hund', 'silver_diff_hund', 'Time', 'ConvertedHundredths'], axis=1)

    return df.sort_values(['LastName', 'FirstName'], ascending=[True, True])
    
def main():
    parser = argparse.ArgumentParser(__file__)

    parser.add_argument("file", default=None, help="Best times file exported from Swimtopia (required)")

    options = parser.parse_args()

    # Read the CSV file
    best_times = read_csv_files(options.file)
    # Create event names from AgeGroup and Event fields
    best_times = best_times.assign(
        distance=best_times.Event.str.split(' ',n=1, expand=True)[0],
        stroke=best_times.Event.str.split(' ',n=1, expand=True)[1],
        age_group=best_times.AgeGroup.astype(str)
    )
    # Use our utility to create the Event_name field
    from utils import add_event_names_column
    best_times = add_event_names_column(best_times)
    best_times = best_times.drop(['AgeGroup', 'Event', 'Age', 'Date', 'SwimMeet', 'age_group', 'distance', 'stroke'], axis=1)

    # Get the current standards, generate event_names
    current_standards = read_csv_files('./current_standards.csv')
    # Use our utility to create event names
    from utils import add_event_names_column
    current_standards = add_event_names_column(current_standards)
    current_standards = current_standards.drop(['age_group', 'distance', 'stroke','gold_s', 'silver_s'], axis=1)

    best_times_with_standards = pd.merge(best_times, current_standards, on='Event_name')
    compared_times = compare_with_standards(best_times_with_standards)
    
    # column order
    col_order = ["LastName", "FirstName", "Event_name", "ConvertedTime", "qualified_for", "gold_y", "silver_y"]
    compared_times = compared_times[col_order]
    compared_times.rename(columns={'LastName': 'Last Name', 'FirstName': 'First Name', 'Event_name': 'Event', 'ConvertedTime': 'Best Time', 'qualified_for': 'Championship Meet', 'gold_diff': 'Gold Difference', 'silver_diff': 'Silver Difference', 'gold_y': 'Gold Time', 'silver_y': 'Silver Time'}, inplace=True)

    compared_times.to_csv('close_to_pin.csv', index=False)

# Main execution
if __name__ == "__main__":
    main()