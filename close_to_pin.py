import pandas as pd
import argparse
from functools import reduce
from datetime import datetime
import math
import glob

# Function to read CSV files and concatenate them into a single DataFrame
def read_csv_files(file_path_pattern):
    # Use glob to find all files matching the pattern
    files = glob.glob(file_path_pattern)
    # Read all files and concatenate them into a single DataFrame
    df_list = [pd.read_csv(file) for file in files]
    combined_df = pd.concat(df_list, ignore_index=True)
    return combined_df

# Function to convert hundredths of a second to MM:SS.hh format
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

def convert_time_to_hundredths(time):
    if not isinstance(time, str):
        return 0
    if ":" not in time:
        time = f'00:{time}'
    minutes = int(time.split(":")[0])
    seconds_with_hundredths = time.split(":")[1]
    
    try:
        seconds = int(int(seconds_with_hundredths.split('.')[0]) + (minutes * 60))
    except:
        print(time)
    hundredths = int(seconds_with_hundredths.split('.')[1])
    
    total_hundredths = int((hundredths + (seconds * 100)))
    return total_hundredths

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
    best_times = best_times.assign(Event_name = best_times.AgeGroup.astype(str) + '_' + \
        best_times.Event.str.split(' ',n=1, expand=True)[0] + '_' + best_times.Event.str.split(' ',n=1, expand=True)[1])
    best_times = best_times.drop(['AgeGroup', 'Event', 'Age', 'Date', 'SwimMeet'], axis=1)

    # Get the current standards, generate event_names
    current_standards = read_csv_files('./current_standards.csv')
    current_standards = current_standards.assign(Event_name = current_standards.age_group.astype(str) + '_' + \
        current_standards.distance.astype(str) + '_' + current_standards.stroke.astype(str))
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