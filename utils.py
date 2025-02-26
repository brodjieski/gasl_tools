import pandas as pd
import glob
from datetime import datetime
import math


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
            return f"{hours:02}:{int(minutes % 60)}:{seconds:02}"
    else:
        if negative:
            return f"-{minutes:02}:{seconds:02}.{hundredths:02}"
        else:
            return f"{minutes:02}:{seconds:02}.{hundredths:02}"


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


# Generate standardized event names from age group, distance, and stroke
def create_event_name(age_group, distance, stroke):
    return f"{age_group}_{distance}_{stroke}"


# Add event names to a dataframe with age_group, distance, and stroke columns
def add_event_names_column(df):
    df['Event_name'] = df.apply(
        lambda row: create_event_name(row['age_group'], row['distance'], row['stroke']), 
        axis=1
    )
    return df