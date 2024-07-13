import pandas as pd
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

# Function to get the 90th percentile threshold and count of values meeting/exceeding the threshold for each unique event
def get_percentile_summary(df, standard, pct):
    # Group by 'age_group', 'distance', and 'stroke' to create unique events
    grouped = df.groupby(['age_group', 'distance', 'stroke'])
    
    # Lists to store the results
    event_names = []
    thresholds = []
    thresholds_meters = []
    counts = []
    
    # Process each group
    for name, group in grouped:
        # if standard.lower() == "silver":
        #     if "15-18" in name[0]:
        #         print("need to do something about silver events for 15-18")

        # Calculate the 90th percentile threshold
        threshold_value = group['converted_hundredths'].quantile(pct)
        # Convert the threshold value to MM:SS.hh format
        threshold = convert_hundredths_to_time(threshold_value)
        # # Count the number of entries meeting or exceeding the threshold
        count = (group['converted_hundredths'] >= threshold_value).sum()
        
        # Create the event name
        event_name = f"{name[0]}_{name[1]}_{name[2]}"
        
        # Store the results
        event_names.append(event_name)
        thresholds.append(threshold)
        thresholds_meters.append(convert_hundredths_to_time(threshold_value * 1.11))
        counts.append(count)
    
    # Create a summary DataFrame
    summary_df = pd.DataFrame({
        'Event_name': event_names,
        f'new_{standard}_y': thresholds,
        f'new_{standard}_s' : thresholds_meters
    })
    
    return summary_df

def add_event_names(df):
    # Group by 'age_group', 'distance', and 'stroke' to create unique events
    grouped = df.groupby(['age_group', 'distance', 'stroke'])

    # Lists to store the results
    event_names = []

    for name, group in grouped:
        # Create the event name
        event_name = f"{name[0]}_{name[1]}_{name[2]}"

        # Store the results
        event_names.append(event_name)
    
    # Create a summary DataFrame
    summary_df = pd.DataFrame({
        'Event_name': event_names
    })
    df['Event_name'] = event_names

    return 

def get_qualifiers_summary(df, proposed_times):
    # Group by 'age_group', 'distance', and 'stroke' to create unique events
    grouped = df.groupby(['age_group', 'distance', 'stroke'])

    season = df['date'].iloc[0]
    _dt = datetime.strptime(season, '%m/%d/%y')
    
    # Lists to store the results
    event_names = []
    gold_qualifiers = []
    silver_qualifiers = []
    bronze_qualifiers = []
    gold_event_times = []
    silver_event_times = []
    bronze_event_times = []
    
    # Process each group
    for name, group in grouped:
        # Create the event name
        event_name = f"{name[0]}_{name[1]}_{name[2]}"

        #lookup time standards
        _row = proposed_times.index[proposed_times['Event_name']==event_name].tolist()
        _gold = proposed_times.loc[_row, "new_gold_y"]
        _silver = proposed_times.loc[_row, "new_silver_y"]
        
        _gold_hundredths = convert_time_to_hundredths(_gold.item())
        _silver_hundredths = convert_time_to_hundredths(_silver.item())

        # # Count the number of entries meeting or exceeding the threshold
        gold_count = (group['converted_hundredths'] <= _gold_hundredths).sum()
        silver_count = (group['converted_hundredths'] <= _silver_hundredths).sum()
        bronze_count = (group['converted_hundredths'] > _silver_hundredths).sum()

        _gold_heats = math.ceil(gold_count/6)
        _silver_heats = math.ceil((silver_count - gold_count)/6)
        _bronze_heats = math.ceil(bronze_count/6)

        # Store the results
        event_names.append(event_name)
        gold_qualifiers.append(gold_count)
        silver_qualifiers.append(silver_count - gold_count)
        bronze_qualifiers.append(bronze_count)
        gold_event_times.append(_gold_heats * (_gold_hundredths + 1500) + 3000)
        silver_event_times.append(_silver_heats * (_silver_hundredths + 1500) + 3000)
        bronze_event_times.append(_bronze_heats * (_silver_hundredths + 1500) + 3000)
        

    # Create a summary DataFrame
    times_df = pd.DataFrame({
        'Event_name': event_names,
        f'gold_qualifiers-{_dt.year}' : gold_qualifiers,
        f'silver_qualifiers-{_dt.year}' : silver_qualifiers,
        f'bronze_qualifiers-{_dt.year}' : bronze_qualifiers,
        f'gold_est_duration-{_dt.year}' : gold_event_times,
        f'silver_est_duration-{_dt.year}' : silver_event_times,
        f'bronze_est_duration-{_dt.year}' : bronze_event_times
    })

    gold_duration = times_df[f'gold_est_duration-{_dt.year}'].sum()
    silver_duration = times_df[f'silver_est_duration-{_dt.year}'].sum()
    bronze_duration = times_df[f'bronze_est_duration-{_dt.year}'].sum()
    print(f'Estimated meet run time for {_dt.year} Gold Meet: {convert_hundredths_to_time(gold_duration)}')
    print(f'Estimated meet run time for {_dt.year} Silver Meet: {convert_hundredths_to_time(silver_duration / 2)}')
    print(f'Estimated meet run time for {_dt.year} Bronze Meet: {convert_hundredths_to_time(bronze_duration / 2)}')
    
    return times_df, _dt.year

def get_new_time_diffs(df):
    df['gold_new_hund_y'] = df['new_gold_y'].apply(lambda x: convert_time_to_hundredths(x))
    df['gold_new_hund_s'] = df['new_gold_s'].apply(lambda x: convert_time_to_hundredths(x))
    df['gold_hund_s'] = df['gold_s'].apply(lambda x: convert_time_to_hundredths(x))
    df['gold_hund_y'] = df['gold_y'].apply(lambda x: convert_time_to_hundredths(x))
    df['silver_new_hund_y'] = df['new_silver_y'].apply(lambda x: convert_time_to_hundredths(x))
    df['silver_new_hund_s'] = df['new_silver_s'].apply(lambda x: convert_time_to_hundredths(x))
    df['silver_hund_s'] = df['silver_s'].apply(lambda x: convert_time_to_hundredths(x))
    df['silver_hund_y'] = df['silver_y'].apply(lambda x: convert_time_to_hundredths(x))

    df['gold_diff_y_hund'] = df['gold_new_hund_y'] - df['gold_hund_y']
    df['gold_diff_s_hund'] = df['gold_new_hund_s'] - df['gold_hund_s']
    df['silver_diff_y_hund'] = df['silver_new_hund_y'] - df['silver_hund_y']
    df['silver_diff_s_hund'] = df['silver_new_hund_s'] - df['silver_hund_s']

    df['gold_diff_y'] = df['gold_diff_y_hund'].apply(lambda x: convert_hundredths_to_time(x))
    df['gold_diff_s'] = df['gold_diff_s_hund'].apply(lambda x: convert_hundredths_to_time(x))
    df['silver_diff_y'] = df['silver_diff_y_hund'].apply(lambda x: convert_hundredths_to_time(x))
    df['silver_diff_s'] = df['silver_diff_s_hund'].apply(lambda x: convert_hundredths_to_time(x))

    

    df = df.drop(['gold_new_hund_y', 'gold_new_hund_s', 'gold_hund_y', 'gold_hund_s', 'silver_new_hund_y', 'silver_new_hund_s', 'silver_hund_y', 'silver_hund_s'], axis=1)
    return df

def clear_teen_event(row, col):
    if "15-18" in row["Event_name"]:
        return ""
    else:
        return row[col]

def clean_up_events(df):
    columns_to_clean = ["new_silver_y", "silver_diff_y", "new_silver_s", "silver_diff_y"]

    for _col in columns_to_clean:
        df[_col] = df.apply(clear_teen_event, col=_col, axis=1)

    return df

def main():
    # Adjust the file path pattern as needed (e.g., '*.csv' for all CSV files in the directory)
    file_path_pattern = './gasl*.csv'
    
    # Read the CSV files
    df = read_csv_files(file_path_pattern)

    # Get the current standards, generate event_names
    current_standards = read_csv_files('./current_standards.csv')
    current_standards = current_standards.assign(Event_name = current_standards.age_group.astype(str) + '_' + \
        current_standards.distance.astype(str) + '_' + current_standards.stroke.astype(str))
    current_standards = current_standards.drop(['age_group', 'distance', 'stroke'], axis=1)
    
    # get input for percentials
    gold_pct = float(input('Enter percentile for Gold Meet Standard (default: .20): ').strip() or ".2")
    silver_pct = float(input('Enter percentile for Silver Meet Standard (default: .60): ').strip() or ".6")

    standards = {
        "gold": gold_pct,
        "silver" : silver_pct
    }
    
    summary = []
    for standard, pct in standards.items():
        # Get the percentile summary
        summary_df = get_percentile_summary(df, standard, pct)
        summary.append(summary_df)

    combined=reduce(lambda x, y: pd.merge(x, y, on = 'Event_name'), summary)

    add_current = combined.merge(current_standards, on = "Event_name")
    proposed_with_differences = get_new_time_diffs(add_current)

    # remove any proposed times for silver 15-18 events
    proposed_with_differences = clean_up_events(proposed_with_differences)

    # column order
    col_order = ["Event_name", "gold_y", "new_gold_y", "gold_diff_y", "gold_s", "new_gold_s", "gold_diff_s", "silver_y", "new_silver_y", "silver_diff_y", "silver_s", "new_silver_s", "silver_diff_y"]
    proposed_with_differences = proposed_with_differences[col_order]
    proposed_with_differences.rename(columns={"Event_name" : "Event", 
        "gold_y" : "Current Gold Time (yards)", 
        "new_gold_y" : "Proposed Gold Time (yards)", 
        "gold_diff_y" : "Gold delta (yards)", 
        "gold_s" : "Current Gold Time (meters)", 
        "new_gold_s" : "Proposed Gold Time (meters)", 
        "gold_diff_s" : "Gold delta (meters)", 
        "silver_y" : "Current Silver Time (yards)", 
        "new_silver_y" : "Proposed Silver Time (yards)", 
        "silver_diff_y" : "Silver delta (yards)", 
        "silver_s" : "Current Silver Time (meters)", 
        "new_silver_s" : "Proposed Silver Time (meters)", 
        "silver_diff_y" : "Silver delta (meters)"}, inplace=True)

    proposed_with_differences.to_csv('proposed_new_standards.csv', index=False)

    # determine the number of qualifiers for each event based on new calculated standards
    qualifiers_list = []
    qualifiers_list.append(combined)
    
    print('\n\nBased on the newly calculated time standards, we estimate how long each meet would take:')
    for csv in glob.glob(file_path_pattern):
        times = pd.read_csv(csv)
        qualifiers, season = get_qualifiers_summary(times, combined)
        qualifiers_list.append(qualifiers)
        qualifiers.to_csv(f'{season}-qualifiers.csv', index=False)

# Main execution
if __name__ == "__main__":
    main()