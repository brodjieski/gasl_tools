import pandas as pd
from functools import reduce
from datetime import datetime
import math
import glob
import hashlib
from utils import read_csv_files, convert_hundredths_to_time, convert_time_to_hundredths, create_event_name, add_event_names_column

# Function to get the 90th percentile threshold and count of values meeting/exceeding the threshold for each unique event
def get_percentile_summary(df, standard, pct):
    # Group by 'age_group', 'distance', and 'stroke' to create unique events
    grouped = df.groupby(['age_group', 'distance', 'stroke'])
    # print(df.columns.tolist())
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

def dedup_entries(df):
    # Define meet ranking order using pd.Categorical
    df['qualified_meet'] = pd.Categorical(df['qualified_meet'], categories=["GOLD", "SILVER", "BRONZE"], ordered=True)

    # Sort by swimmer, meet priority, and time
    df_sorted = df.sort_values(by=['athlete_id', 'qualified_meet', 'converted_hundredths'])

    # For each swimmer, keep only the highest-priority meet by dropping duplicates based on swimmer and meet priority order
    df_highest_priority = (
            df_sorted[df_sorted.groupby('athlete_id')['qualified_meet'].transform('min') == df_sorted['qualified_meet']]
        )

    # For each swimmer-meet combination, keep only the top 3 fastest times
    df_final = pd.concat(
            [group.sample(n=min(3, len(group)), random_state=1) for _, group in df_highest_priority.groupby(['athlete_id', 'qualified_meet'], observed=False)]
        ).reset_index(drop=True)
    
    return df_final
def add_event_names(df):
    # Use the utility function to add event names to the DataFrame
    add_event_names_column(df)
    return

def get_team_attendance_summary(df):
    summary = (
        df.groupby(['team_abbr', 'qualified_meet'], observed=False)['athlete_id']
            .nunique()
            .unstack(fill_value=0)  # Fill missing values with 0
        )
    # Add a "Total" column for each team (row-wise sum)
    summary['Total'] = summary.sum(axis=1)

    # Add a row of column totals and an overall total
    summary.loc['Total'] = summary.sum()
    print(summary)


def get_estimated_meet_duration(df, season, proposed_times, heat_time, event_delay):
    grouped = df.groupby(['age_group', 'distance', 'stroke'])

    # Lists to store the results
    event_names = []
    gold_qualifiers = []
    gold_heats = []
    silver_qualifiers = []
    silver_heats = []
    bronze_qualifiers = []
    bronze_heats = []
    gold_event_times = []
    silver_event_times = []
    bronze_event_times = []

    heat_time_hundredths = heat_time * 100
    event_delay_hundredths = event_delay * 100

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
        gold_count = (group['qualified_meet'] == "GOLD").sum()
        silver_count = (group['qualified_meet'] == "SILVER").sum()
        bronze_count = (group['qualified_meet'] == "BRONZE").sum()

        _gold_heats = math.ceil(gold_count/6)
        _silver_heats = math.ceil(silver_count/6)
        _bronze_heats = math.ceil(bronze_count/6)

        # print(f'{season} GOLD: Number of heats: {_gold_heats} for {event_name}')
        # print(f'{season} SILVER: Number of heats: {_silver_heats} for {event_name}')
        # print(f'{season} BRONZE: Number of heats: {_bronze_heats} for {event_name}')

        # Store the results
        event_names.append(event_name)
        gold_qualifiers.append(gold_count)
        silver_qualifiers.append(silver_count)
        bronze_qualifiers.append(bronze_count)
        gold_heats.append(_gold_heats)
        silver_heats.append(_silver_heats)
        bronze_heats.append(_bronze_heats)
        

        # add 15 seconds between heats, 30 seconds between events, add 20 minutes for relays
        gold_event_times.append(_gold_heats * (_gold_hundredths + heat_time_hundredths) + event_delay_hundredths)
        silver_event_times.append(_silver_heats * (_silver_hundredths + heat_time_hundredths) + event_delay_hundredths)
        bronze_event_times.append(_bronze_heats * (_silver_hundredths + heat_time_hundredths) + event_delay_hundredths)
    
    # Create a summary DataFrame
    times_df = pd.DataFrame({
        'Event_name': event_names,
        f'gold_qualifiers-{season}' : gold_qualifiers,
        f'gold_heats-{season}' : gold_heats,
        f'gold_est_duration-{season}' : gold_event_times,
        f'silver_qualifiers-{season}' : silver_qualifiers,
        f'silver_heats-{season}' : silver_heats,
        f'silver_est_duration-{season}' : silver_event_times,
        f'bronze_qualifiers-{season}' : bronze_qualifiers,
        f'bronze_heats-{season}' : bronze_heats,
        f'bronze_est_duration-{season}' : bronze_event_times
    })

    return times_df

def  get_qualifiers_summary(df, proposed_times, current_times, heat_time, event_time):
    # Group by 'age_group', 'distance', and 'stroke' to create unique events
    grouped = df.groupby(['age_group', 'distance', 'stroke'])
    season = df['date'].iloc[0]
    _dt = datetime.strptime(season, '%m/%d/%y')
    # Lists to store the results
    entries = pd.DataFrame()
    entries_old = pd.DataFrame()
    
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

        _gold_peeps = group[group['converted_hundredths'] <= _gold_hundredths]
        _silver_peeps = group[(group['converted_hundredths'] <= _silver_hundredths) & (group['converted_hundredths'] >= _gold_hundredths)]
        _bronze_peeps = group[group['converted_hundredths'] > _silver_hundredths]
                
        _gold_entries = _gold_peeps[_gold_peeps['converted_hundredths'] <= _gold_hundredths].assign(qualified_meet='GOLD')
        _silver_entries = _silver_peeps[_silver_peeps['converted_hundredths'] <= _silver_hundredths].assign(qualified_meet='SILVER')
        if "15-18" in event_name:
            _bronze_entries = _bronze_peeps[_bronze_peeps['converted_hundredths'] > _silver_hundredths].assign(qualified_meet='SILVER')
        else:
            _bronze_entries = _bronze_peeps[_bronze_peeps['converted_hundredths'] > _silver_hundredths].assign(qualified_meet='BRONZE')

        entries = pd.concat([entries, _gold_entries], ignore_index=True) 
        entries = pd.concat([entries, _silver_entries], ignore_index=True) 
        entries = pd.concat([entries, _bronze_entries], ignore_index=True)  

        #lookup time standards
        _row = current_times.index[current_times['Event_name']==event_name].tolist()
        _gold = current_times.loc[_row, "gold_y"]
        _silver = current_times.loc[_row, "silver_y"]
        
        _gold_hundredths = convert_time_to_hundredths(_gold.item())
        _silver_hundredths = convert_time_to_hundredths(_silver.item())

        _gold_peeps = group[group['converted_hundredths'] <= _gold_hundredths]
        _silver_peeps = group[(group['converted_hundredths'] <= _silver_hundredths) & (group['converted_hundredths'] >= _gold_hundredths)]
        _bronze_peeps = group[group['converted_hundredths'] > _silver_hundredths]
                
        _gold_entries = _gold_peeps[_gold_peeps['converted_hundredths'] <= _gold_hundredths].assign(qualified_meet='GOLD')
        _silver_entries = _silver_peeps[_silver_peeps['converted_hundredths'] <= _silver_hundredths].assign(qualified_meet='SILVER')
        if "15-18" in event_name:
            _bronze_entries = _bronze_peeps[_bronze_peeps['converted_hundredths'] > _silver_hundredths].assign(qualified_meet='SILVER')
        else:
            _bronze_entries = _bronze_peeps[_bronze_peeps['converted_hundredths'] > _silver_hundredths].assign(qualified_meet='BRONZE')

        entries_old = pd.concat([entries_old, _gold_entries], ignore_index=True) 
        entries_old = pd.concat([entries_old, _silver_entries], ignore_index=True) 
        entries_old = pd.concat([entries_old, _bronze_entries], ignore_index=True) 


    # pass the entries to a function that will determine meet length
    cleaned_up_entries = dedup_entries(entries)

    cleaned_up_entries_sorted = cleaned_up_entries.sort_values(by=['athlete_id'])
    cleaned_up_entries_sorted.drop_duplicates(subset=['athlete_id'], keep='first', inplace=True)

    cleaned_up_entries_old = dedup_entries(entries_old)

    cleaned_up_entries_old_sorted = cleaned_up_entries_old.sort_values(by=['athlete_id'])
    cleaned_up_entries_old_sorted.drop_duplicates(subset=['athlete_id'], keep='first', inplace=True)
    
    
    number_of_gold_athletes = (cleaned_up_entries_sorted['qualified_meet'] == "GOLD").sum()
    number_of_silver_athletes = (cleaned_up_entries_sorted['qualified_meet'] == "SILVER").sum()
    number_of_bronze_athletes = (cleaned_up_entries_sorted['qualified_meet'] == "BRONZE").sum()

    times_df=get_estimated_meet_duration(cleaned_up_entries, _dt.year, proposed_times, heat_time, event_time)

    gold_duration = times_df[f'gold_est_duration-{_dt.year}'].sum() + 12000
    silver_duration = times_df[f'silver_est_duration-{_dt.year}'].sum() + 24000
    bronze_duration = times_df[f'bronze_est_duration-{_dt.year}'].sum() + 24000
    _g_q=times_df[f'gold_qualifiers-{_dt.year}'].sum()
    _s_q=(times_df[f'silver_qualifiers-{_dt.year}'].sum() / 2)
    _b_q=(times_df[f'bronze_qualifiers-{_dt.year}'].sum() / 2)
    
    print(f'\nEstimated run time for {_dt.year} Gold Meet: {convert_hundredths_to_time(gold_duration)} ({_g_q} entries, {number_of_gold_athletes} athletes)')
    print(f'Estimated run time for each {_dt.year} Silver Meet: {convert_hundredths_to_time(silver_duration / 2)} ({_s_q} entries per meet, {number_of_silver_athletes} athletes total)')
    print(f'Estimated run time for each {_dt.year} Bronze Meet: {convert_hundredths_to_time(bronze_duration / 2)} ({_b_q} entries per meet, {number_of_bronze_athletes} athletes total)')
    print('\n')
    get_team_attendance_summary(cleaned_up_entries_sorted)
    print('\n Compared to the meet summaries from the current time standards:')
    get_team_attendance_summary(cleaned_up_entries_old_sorted)
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
    columns_to_clean = ["new_silver_y", "silver_diff_y", "new_silver_s", "silver_diff_s"]

    for _col in columns_to_clean:
        df[_col] = df.apply(clear_teen_event, col=_col, axis=1)

    return df

def main():
    # Adjust the file path pattern as needed (e.g., '*.csv' for all CSV files in the directory)
    file_path_pattern = './gasl*.csv'
    
    # Read the CSV files
    df = read_csv_files(file_path_pattern)

    # add a column that can be an athlete identifier
    df['athlete_id'] = df.apply(lambda row: hashlib.sha256(f"{row['first_name']}_{row['last_name']}_{row['team_abbr']}_{row['age']}".encode()).hexdigest(), axis=1)

    # Get the current standards, generate event_names
    current_standards = read_csv_files('./current_standards.csv')
    current_standards = add_event_names_column(current_standards)
    current_standards = current_standards.drop(['age_group', 'distance', 'stroke'], axis=1)
    
    # get input for percentials
    gold_pct = float(input('Enter percentile for Gold Meet Standard (default: .20): ').strip() or ".2")
    silver_pct = float(input('Enter percentile for Silver Meet Standard (default: .60): ').strip() or ".6")
    heat_time = int(input('To estimate meet length, enter the number of seconds between heats (default: 15): ').strip() or "15")
    event_time = int(input('To estimate meet length, enter the number of seconds between events (default: 30): ').strip() or "30")

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
    col_order = ["Event_name", "gold_y", "new_gold_y", "gold_diff_y", "gold_s", "new_gold_s", "gold_diff_s", "silver_y", "new_silver_y", "silver_diff_y", "silver_s", "new_silver_s", "silver_diff_s"]
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
        "silver_diff_s" : "Silver delta (meters)"}, inplace=True)

    proposed_with_differences.to_csv(f'proposed_new_standards_{gold_pct}_{silver_pct}.csv', index=False)

    print(f'\n\nBased on the newly calculated time standards with the top {gold_pct:.0%} for Gold and top {silver_pct:.0%} for silver, lets estimate how long each meet would take (using last season data):')
    # look at all of the gasl times, print out summary based on current season
    for csv in glob.glob(file_path_pattern):
        times = pd.read_csv(csv)
        _season = times['date'].iloc[0]
        _dt = datetime.strptime(_season, '%m/%d/%y')
        # add a column that can be an athlete identifier
        if _dt.year == 2024: #datetime.now().year:
            times['athlete_id'] = times.apply(lambda row: hashlib.sha256(f"{row['first_name']}_{row['last_name']}_{row['team_abbr']}_{row['age']}".encode()).hexdigest(), axis=1)
            qualifiers, season = get_qualifiers_summary(times, combined, add_current, heat_time, event_time)
            qualifiers.to_csv(f'{season}-qualifiers.csv', index=False)
    
    print(f'\nCalulated times file written to: ./proposed_new_standards_{gold_pct}_{silver_pct}.csv')

# Main execution
if __name__ == "__main__":
    main()