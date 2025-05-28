import glob
import pandas as pd
from datetime import datetime
from time_standards_service import TimeStandardsService


def main():
    """Main function using the new service-based approach."""
    # Initialize service
    service = TimeStandardsService()
    
    # Adjust the file path pattern as needed
    file_path_pattern = './gasl*.csv'
    current_standards_file = './current_standards.csv'
    
    # Get input for percentiles
    gold_pct = float(input('Enter percentile for Gold Meet Standard (default: .15): ').strip() or ".15")
    silver_pct = float(input('Enter percentile for Silver Meet Standard (default: .55): ').strip() or ".55")
    heat_time = int(input('To estimate meet length, enter the number of seconds between heats (default: 15): ').strip() or "15")
    event_time = int(input('To estimate meet length, enter the number of seconds between events (default: 30): ').strip() or "30")

    # Calculate new standards
    results = service.calculate_new_standards(
        file_path_pattern, 
        current_standards_file,
        gold_pct=gold_pct, 
        silver_pct=silver_pct
    )
    
    proposed_with_differences = results['proposed_standards']
    combined = results['combined_new']
    add_current = results['current_standards']
    df = results['raw_data']
    
    # Show current percentile analysis
    current_analysis = service.get_current_percentile_summary(df, add_current)
    current_analysis.to_csv('current_percentile_analysis.csv', index=False)
    
    # Column ordering and renaming
    col_order = [
        "GASL_Event_ID_x", "Event_name", "gold_y", "new_gold_y", "gold_diff_y", 
        "gold_s", "new_gold_s", "gold_diff_s", "silver_y", "new_silver_y", 
        "silver_diff_y", "silver_s", "new_silver_s", "silver_diff_s"
    ]
    
    proposed_with_differences = proposed_with_differences[col_order]
    proposed_with_differences = proposed_with_differences.rename(columns={
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
    
    proposed_with_differences = proposed_with_differences.sort_values(by=['Event_ID'])
    proposed_with_differences.to_csv(f'proposed_new_standards_{gold_pct}_{silver_pct}.csv', index=False)

    # Generate AppleScript for automated entry
    selected_columns = [
        'Proposed Gold Time (yards)', 'Proposed Gold Time (meters)', 
        'Proposed Silver Time (yards)', 'Proposed Silver Time (meters)'
    ]

    with open('enter_time_standards.applescript', 'w') as f:
        f.write('tell application "Safari"\n')
        f.write('activate\n')
        f.write('delay 0.2\n')
        f.write('tell application "System Events"\n')
        for _, row in proposed_with_differences.iterrows():
            for col in selected_columns:
                value = row[col]
                if pd.isna(value) or str(value).strip() == "":
                    f.write('key code 51\n')
                    f.write('delay 0.2\n')
                    f.write('keystroke {tab}\n')
                    f.write('delay 0.2\n')
                else:
                    f.write('key code 51\n')
                    f.write('delay 0.2\n')
                    f.write(f'keystroke "{value}"\n')
                    f.write('delay 0.2\n')
                    f.write('keystroke {tab}\n')
                    f.write('delay 0.2\n')
            f.write('key code 51\n')
            f.write('delay 0.2\n')
            f.write('keystroke {tab}\n')
            f.write('delay 0.2\n')
            f.write('key code 51\n')
            f.write('delay 0.2\n')
            f.write('keystroke {tab}\n')
            f.write('delay 0.2\n')
            f.write('key code 51\n')
            f.write('delay 0.2\n')
        f.write('end tell\n')
        f.write('end tell\n')

    print(f'\n\nBased on the newly calculated time standards with the top {gold_pct:.0%} for Gold and top {silver_pct:.0%} for silver, lets estimate how long each meet would take (using last season data):')
    
    # Analyze each season's data
    for csv in glob.glob(file_path_pattern):
        times = pd.read_csv(csv)
        _season = times['date'].iloc[0]
        _dt = datetime.strptime(_season, '%m/%d/%y')
        
        if _dt.year == 2024:  # Current season
            import hashlib
            times['athlete_id'] = times.apply(
                lambda row: hashlib.sha256(
                    f"{row['first_name']}_{row['last_name']}_{row['team_abbr']}_{row['age']}".encode()
                ).hexdigest(), 
                axis=1
            )
            
            qualifiers_result = service.get_qualifiers_summary(
                times, combined, add_current, heat_time, event_time
            )
            
            qualifiers_result['times_df'].to_csv(f'{qualifiers_result["season"]}-qualifiers.csv', index=False)
    
    print(f'\nCalculated times file written to: ./proposed_new_standards_{gold_pct}_{silver_pct}.csv')


if __name__ == "__main__":
    main()