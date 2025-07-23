import pandas as pd
import hashlib
import math
from datetime import datetime
from functools import reduce
from utils import read_csv_files, convert_hundredths_to_time, convert_time_to_hundredths, add_event_names_column, read_csv_with_metadata


class TimeStandardsService:
    """Service class for calculating swimming time standards and meet analysis."""
    
    def __init__(self):
        # Relay time adjustments in hundredths of seconds (minutes * 60 * 100)
        self.RELAY_TIME_GOLD = 25 * 60 * 100    # 25 minutes
        self.RELAY_TIME_SILVER = 20 * 60 * 100  # 20 minutes
        self.RELAY_TIME_BRONZE = 15 * 60 * 100  # 15 minutes
        
        self.event_map = {
            "Boys 10 & Under_100_Individual Medley": 4,
            "Boys 11-12_50_Backstroke": 30,
            "Boys 11-12_50_Breaststroke": 40,
            "Boys 11-12_50_Butterfly": 50,
            "Boys 11-12_50_Freestyle": 18,
            "Boys 11-12_100_Individual Medley": 6,
            "Boys 13-14_50_Backstroke": 32,
            "Boys 13-14_50_Breaststroke": 42,
            "Boys 13-14_50_Butterfly": 52,
            "Boys 13-14_50_Freestyle": 20,
            "Boys 13-14_100_Individual Medley": 8,
            "Boys 6 & Under_25_Backstroke": 24,
            "Boys 6 & Under_25_Freestyle": 12,
            "Boys 7-8_25_Backstroke": 26,
            "Boys 7-8_25_Freestyle": 14,
            "Boys 8 & Under_25_Breaststroke": 36,
            "Boys 8 & Under_25_Butterfly": 46,
            "Boys 9-10_25_Backstroke": 28,
            "Boys 9-10_25_Breaststroke": 38,
            "Boys 9-10_25_Butterfly": 48,
            "Boys 9-10_50_Freestyle": 16,
            "Girls 10 & Under_100_Individual Medley": 5,
            "Girls 11-12_50_Backstroke": 31,
            "Girls 11-12_50_Breaststroke": 41,
            "Girls 11-12_50_Butterfly": 51,
            "Girls 11-12_50_Freestyle": 19,
            "Girls 11-12_100_Individual Medley": 7,
            "Girls 13-14_50_Backstroke": 33,
            "Girls 13-14_50_Breaststroke": 43,
            "Girls 13-14_50_Butterfly": 53,
            "Girls 13-14_50_Freestyle": 21,
            "Girls 13-14_100_Individual Medley": 9,
            "Girls 6 & Under_25_Backstroke": 25,
            "Girls 6 & Under_25_Freestyle": 13,
            "Girls 7-8_25_Backstroke": 27,
            "Girls 7-8_25_Freestyle": 15,
            "Girls 8 & Under_25_Breaststroke": 37,
            "Girls 8 & Under_25_Butterfly": 47,
            "Girls 9-10_25_Backstroke": 29,
            "Girls 9-10_25_Breaststroke": 39,
            "Girls 9-10_25_Butterfly": 49,
            "Girls 9-10_50_Freestyle": 17,
            "Men 15-18_50_Backstroke": 34,
            "Men 15-18_50_Breaststroke": 44,
            "Men 15-18_50_Butterfly": 54,
            "Men 15-18_50_Freestyle": 22,
            "Men 15-18_100_Individual Medley": 10,
            "Women 15-18_50_Backstroke": 35,
            "Women 15-18_50_Breaststroke": 45,
            "Women 15-18_50_Butterfly": 55,
            "Women 15-18_50_Freestyle": 23,
            "Women 15-18_100_Individual Medley": 11
        }

    def gasl_event_id(self, event):
        """Get GASL event ID for a given event name."""
        return self.event_map.get(event, 0)

    def prepare_data(self, file_path_pattern):
        """Read and prepare data with athlete IDs."""
        df = read_csv_files(file_path_pattern)
        df['athlete_id'] = df.apply(
            lambda row: hashlib.sha256(
                f"{row['first_name']}_{row['last_name']}_{row['team_abbr']}_{row['age']}".encode()
            ).hexdigest(), 
            axis=1
        )
        return df

    def get_percentile_summary(self, df, standard, pct):
        """Calculate percentile-based time standards for each event."""
        grouped = df.groupby(['age_group', 'distance', 'stroke'])
        
        event_names = []
        event_ids = []
        thresholds = []
        thresholds_meters = []
        counts = []
        
        for name, group in grouped:
            threshold_value = group['converted_hundredths'].quantile(pct)
            threshold = convert_hundredths_to_time(threshold_value)
            count = (group['converted_hundredths'] >= threshold_value).sum()
            
            event_name = f"{name[0]}_{name[1]}_{name[2]}"
            event_id = self.gasl_event_id(event_name)
            
            event_names.append(event_name)
            event_ids.append(event_id)
            thresholds.append(threshold)
            thresholds_meters.append(convert_hundredths_to_time(threshold_value * 1.11))
            counts.append(count)
        
        summary_df = pd.DataFrame({
            'Event_name': event_names,
            'GASL_Event_ID': event_ids,
            f'new_{standard}_y': thresholds,
            f'new_{standard}_s': thresholds_meters
        })
        return summary_df

    def get_current_percentile_summary(self, df, current_times):
        """Analyze current percentiles for existing time standards."""
        grouped = df.groupby(['age_group', 'distance', 'stroke'])
        
        event_names = []
        current_gold_times = []
        current_silver_times = []
        current_gold_percentile = []
        current_silver_percentile = []
        
        for name, group in grouped:
            event_name = f"{name[0]}_{name[1]}_{name[2]}"
            
            gold_time = current_times.loc[current_times['Event_name'] == event_name, 'gold_y'].values[0]
            gold_time_hundredths = convert_time_to_hundredths(gold_time)
            gold_percentile = (group['converted_hundredths'] < gold_time_hundredths).mean() * 100
            
            silver_time = current_times.loc[current_times['Event_name'] == event_name, 'silver_y'].values[0]
            silver_time_hundredths = convert_time_to_hundredths(silver_time)
            silver_percentile = (group['converted_hundredths'] < silver_time_hundredths).mean() * 100
            
            event_names.append(event_name)
            current_gold_times.append(gold_time_hundredths)
            current_gold_percentile.append(gold_percentile)
            current_silver_times.append(silver_time_hundredths)
            current_silver_percentile.append(silver_percentile)
        
        summary_df = pd.DataFrame({
            'Event_name': event_names,
            'current_gold_time': current_gold_times,
            'current_gold_percentile': current_gold_percentile,
            'current_silver_time': current_silver_times,
            'current_silver_percentile': current_silver_percentile
        })
        
        return summary_df

    def dedup_entries(self, df):
        """Remove duplicate entries based on athlete priority and performance."""
        df['qualified_meet'] = pd.Categorical(
            df['qualified_meet'], 
            categories=["GOLD", "SILVER", "BRONZE"], 
            ordered=True
        )
        
        df_sorted = df.sort_values(by=['athlete_id', 'qualified_meet', 'converted_hundredths'])
        
        df_highest_priority = (
            df_sorted[df_sorted.groupby('athlete_id')['qualified_meet'].transform('min') == df_sorted['qualified_meet']]
        )
        
        df_final = pd.concat(
            [group.sample(n=min(3, len(group)), random_state=1) 
             for _, group in df_highest_priority.groupby(['athlete_id', 'qualified_meet'], observed=False)]
        ).reset_index(drop=True)
        
        return df_final

    def get_team_attendance_summary(self, df):
        """Generate team attendance summary by meet level."""
        # Debug: check what's in the dataframe
        if df.empty:
            # Return empty summary with proper structure
            return pd.DataFrame(columns=['GOLD', 'SILVER', 'BRONZE', 'Total'])
        
        # Ensure all qualification levels are present
        all_levels = ['GOLD', 'SILVER', 'BRONZE']
        
        # Group by team and qualification level, count unique athletes
        summary = (
            df.groupby(['team_abbr', 'qualified_meet'], observed=False)['athlete_id']
            .nunique()
            .unstack(fill_value=0)
        )
        
        # The unstack should make qualified_meet values (GOLD, SILVER, BRONZE) the columns
        # and team_abbr values the index (rows)
        
        # Ensure all expected columns exist
        for level in all_levels:
            if level not in summary.columns:
                summary[level] = 0
        
        # Reorder columns to ensure consistent order
        summary = summary[all_levels]
        
        # Add total column
        summary['Total'] = summary.sum(axis=1)
        
        # Add total row
        summary.loc['Total'] = summary.sum()
        
        return summary

    def get_estimated_meet_duration(self, df, season, proposed_times, heat_time, event_delay):
        """Estimate meet duration based on qualified swimmers and heat configurations."""
        grouped = df.groupby(['age_group', 'distance', 'stroke'])
        
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
            event_name = f"{name[0]}_{name[1]}_{name[2]}"
            
            _row = proposed_times.index[proposed_times['Event_name'] == event_name].tolist()
            _gold = proposed_times.loc[_row, "new_gold_y"]
            _silver = proposed_times.loc[_row, "new_silver_y"]
            
            _gold_hundredths = convert_time_to_hundredths(_gold.item())
            _silver_hundredths = convert_time_to_hundredths(_silver.item())
            
            gold_count = (group['qualified_meet'] == "GOLD").sum()
            silver_count = (group['qualified_meet'] == "SILVER").sum()
            bronze_count = (group['qualified_meet'] == "BRONZE").sum()
            
            _gold_heats = math.ceil(gold_count / 6)
            _silver_heats = math.ceil(silver_count / 6)
            _bronze_heats = math.ceil(bronze_count / 6)
            
            event_names.append(event_name)
            gold_qualifiers.append(gold_count)
            silver_qualifiers.append(silver_count)
            bronze_qualifiers.append(bronze_count)
            gold_heats.append(_gold_heats)
            silver_heats.append(_silver_heats)
            bronze_heats.append(_bronze_heats)
            
            gold_event_times.append(_gold_heats * (_gold_hundredths + heat_time_hundredths) + event_delay_hundredths)
            silver_event_times.append(_silver_heats * (_silver_hundredths + heat_time_hundredths) + event_delay_hundredths)
            bronze_event_times.append(_bronze_heats * (_silver_hundredths + heat_time_hundredths) + event_delay_hundredths)
        
        times_df = pd.DataFrame({
            'Event_name': event_names,
            f'gold_qualifiers-{season}': gold_qualifiers,
            f'gold_heats-{season}': gold_heats,
            f'gold_est_duration-{season}': gold_event_times,
            f'silver_qualifiers-{season}': silver_qualifiers,
            f'silver_heats-{season}': silver_heats,
            f'silver_est_duration-{season}': silver_event_times,
            f'bronze_qualifiers-{season}': bronze_qualifiers,
            f'bronze_heats-{season}': bronze_heats,
            f'bronze_est_duration-{season}': bronze_event_times
        })
        
        return times_df

    def get_meet_duration_with_current_standards(self, df, season, current_times, heat_time, event_delay, team_assignments=None):
        """Estimate meet duration using current time standards for qualification and timing."""
        # Debug: check what columns we have
        print(f"DEBUG: Available columns in df: {list(df.columns)}")
        print(f"DEBUG: Sample of first few rows:")
        print(df.head())
        
        grouped = df.groupby(['age_group', 'distance', 'stroke'])
        
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
        
        # First, qualify swimmers using current standards
        entries = pd.DataFrame()
        
        for name, group in grouped:
            event_name = f"{name[0]}_{name[1]}_{name[2]}"
            
            # Use current standards for qualification
            _row = current_times.index[current_times['Event_name'] == event_name].tolist()
            if not _row:
                continue
                
            _gold = current_times.loc[_row, "gold_y"]
            _silver = current_times.loc[_row, "silver_y"]
            
            _gold_hundredths = convert_time_to_hundredths(_gold.item())
            _silver_hundredths = convert_time_to_hundredths(_silver.item())
            
            # Qualify swimmers based on current standards
            _gold_peeps = group[group['converted_hundredths'] <= _gold_hundredths]
            _silver_peeps = group[(group['converted_hundredths'] <= _silver_hundredths) & 
                                 (group['converted_hundredths'] >= _gold_hundredths)]
            _bronze_peeps = group[group['converted_hundredths'] > _silver_hundredths]
            
            _gold_entries = _gold_peeps.assign(qualified_meet='GOLD')
            _silver_entries = _silver_peeps.assign(qualified_meet='SILVER')
            
            if "15-18" in event_name:
                _bronze_entries = _bronze_peeps.assign(qualified_meet='SILVER')
            else:
                _bronze_entries = _bronze_peeps.assign(qualified_meet='BRONZE')
            
            entries = pd.concat([entries, _gold_entries, _silver_entries, _bronze_entries], ignore_index=True)
        
        # Clean up entries
        cleaned_up_entries = self.dedup_entries(entries)
        
        # Now calculate duration estimates using current standards
        grouped_qualified = cleaned_up_entries.groupby(['age_group', 'distance', 'stroke'])
        
        for name, group in grouped_qualified:
            event_name = f"{name[0]}_{name[1]}_{name[2]}"
            
            # Use current standards for timing calculations
            _row = current_times.index[current_times['Event_name'] == event_name].tolist()
            if not _row:
                continue
                
            _gold = current_times.loc[_row, "gold_y"]
            _silver = current_times.loc[_row, "silver_y"]
            
            _gold_hundredths = convert_time_to_hundredths(_gold.item())
            _silver_hundredths = convert_time_to_hundredths(_silver.item())
            
            gold_count = (group['qualified_meet'] == "GOLD").sum()
            silver_count = (group['qualified_meet'] == "SILVER").sum()
            bronze_count = (group['qualified_meet'] == "BRONZE").sum()
            
            _gold_heats = math.ceil(gold_count / 6)
            _silver_heats = math.ceil(silver_count / 6)
            _bronze_heats = math.ceil(bronze_count / 6)
            
            event_names.append(event_name)
            gold_qualifiers.append(gold_count)
            silver_qualifiers.append(silver_count)
            bronze_qualifiers.append(bronze_count)
            gold_heats.append(_gold_heats)
            silver_heats.append(_silver_heats)
            bronze_heats.append(_bronze_heats)
            
            gold_event_times.append(_gold_heats * (_gold_hundredths + heat_time_hundredths) + event_delay_hundredths)
            silver_event_times.append(_silver_heats * (_silver_hundredths + heat_time_hundredths) + event_delay_hundredths)
            bronze_event_times.append(_bronze_heats * (_silver_hundredths + heat_time_hundredths) + event_delay_hundredths)
        
        # Create detailed event breakdown
        times_df = pd.DataFrame({
            'Event_name': event_names,
            f'gold_qualifiers-{season}': gold_qualifiers,
            f'gold_heats-{season}': gold_heats,
            f'gold_est_duration-{season}': gold_event_times,
            f'silver_qualifiers-{season}': silver_qualifiers,
            f'silver_heats-{season}': silver_heats,
            f'silver_est_duration-{season}': silver_event_times,
            f'bronze_qualifiers-{season}': bronze_qualifiers,
            f'bronze_heats-{season}': bronze_heats,
            f'bronze_est_duration-{season}': bronze_event_times
        })
        
        # Calculate total meet durations
        total_gold_duration = sum(gold_event_times) if gold_event_times else 0
        total_silver_duration = sum(silver_event_times) if silver_event_times else 0
        total_bronze_duration = sum(bronze_event_times) if bronze_event_times else 0
        
        # Calculate unique swimmer counts (not total qualifiers)
        gold_swimmers = len(cleaned_up_entries[cleaned_up_entries['qualified_meet'] == 'GOLD']['athlete_id'].unique())
        silver_swimmers = len(cleaned_up_entries[cleaned_up_entries['qualified_meet'] == 'SILVER']['athlete_id'].unique())
        bronze_swimmers = len(cleaned_up_entries[cleaned_up_entries['qualified_meet'] == 'BRONZE']['athlete_id'].unique())
        
        # Create meet summary based on team assignments
        if team_assignments:
            # Calculate duration for specific team assignments
            meet_summary = self._calculate_meet_summary_with_assignments(
                cleaned_up_entries, current_times, heat_time, event_delay, team_assignments
            )
        else:
            # Default: split Silver and Bronze meets in half
            # Convert to regular Python ints for JSON serialization
            silver_meet_duration = int(total_silver_duration // 2)
            bronze_meet_duration = int(total_bronze_duration // 2)
            silver_meet_swimmers = int(silver_swimmers // 2)
            bronze_meet_swimmers = int(bronze_swimmers // 2)
            
            # Add relay times to durations
            gold_duration_with_relay = int(total_gold_duration) + self.RELAY_TIME_GOLD
            silver_duration_with_relay = int(silver_meet_duration) + self.RELAY_TIME_SILVER
            bronze_duration_with_relay = int(bronze_meet_duration) + self.RELAY_TIME_BRONZE
            
            meet_summary = {
                'gold_meet': {
                    'total_swimmers': int(gold_swimmers),
                    'total_duration_hundredths': gold_duration_with_relay,
                    'total_duration_formatted': convert_hundredths_to_time(gold_duration_with_relay)
                },
                'silver_meet_1': {
                    'total_swimmers': silver_meet_swimmers,
                    'total_duration_hundredths': silver_duration_with_relay,
                    'total_duration_formatted': convert_hundredths_to_time(silver_duration_with_relay)
                },
                'silver_meet_2': {
                    'total_swimmers': silver_meet_swimmers,
                    'total_duration_hundredths': silver_duration_with_relay,
                    'total_duration_formatted': convert_hundredths_to_time(silver_duration_with_relay)
                },
                'bronze_meet_1': {
                    'total_swimmers': bronze_meet_swimmers,
                    'total_duration_hundredths': bronze_duration_with_relay,
                    'total_duration_formatted': convert_hundredths_to_time(bronze_duration_with_relay)
                },
                'bronze_meet_2': {
                    'total_swimmers': bronze_meet_swimmers,
                    'total_duration_hundredths': bronze_duration_with_relay,
                    'total_duration_formatted': convert_hundredths_to_time(bronze_duration_with_relay)
                }
            }
        
        return {
            'times_df': times_df,
            'meet_summary': meet_summary,
            'season': season,
            'entries': cleaned_up_entries
        }

    def _calculate_meet_summary_with_assignments(self, entries, current_times, heat_time, event_delay, team_assignments):
        """Calculate meet durations based on specific team assignments."""
        heat_time_hundredths = heat_time * 100
        event_delay_hundredths = event_delay * 100
        
        # Extract host team information if provided
        host_teams = team_assignments.get('host_teams', {})
        
        meet_summary = {}
        
        # Calculate Gold meet (all Gold qualifiers)
        gold_entries = entries[entries['qualified_meet'] == 'GOLD']
        gold_duration, _ = self._calculate_meet_duration_for_entries(
            gold_entries, current_times, heat_time_hundredths, event_delay_hundredths
        )
        # Count unique swimmers in gold meet
        gold_swimmers = len(gold_entries['athlete_id'].unique()) if len(gold_entries) > 0 else 0
        
        # Add relay time to gold meet duration
        gold_duration_with_relay = int(gold_duration) + self.RELAY_TIME_GOLD
        
        meet_summary['gold_meet'] = {
            'total_swimmers': int(gold_swimmers),
            'total_duration_hundredths': gold_duration_with_relay,
            'total_duration_formatted': convert_hundredths_to_time(gold_duration_with_relay)
        }
        
        # Calculate Silver and Bronze meets based on team assignments
        meets = {
            'silver_meet_1': ('SILVER', team_assignments.get('silver1', []), host_teams.get('silver1_host', 'TBD')),
            'silver_meet_2': ('SILVER', team_assignments.get('silver2', []), host_teams.get('silver2_host', 'TBD')),
            'bronze_meet_1': ('BRONZE', team_assignments.get('bronze1', []), host_teams.get('bronze1_host', 'TBD')),
            'bronze_meet_2': ('BRONZE', team_assignments.get('bronze2', []), host_teams.get('bronze2_host', 'TBD'))
        }
        
        for meet_name, (qualification_level, assigned_teams, host_team) in meets.items():
            if assigned_teams:
                # Filter entries by qualification level and assigned teams
                meet_entries = entries[
                    (entries['qualified_meet'] == qualification_level) &
                    (entries['team_abbr'].isin(assigned_teams))
                ]
            else:
                # If no teams assigned, use empty entries
                meet_entries = entries[entries['qualified_meet'] == 'DUMMY']  # Empty result
            
            duration, _ = self._calculate_meet_duration_for_entries(
                meet_entries, current_times, heat_time_hundredths, event_delay_hundredths
            )
            # Count unique swimmers in this meet
            swimmers = len(meet_entries['athlete_id'].unique()) if len(meet_entries) > 0 else 0
            
            # Add appropriate relay time based on meet type
            if qualification_level == 'SILVER':
                duration_with_relay = int(duration) + self.RELAY_TIME_SILVER
            else:  # BRONZE
                duration_with_relay = int(duration) + self.RELAY_TIME_BRONZE
            
            meet_summary[meet_name] = {
                'total_swimmers': int(swimmers),
                'total_duration_hundredths': duration_with_relay,
                'total_duration_formatted': convert_hundredths_to_time(duration_with_relay),
                'assigned_teams': assigned_teams,
                'host_team': host_team
            }
        
        return meet_summary

    def _calculate_meet_duration_for_entries(self, entries, current_times, heat_time_hundredths, event_delay_hundredths):
        """Calculate total duration for a set of entries."""
        if len(entries) == 0:
            return 0, 0
        
        grouped = entries.groupby(['age_group', 'distance', 'stroke'])
        total_duration = 0
        total_qualifiers = len(entries)
        
        for name, group in grouped:
            event_name = f"{name[0]}_{name[1]}_{name[2]}"
            
            # Get current standards for timing
            _row = current_times.index[current_times['Event_name'] == event_name].tolist()
            if not _row:
                continue
                
            # Use the qualification level's time standard
            qual_level = group['qualified_meet'].iloc[0]
            if qual_level == 'GOLD':
                time_standard = current_times.loc[_row, "gold_y"]
            else:  # SILVER or BRONZE
                time_standard = current_times.loc[_row, "silver_y"]
            
            time_hundredths = convert_time_to_hundredths(time_standard.item())
            
            # Calculate heats (6 swimmers per heat)
            swimmer_count = len(group)
            heats = math.ceil(swimmer_count / 6)
            
            # Calculate event duration
            event_duration = heats * (time_hundredths + heat_time_hundredths) + event_delay_hundredths
            total_duration += event_duration
        
        return total_duration, total_qualifiers

    def get_qualifiers_summary(self, df, proposed_times, current_times, heat_time, event_time):
        """Generate qualifier summary and meet duration estimates."""
        grouped = df.groupby(['age_group', 'distance', 'stroke'])
        season = df['date'].iloc[0]
        _dt = datetime.strptime(season, '%m/%d/%y')
        
        entries = pd.DataFrame()
        entries_old = pd.DataFrame()
        
        for name, group in grouped:
            event_name = f"{name[0]}_{name[1]}_{name[2]}"
            
            # New standards
            _row = proposed_times.index[proposed_times['Event_name'] == event_name].tolist()
            _gold = proposed_times.loc[_row, "new_gold_y"]
            _silver = proposed_times.loc[_row, "new_silver_y"]
            
            _gold_hundredths = convert_time_to_hundredths(_gold.item())
            _silver_hundredths = convert_time_to_hundredths(_silver.item())
            
            _gold_peeps = group[group['converted_hundredths'] <= _gold_hundredths]
            _silver_peeps = group[(group['converted_hundredths'] <= _silver_hundredths) & 
                                 (group['converted_hundredths'] >= _gold_hundredths)]
            _bronze_peeps = group[group['converted_hundredths'] > _silver_hundredths]
            
            _gold_entries = _gold_peeps.assign(qualified_meet='GOLD')
            _silver_entries = _silver_peeps.assign(qualified_meet='SILVER')
            
            if "15-18" in event_name:
                _bronze_entries = _bronze_peeps.assign(qualified_meet='SILVER')
            else:
                _bronze_entries = _bronze_peeps.assign(qualified_meet='BRONZE')
            
            entries = pd.concat([entries, _gold_entries, _silver_entries, _bronze_entries], ignore_index=True)
            
            # Current standards
            _row = current_times.index[current_times['Event_name'] == event_name].tolist()
            _gold = current_times.loc[_row, "gold_y"]
            _silver = current_times.loc[_row, "silver_y"]
            
            _gold_hundredths = convert_time_to_hundredths(_gold.item())
            _silver_hundredths = convert_time_to_hundredths(_silver.item())
            
            _gold_peeps = group[group['converted_hundredths'] <= _gold_hundredths]
            _silver_peeps = group[(group['converted_hundredths'] <= _silver_hundredths) & 
                                 (group['converted_hundredths'] >= _gold_hundredths)]
            _bronze_peeps = group[group['converted_hundredths'] > _silver_hundredths]
            
            _gold_entries = _gold_peeps.assign(qualified_meet='GOLD')
            _silver_entries = _silver_peeps.assign(qualified_meet='SILVER')
            
            if "15-18" in event_name:
                _bronze_entries = _bronze_peeps.assign(qualified_meet='SILVER')
            else:
                _bronze_entries = _bronze_peeps.assign(qualified_meet='BRONZE')
            
            entries_old = pd.concat([entries_old, _gold_entries, _silver_entries, _bronze_entries], ignore_index=True)
        
        cleaned_up_entries = self.dedup_entries(entries)
        cleaned_up_entries_sorted = cleaned_up_entries.sort_values(by=['athlete_id'])
        cleaned_up_entries_sorted.drop_duplicates(subset=['athlete_id'], keep='first', inplace=True)
        
        cleaned_up_entries_old = self.dedup_entries(entries_old)
        cleaned_up_entries_old_sorted = cleaned_up_entries_old.sort_values(by=['athlete_id'])
        cleaned_up_entries_old_sorted.drop_duplicates(subset=['athlete_id'], keep='first', inplace=True)
        
        times_df = self.get_estimated_meet_duration(cleaned_up_entries, _dt.year, proposed_times, heat_time, event_time)
        
        return {
            'times_df': times_df,
            'season': _dt.year,
            'new_entries': cleaned_up_entries_sorted,
            'current_entries': cleaned_up_entries_old_sorted
        }

    def calculate_time_differences(self, df):
        """Calculate differences between new and current time standards."""
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
        
        df = df.drop([
            'gold_new_hund_y', 'gold_new_hund_s', 'gold_hund_y', 'gold_hund_s',
            'silver_new_hund_y', 'silver_new_hund_s', 'silver_hund_y', 'silver_hund_s'
        ], axis=1)
        
        return df

    def clear_teen_silver_standards(self, df):
        """Clear silver standards for 15-18 age groups."""
        columns_to_clean = ["new_silver_y", "silver_diff_y", "new_silver_s", "silver_diff_s"]
        
        def clear_teen_event(row, col):
            if "15-18" in row["Event_name"]:
                return ""
            else:
                return row[col]
        
        for _col in columns_to_clean:
            df[_col] = df.apply(clear_teen_event, col=_col, axis=1)
        
        return df

    def calculate_new_standards(self, data_file_pattern, current_standards_file, 
                              gold_pct=0.15, silver_pct=0.55):
        """Main calculation method for generating new time standards."""
        # Prepare data
        df = self.prepare_data(data_file_pattern)
        
        # Get current standards
        current_standards = read_csv_with_metadata(current_standards_file)
        current_standards = add_event_names_column(current_standards)
        current_standards = current_standards.drop(['age_group', 'distance', 'stroke'], axis=1)
        
        # Calculate new standards
        standards = {"gold": gold_pct, "silver": silver_pct}
        summary = []
        
        for standard, pct in standards.items():
            summary_df = self.get_percentile_summary(df, standard, pct)
            summary.append(summary_df)
        
        # Combine results
        combined = reduce(lambda x, y: pd.merge(x, y, on='Event_name'), summary)
        add_current = combined.merge(current_standards, on="Event_name")
        proposed_with_differences = self.calculate_time_differences(add_current)
        
        # Clean up for teen events
        proposed_with_differences = self.clear_teen_silver_standards(proposed_with_differences)
        
        return {
            'proposed_standards': proposed_with_differences,
            'combined_new': combined,
            'current_standards': add_current,
            'raw_data': df
        }