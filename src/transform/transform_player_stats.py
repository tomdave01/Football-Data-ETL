import os
import json
import pandas as pd
import glob

def transform_player_stats(input_file: str, output_file: str) -> None:
    """
    Transforms raw player statistics into structured data.
    
    Args:
        input_file (str): Path to the input JSON file containing raw player statistics.
        output_file (str): Path to the output CSV file where transformed data will be saved.
    """

    player_files = glob.glob('data/raw/*/players_stats.json')

    if not player_files:
        print("No player statistics files found.")
        raise FileNotFoundError("No player statistics files found.")
    
    player_stats_df = pd.DataFrame()
    player_shooting_df = pd.DataFrame()
    player_passing_df = pd.DataFrame()
    player_passing_types_df = pd.DataFrame()
    player_goal_creation_actions_df = pd.DataFrame()
    player_defense_df = pd.DataFrame()
    player_possession_df = pd.DataFrame()
    player_playing_time_df = pd.DataFrame()
    player_miscellaneous_df = pd.DataFrame()

    for player_file in player_files:
        try:
            input_file = player_file
            directory_name = os.path.dirname(input_file)
            team_name = os.path.basename(directory_name)

            with open(input_file, 'r') as file:
                raw_data = json.load(file)
            
            for player_data in raw_data['players']:
                player_id = player_data['meta_data']['player_id']
                player_name = player_data['meta_data']['player_name']
                player_country_code = player_data['meta_data']['player_country_code']
                player_age = player_data['meta_data']['age']
        
                stats = pd.json_normalize(player_data["stats"]["stats"])
                stats['player_id'] = player_id
                stats['player_name'] = player_name
                stats['team_name'] = team_name
                stats['player_country_code'] = player_country_code
                stats['player_age'] = player_age
                stats['section'] = "stats"
                player_stats_df = pd.concat([player_stats_df, stats], ignore_index=True)

                stats = pd.json_normalize(player_data["stats"]["shooting"])
                stats['player_id'] = player_id
                stats['player_name'] = player_name
                stats['team_name'] = team_name
                stats['player_country_code'] = player_country_code
                stats['player_age'] = player_age
                stats['section'] = "shooting"
                player_shooting_df = pd.concat([player_shooting_df, stats], ignore_index=True)

                stats = pd.json_normalize(player_data["stats"]["passing"])
                stats['player_id'] = player_id
                stats['player_name'] = player_name
                stats['team_name'] = team_name
                stats['player_country_code'] = player_country_code
                stats['player_age'] = player_age
                stats['section'] = "passing"
                player_passing_df = pd.concat([player_passing_df, stats], ignore_index=True)

                stats = pd.json_normalize(player_data["stats"]["passing_types"])
                stats['player_id'] = player_id
                stats['player_name'] = player_name
                stats['team_name'] = team_name
                stats['player_country_code'] = player_country_code
                stats['player_age'] = player_age
                stats['section'] = "passing_types"
                player_passing_types_df = pd.concat([player_passing_types_df, stats], ignore_index=True)

                stats = pd.json_normalize(player_data["stats"]["gca"])
                stats['player_id'] = player_id
                stats['player_name'] = player_name
                stats['team_name'] = team_name
                stats['player_country_code'] = player_country_code
                stats['player_age'] = player_age
                stats['section'] = "goal_creation_actions"
                player_goal_creation_actions_df = pd.concat([player_goal_creation_actions_df, stats], ignore_index=True)

                stats = pd.json_normalize(player_data["stats"]["defense"])
                stats['player_id'] = player_id
                stats['player_name'] = player_name
                stats['team_name'] = team_name
                stats['player_country_code'] = player_country_code
                stats['player_age'] = player_age
                stats['section'] = "defense"
                player_defense_df = pd.concat([player_defense_df, stats], ignore_index=True)

                stats = pd.json_normalize(player_data["stats"]["possession"])
                stats['player_id'] = player_id
                stats['player_name'] = player_name
                stats['team_name'] = team_name
                stats['player_country_code'] = player_country_code
                stats['player_age'] = player_age
                stats['section'] = "possession"
                player_possession_df = pd.concat([player_possession_df, stats], ignore_index=True)

                stats = pd.json_normalize(player_data["stats"]["playingtime"])
                stats['player_id'] = player_id
                stats['player_name'] = player_name
                stats['team_name'] = team_name
                stats['player_country_code'] = player_country_code
                stats['player_age'] = player_age
                stats['section'] = "playing_time"
                player_playing_time_df = pd.concat([player_playing_time_df, stats], ignore_index=True)

                stats = pd.json_normalize(player_data["stats"]["misc"])
                stats['player_id'] = player_id
                stats['player_name'] = player_name
                stats['team_name'] = team_name
                stats['player_country_code'] = player_country_code
                stats['player_age'] = player_age
                stats['section'] = "miscellaneous"
                player_miscellaneous_df = pd.concat([player_miscellaneous_df, stats], ignore_index=True)

        except FileNotFoundError:
            print(f"Error: The file '{input_file}' was not found.")
            return
        except json.JSONDecodeError:
            print(f"Error: The file '{input_file}' is not a valid JSON.")
            return

    # Save the transformed data to a CSV file
    output_file = output_file or 'data/processed/player_stats.csv'
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    player_stats_df.to_csv(output_file, index=False)

if __name__ == "__main__":
    input_file = 'data/raw/players_stats.json'
    output_file = 'data/processed/player_stats.csv'
    transform_player_stats(input_file, output_file)
    print(f"Transformed player statistics saved to {output_file}")