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

    for player_file in player_files:
        try:
            input_file = player_file
            directory_name = os.path.dirname(input_file)
            team_name = os.path.basename(directory_name)

            with open(input_file, 'r') as file:
                raw_data = json.load(file)
        except FileNotFoundError:
            print(f"Error: The file '{input_file}' was not found.")
            return
        except json.JSONDecodeError:
            print(f"Error: The file '{input_file}' is not a valid JSON.")
            return

    player_stats_df = pd.DataFrame()
    
    for player_data in raw_data['data']:
        player_id = player_data['meta_data']['player_id']
        player_name = player_data['meta_data']['player_name']
        
        # Flatten the stats and add player_id and player_name
        stats = pd.json_normalize(player_data["stats"])
        stats['player_id'] = player_id
        stats['player_name'] = player_name
        
        # Concatenate the current player's stats with the main DataFrame
        player_stats_df = pd.concat([player_stats_df, stats], ignore_index=True)

    # Save the transformed data to a CSV file
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    player_stats_df.to_csv(output_file, index=False)