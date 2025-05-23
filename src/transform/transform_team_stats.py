import json
import os
import pandas as pd

def transform_team_stats():
    """
    Transforms raw team statistics into structured data.
    """
    try:
        with open('data/raw/team_stats.json', 'r') as file:
            raw_data = json.load(file)
    except FileNotFoundError:
        print("Error: The file 'data/raw/team_stats.json' was not found.")
        return None
    except json.JSONDecodeError:
        print("Error: The file 'data/raw/team_stats.json' is not a valid JSON.")
        return None
    
    general_stats_df = pd.DataFrame()
    keeper_stats_df = pd.DataFrame()
    keeper_stats_advanced_df = pd.DataFrame()
    shooting_stats_df =pd.DataFrame()
    passing_stats_df = pd.DataFrame()
    passing_types_stats_df = pd.DataFrame()
    goal_creation_stats_df = pd.DataFrame()
    defense_stats_df = pd.DataFrame()
    possession_stats_df = pd.DataFrame()
    playing_time_stats_df = pd.DataFrame()
    miscellaneous_stats_df = pd.DataFrame()

    for team_data in raw_data['data']:
        team_id = team_data['meta_data']['team_id']
        team_name = team_data['meta_data']['team_name']
        
        general_stats = pd.json_normalize(team_data["stats"]["stats"])
        general_stats['team_id'] = team_id
        general_stats['team_name'] = team_name
        general_stats_df = pd.concat([general_stats_df, general_stats], ignore_index=True)

        
        keeper_stats = pd.json_normalize(team_data["stats"]["keepers"])
        keeper_stats['team_id'] = team_id
        keeper_stats['team_name'] = team_name
        keeper_stats_df = pd.concat([keeper_stats_df, keeper_stats], ignore_index=True)
        
        keeper_stats_advanced = pd.json_normalize(team_data["stats"]["keepersadv"])
        keeper_stats_advanced['team_id'] = team_id
        keeper_stats_advanced['team_name'] = team_name
        keeper_stats_advanced_df = pd.concat([keeper_stats_advanced_df, keeper_stats_advanced], ignore_index=True)

        shooting_stats = pd.json_normalize(team_data["stats"]["shooting"])
        shooting_stats['team_id'] = team_id
        shooting_stats['team_name'] = team_name
        shooting_stats_df = pd.concat([shooting_stats_df, shooting_stats], ignore_index=True)

        passing_stats = pd.json_normalize(team_data["stats"]["passing"])
        passing_stats['team_id'] = team_id
        passing_stats['team_name'] = team_name
        passing_stats_df = pd.concat([passing_stats_df, passing_stats], ignore_index=True)

        passing_types_stats = pd.json_normalize(team_data["stats"]["passing_types"])
        passing_types_stats['team_id'] = team_id
        passing_types_stats['team_name'] = team_name
        passing_types_stats_df = pd.concat([passing_types_stats_df, passing_types_stats], ignore_index=True)

        goal_creation_stats = pd.json_normalize(team_data["stats"]["gca"])
        goal_creation_stats['team_id'] = team_id
        goal_creation_stats['team_name'] = team_name
        goal_creation_stats_df = pd.concat([goal_creation_stats_df, goal_creation_stats], ignore_index=True)

        defense_stats = pd.json_normalize(team_data["stats"]["defense"])
        defense_stats['team_id'] = team_id
        defense_stats['team_name'] = team_name
        defense_stats_df = pd.concat([defense_stats_df, defense_stats], ignore_index=True)

        possession_stats = pd.json_normalize(team_data["stats"]["possession"])
        possession_stats['team_id'] = team_id
        possession_stats['team_name'] = team_name
        possession_stats_df = pd.concat([possession_stats_df, possession_stats], ignore_index=True)

        playing_time_stats = pd.json_normalize(team_data["stats"]["playingtime"])
        playing_time_stats['team_id'] = team_id
        playing_time_stats['team_name'] = team_name
        playing_time_stats_df = pd.concat([playing_time_stats_df, playing_time_stats], ignore_index=True)

        miscellaneous_stats = pd.json_normalize(team_data["stats"]["misc"])
        miscellaneous_stats['team_id'] = team_id
        miscellaneous_stats['team_name'] = team_name
        miscellaneous_stats_df = pd.concat([miscellaneous_stats_df, miscellaneous_stats], ignore_index=True)

    os.makedirs('data/processed', exist_ok=True)
    general_stats_df.to_csv('data/processed/general_stats.csv', index=False)
    keeper_stats_df.to_csv('data/processed/keeper_stats.csv', index=False)
    keeper_stats_advanced_df.to_csv('data/processed/keeper_stats_advanced.csv', index=False)
    shooting_stats_df.to_csv('data/processed/shooting_stats.csv', index=False)
    passing_stats_df.to_csv('data/processed/passing_stats.csv', index=False)
    passing_types_stats_df.to_csv('data/processed/passing_types_stats.csv', index=False)
    goal_creation_stats_df.to_csv('data/processed/goal_creation_stats.csv', index=False)
    defense_stats_df.to_csv('data/processed/defense_stats.csv', index=False)
    possession_stats_df.to_csv('data/processed/possession_stats.csv', index=False)
    playing_time_stats_df.to_csv('data/processed/playing_time_stats.csv', index=False)
    miscellaneous_stats_df.to_csv('data/processed/miscellaneous_stats.csv', index=False)
    print("Data transformation complete. Processed files saved in 'data/processed' directory.")

    return {
        'general_stats': general_stats_df,
        'keeper_stats': keeper_stats_df,
        'keeper_stats_advanced': keeper_stats_advanced_df,
        'shooting_stats': shooting_stats_df,
        'passing_stats': passing_stats_df,
        'passing_types_stats': passing_types_stats_df,
        'goal_creation_stats': goal_creation_stats_df,
        'defense_stats': defense_stats_df,
        'possession_stats': possession_stats_df,
        'playing_time_stats': playing_time_stats_df,
        'miscellaneous_stats': miscellaneous_stats_df
    }

        