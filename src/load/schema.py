import psycopg2
from psycopg2 import sql
import os
import sys
import pandas as pd
import glob
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from config import load_config

def create_schema():

    config = load_config()
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE SCHEMA IF NOT EXISTS football")
                print("Schema 'football' created successfully.")
                
                create_reference_tables(cur)
                
                team_stats_path = str(Path(__file__).resolve().parent.parent.parent / 'data' / 'processed' / 'team_stats')
                create_team_stats_tables(cur, team_stats_path)
                
                player_stats_path = str(Path(__file__).resolve().parent.parent.parent / 'data' / 'processed' / 'player_stats.csv')
                create_player_stats_tables(cur, player_stats_path)
                
                print("All tables created successfully.")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f'Error creating schema: {error}')
        raise

def create_reference_tables(cur):

    commands = [
        """
        CREATE TABLE IF NOT EXISTS football.leagues (
            league_id TEXT PRIMARY KEY,
            league_name TEXT NOT NULL,
            country TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS football.seasons (
            season_id TEXT PRIMARY KEY,
            start_year INTEGER,
            end_year INTEGER
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS football.teams (
            team_id TEXT NOT NULL,
            team_name TEXT NOT NULL,
            league_id TEXT NOT NULL,
            season_id TEXT NOT NULL,
            PRIMARY KEY (team_id, league_id, season_id),
            FOREIGN KEY (league_id) REFERENCES football.leagues (league_id),
            FOREIGN KEY (season_id) REFERENCES football.seasons (season_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS football.players (
            player_id TEXT NOT NULL,
            player_name TEXT NOT NULL,
            team_id TEXT NOT NULL,
            league_id TEXT NOT NULL,
            season_id TEXT NOT NULL,
            player_country_code TEXT,
            player_age INTEGER,
            PRIMARY KEY (player_id, league_id, season_id),
            FOREIGN KEY (team_id, league_id, season_id)
                REFERENCES football.teams (team_id, league_id, season_id)
        )
        """
    ]

    for command in commands:
        cur.execute(command)

    print("Reference tables created successfully.")

def create_team_stats_tables(cur, team_stats_dir):
    
    csv_files = glob.glob(os.path.join(team_stats_dir, "*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {team_stats_dir}")
        return
    
    for csv_path in csv_files:
        try:
            create_table_from_csv(cur, csv_path, table_type='team')
        except Exception as e:
            print(f"Error processing {csv_path}: {e}")

def create_player_stats_tables(cur, player_stats_path):

    try:
        create_table_from_csv(cur, player_stats_path, table_type='player')
    except Exception as e:
        print(f"Error processing player stats: {e}")

def create_table_from_csv(cur, csv_path, table_type='team'):
    
    df = pd.read_csv(csv_path)
    
    table_name = os.path.splitext(os.path.basename(csv_path))[0]
    
    if table_type == 'team':
        primary_key_cols = ['team_id']
        reference_cols = primary_key_cols + ['team_name']
    else:  
        primary_key_cols = ['player_id']
        reference_cols = primary_key_cols + ['player_name', 'team_name', 'player_country_code', 'player_age', 'section']
    
    stat_cols = [col for col in df.columns if col not in reference_cols]
    
    
    column_definitions = []
    for col in stat_cols:
        
        if pd.api.types.is_numeric_dtype(df[col]):
            if df[col].apply(lambda x: x == int(x) if pd.notnull(x) else True).all():
                column_definitions.append(f"{col} INTEGER")
            else:
                column_definitions.append(f"{col} FLOAT")
        else:
            column_definitions.append(f"{col} TEXT")    

    if table_type == 'team':
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS football.{table_name} (
            team_id TEXT NOT NULL,
            league_id TEXT NOT NULL,
            season_id TEXT NOT NULL,
            {', '.join(column_definitions)},
            PRIMARY KEY (team_id, league_id, season_id),
            FOREIGN KEY (team_id, league_id, season_id)
                REFERENCES football.teams (team_id, league_id, season_id)
        )
        """
    else:  
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS football.{table_name} (
            player_id TEXT NOT NULL,
            league_id TEXT NOT NULL,
            season_id TEXT NOT NULL,
            team_id TEXT NOT NULL,
            {', '.join(column_definitions)},
            PRIMARY KEY (player_id, league_id, season_id),
            FOREIGN KEY (player_id, league_id, season_id)
                REFERENCES football.players (player_id, league_id, season_id),
            FOREIGN KEY (team_id, league_id, season_id)
                REFERENCES football.teams (team_id, league_id, season_id)
        )
        """
    cur.execute(create_table_sql)
    print(f"Table football.{table_name} created successfully.")

if __name__ == "__main__":


    
    print("\nSchema creation process completed.")
    