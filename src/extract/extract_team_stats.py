import requests
import os
import json
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

API_URL = os.getenv('FBREF_BASE_URL')
API_KEY = os.getenv('FBREF_API_KEY')

def get_team_stats():
    """
    Fetches team statistics for the specified league and season.
    """
    params = {
            'league_id': '9' ,  
            'season_id': '2024-2025'
        }
    HEADERS = {'X-API-KEY': API_KEY}
    response = requests.get(f'{API_URL}/team-season-stats', params=params, headers=HEADERS)
    if response.status_code == 200:
        print(response.json())
    else:
        print(f"Error: {response.status_code} - {response.text}")

get_team_stats()