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
        return response.json()
    else:
        return {
            'status_code': response.status_code,
            'error': response.json()
        }

#print(get_team_stats())