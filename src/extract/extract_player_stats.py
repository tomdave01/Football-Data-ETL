import requests
import os
import json
from dotenv import load_dotenv
from extract_team_stats import get_team_details

load_dotenv()

API_URL = os.getenv('FBREF_BASE_URL')
API_KEY = os.getenv('FBREF_API_KEY')

def get_players_stats():

    team_details = get_team_details()

    for team_data in team_details:
        team_id = team_data['team_id']
        team_name = team_data['team_name']
        
        params = {
            'team_id': team_id ,
            'league_id': '9'
        }
        HEADERS = {'X-API-KEY': API_KEY}
        response = requests.get(f'{API_URL}/player-season-stats', params=params, headers=HEADERS)
        if response.status_code == 200:
            filename = f'data/raw/{team_name}/players_stats.json'
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, 'w') as f:
                json.dump(response.json(), f)
        else:
            return {
            'status_code': response.status_code,
            'error': response.json()
        }

if __name__ == "__main__":
    get_players_stats()