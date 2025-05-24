import requests
import os
import json
from dotenv import load_dotenv

load_dotenv()

API_URL = os.getenv('FBREF_BASE_URL')
API_KEY = os.getenv('FBREF_API_KEY')

def get_team_details():
    """
    Fetches Team ID and Team Name for the specified league and season.
    """

    params = {
            'league_id': '9' ,  
            'season_id': '2024-2025'
        }
    HEADERS = {'X-API-KEY': API_KEY}
    response = requests.get(f'{API_URL}/team-season-stats', params=params, headers=HEADERS)
    if response.status_code == 200:
        team_stats = response.json()

        team_details = []

        for team_data in team_stats['data']:
            team_id = team_data['meta_data']['team_id']
            team_name = team_data['meta_data']['team_name']
            team_details.append({
                'team_id': team_id,
                'team_name': team_name
            })

        return team_details

    else:
        raise Exception(f"Error fetching team details: {response.status_code} - {response.text}")
    
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
        filename = f'data/raw/team_stats.json'
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w') as f:
            json.dump(response.json(), f)
        print(f"Data saved to {filename}")
        return response.json()
    else:
        raise Exception(f"Error fetching team statistics: {response.status_code} - {response.text}")

if __name__ == "__main__":
    get_team_stats()