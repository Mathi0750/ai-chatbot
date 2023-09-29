from datetime import datetime, timedelta
import requests, json, os
import pandas as pd
import numpy as np

"""
Cache data that will be accessed often from sportsdata.io API into different folders.
"""

key = 'd387a5b9dd194415a38faf02fa8468ca'

# Helper Functions

def fetch_current_season_year():
    """
    Fetches the current season year from the sportsdata.io API.
    If the API fails, it calculates the season year based on the current date.
    """
    
    url = f"https://api.sportsdata.io/v3/nfl/scores/json/CurrentSeason?key={key}"
    response = requests.get(url)
    
    if response.status_code == 200:
        return response.json()
    
    # API failed, revert to manual calculation
    today = datetime.now()
    if today.month >= 2:  # Assuming NFL league year starts in February
        return today.year
    return today.year - 1

def fetch_current_week():
    """
    Fetches the current week of the NFL season from the sportsdata.io API.
    If the API fails, it reverts to the manual calculation.
    """
    
    url = f"https://api.sportsdata.io/v3/nfl/scores/json/CurrentWeek?key={key}"
    response = requests.get(url)
    
    if response.status_code == 200:
        return response.json()
    
    # API failed, revert to manual calculation
    today_date = datetime.now().date()
    
    # Manual calculation begins here
    # Assume the NFL season starts on the first Thursday of September.
    start_of_season = datetime(today_date.year, 9, 1).date()
    while start_of_season.weekday() != 3:  # Thursday
        start_of_season += timedelta(days=1)

    weeks_passed = (today_date - start_of_season).days // 7
    return weeks_passed + 1  # Since we want to include the current week

def is_game_finished(game_data):
    """
    Determines if the game is finished based on the game's status.

    Args:
    - game_data (dict): Data about a specific game.

    Returns:
    - bool: True if the game is finished, otherwise False.
    """
    
    game_status = game_data.get("Status", "")
    # Here I assume 'Final' is the status for finished games. Adjust as necessary.
    return game_status == "Final"

def compute_covered_team(game_data):
    """
    Determines which team covered the spread for a given game.

    Args:
    - game_data (dict): Data about a specific game.

    Returns:
    - str: The abbreviation of the team that covered.
    """
    
    home_team = game_data["HomeTeamName"]
    away_team = game_data["AwayTeamName"]
    home_score = game_data["HomeTeamScore"]
    away_score = game_data["AwayTeamScore"]

    # By default, set the spread to None
    home_spread = None

    # Extract the consensus spread
    for odds in game_data["PregameOdds"]:
        if odds["Sportsbook"] == "Consensus":
            home_spread = odds.get("HomePointSpread")
            break

    # If there's no spread or scores are missing, return "Push"
    if home_spread is None or home_score is None or away_score is None:
        return "No Data"

    # Determine which team covered based on the spread
    adjusted_home_score = home_score + home_spread
    if adjusted_home_score > away_score:
        return home_team
    elif adjusted_home_score < away_score:
        return away_team
    else:
        return "Push"


THIS_YEAR = fetch_current_season_year()
THIS_WEEK = fetch_current_week()

# Main functions

def cache_odds_data(season):
    """
    Goes through odds data endpoint and caches the results as well as some precomputed values.
    """
    
    if not os.path.exists('odds_data'):
        os.makedirs('odds_data')

    if "PRE" in season:
        week_range = range(0, 4)  # Preseason Weeks 0-3
    elif "POST" in season:
        week_range = range(1, 5)  # Postseason Weeks 1-4
    else:
        if (str(THIS_YEAR) in season): 
            week_range = range(1, THIS_WEEK+1)  # Regular Season Weeks until current
        else:
            week_range = range(1, 18)

    for current_week in week_range:
        response = requests.get(f'https://api.sportsdata.io/v3/nfl/odds/json/GameOddsByWeek/{season}/{current_week}?key={key}')
            
        if response.status_code == 200:
            week_data = pd.DataFrame(json.loads(response.text))
            
            pregameOdds_list = []
            columns_to_keep = ['Sportsbook', 'HomeMoneyLine', 'AwayMoneyLine', 'DrawMoneyLine', 'HomePointSpread', 'AwayPointSpread', 'HomePointSpreadPayout', 'AwayPointSpreadPayout', 'OverUnder', 'OverPayout', 'UnderPayout', 'OddType']
            accepted_books = {'Consensus', 'DraftKings', 'FanDuel', 'PointsBet', 'BetMGM', 'Caesars'}
    
            week_data['index'] = week_data.index  # Add this line to directly set the original index as a column

            for index, row in week_data.iterrows():
                pregameOdds = pd.DataFrame(row['PregameOdds'])
                pregameOdds = pregameOdds[pregameOdds['Sportsbook'].isin(accepted_books)]
                pregameOdds = pregameOdds[columns_to_keep]
                pregameOdds['index'] = index

                pregameOdds_list.append(pregameOdds)

            all_pregameOdds = pd.concat(pregameOdds_list, ignore_index=True)
            grouped_pregameOdds = all_pregameOdds.groupby('index').apply(lambda x: x.to_dict('records')).reset_index(name='Odds')
    
            if len(week_data) == 1:
                week_data['Odds'] = pd.Series([grouped_pregameOdds['Odds'].iloc[0]])
            else:
                week_data = pd.merge(week_data, grouped_pregameOdds, on='index', how='left')
            
            # Add the covered team column only if the game is finished
            week_data['CoveredTeam'] = week_data.apply(
                lambda row: compute_covered_team(row) if is_game_finished(row) else "Pending", 
                axis=1
            )
            
            week_data.drop(columns=['index', 'ScoreId', 'AwayTeamId', 'HomeTeamId', 'GlobalGameId', 'GlobalAwayTeamId', 'GlobalHomeTeamId', 'PregameOdds', 'LiveOdds', 'AlternateMarketPregameOdds'], inplace=True)

            if (THIS_WEEK != current_week):
                week_data.to_csv(f'odds_data/{season}_{current_week}.csv', index=False)
            else:
                week_data.to_csv(f'odds_data/current_week.csv', index=False)
        else:
            print(f"Failed to fetch odds data for Week {current_week} of Season {season}. Status Code: {response.status_code}")

def cache_scores_data(season):
    """
    Caches all data for the given season from the ScoresByWeek endpoint.
    """
    
    if not os.path.exists('scores_data'):
        os.makedirs('scores_data')

    if "PRE" in season:
        week_range = range(0, 4)  # Preseason Weeks 0-3
    elif "POST" in season:
        week_range = range(1, 5)  # Postseason Weeks 1-4
    else:
        if (str(THIS_YEAR) in season): 
            week_range = range(1, THIS_WEEK)  # Regular Season Weeks until current
        else:
            week_range = range(1, 18)
        
    for current_week in week_range:
        response = requests.get(f'https://api.sportsdata.io/v3/nfl/scores/json/ScoresByWeek/{season}/{current_week}?key={key}')
        
        if response.status_code == 200:
            week_data = pd.DataFrame(json.loads(response.text))
            week_data.to_csv(f'scores_data/{season}_{current_week}.csv', index=False)
            
        else:
            print(f"Failed to fetch scores data for Week {current_week} of Season {season}. Status Code: {response.status_code}")

def cache_schedule_data(season):
    """
    Caches all data for the given season from the Schedule endpoint.
    """
    
    if not os.path.exists('schedule_data'):
        os.makedirs('schedule_data')

    response = requests.get(f'https://api.sportsdata.io/v3/nfl/scores/json/Schedules/{season}?key={key}')
    
    if response.status_code == 200:
        schedule_data = pd.DataFrame(json.loads(response.text))
        schedule_data.to_csv(f'schedule_data/{season}.csv', index=False)
        
    else:
        print(f"Failed to fetch schedule data for Season {season}. Status Code: {response.status_code}")

def cache_ats_data(current_season_type, season_year):
    """
    Cache the ATS data from the last 10 weeks leading up to today's date, 
    including data from the previous season type if necessary.
    """

    if not os.path.exists('ats_data'):
        os.makedirs('ats_data')

    weeks_needed = 10
    weeks_data = []
    
    end_week = fetch_current_week()
    start_week = end_week - weeks_needed + 1

    while weeks_needed > 0:
        for week in range(end_week, start_week - 1, -1): # Go backwards from end_week to start_week
            filename = f'odds_data/{current_season_type}_{week}.csv'
            if not os.path.exists(filename):
                if (THIS_WEEK == week):
                    week_data = pd.read_csv("odds_data/current_week.csv")
                else:
                    continue
            else:
                week_data = pd.read_csv(filename)
            
            
            week_results = week_data[['HomeTeamName', 'AwayTeamName', 'CoveredTeam']].to_dict(orient='records')
            weeks_data.append({
                'season_type': current_season_type,
                'week': week,
                'results': week_results
            })

            weeks_needed -= 1
            if weeks_needed == 0:
                break

        # If we still need more weeks, switch to the previous season type
        if weeks_needed > 0:
            if current_season_type.endswith("REG"):
                current_season_type = str(season_year) + "PRE"
                end_week = 3
            elif current_season_type.endswith("PRE"):
                weeks_needed = 0
            start_week = end_week - weeks_needed + 1

    with open(f'ats_data/ATS_last_10_weeks.json', 'w') as f:
        json.dump(weeks_data, f)

def cache_old_data():
    """
    Cache data for previous seasons as well as the recent 2023PRE season.
    Only caches data if the corresponding files don't exist.
    """
    
    seasons_to_cache = ["2023PRE", "2022POST", "2022REG", "2022PRE"] # add more if needed
    
    for season in seasons_to_cache:
        # Check for schedule data
        if not os.path.exists(f'schedule_data/{season}.csv'):
            print(f"Caching schedule data for {season}")
            cache_schedule_data(season)
            print(f"Done. Schedule data cached for {season}")
        
        # Check for odds data for each week
        if "PRE" in season:
            week_range = range(0, 4)
        elif "POST" in season:
            week_range = range(1, 5)
        else:
            week_range = range(1, 18)
        
        for week in week_range:
            if not os.path.exists(f'odds_data/{season}_{week}.csv'):
                print(f"Caching odds data for {season} Week {week}")
                cache_odds_data(season)
                print(f"Done. Odds data cached for {season} Week {week}")

        # Check for scores data for each week
        for week in week_range:
            if not os.path.exists(f'scores_data/{season}_{week}.csv'):
                print(f"Caching scores data for {season} Week {week}")
                cache_scores_data(season)
                print(f"Done. Scores data cached for {season} Week {week}")


if (__name__ == '__main__'):
    season_year = fetch_current_season_year()

    cache_old_data()
    
    # Cache data for regular season
    season_type_reg = str(season_year) + 'REG'
    print("Caching schedule data for regular season")
    cache_schedule_data(season_type_reg)
    print("Done. Caching odds data for regular season")
    cache_odds_data(season_type_reg)
    print("Done. Caching scores data for regular season")
    cache_scores_data(season_type_reg)

    print("Done. Caching ATS data.")
    cache_ats_data(season_type_reg, season_year)

    print("Done. Finished.")