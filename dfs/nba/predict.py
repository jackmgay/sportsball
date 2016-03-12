"""
Predict player values (using a pre-built model) for a game.
This should support games which have not yet taken place, as well as past games.
"""
import pandas as pd, numpy as np
from dfs.extdata.bbr.gamelogs import load_all_game_data
from dfs.nba.featurizers import salary_fzr
from dfs.nba.playerid import get_position, team_tla, name2nbaid
from dfs.fanduel2.io import get_game_info, get_loaded_games, read_player_csv
import datetime
from dateutil.parser import parse

def get_eligible_players_df(game_day, fd_game_id=None, guess_historical=False):
  """
  Get a dataframe of eligible NBA players on the target day for a particular FanDuel game ID.
  :param datetime.datetime game_day: day to grab players for
  :param int game_id: FanDuel game ID
  :param bool guess_historical: whether to guess who was an "eligible" player that day -- only include anyone who actually played
  :return pd.DataFrame: rows for eligible players
  """
  parsed_date = parse(game_day)
  if not guess_historical:
    if fd_game_id is None:
      print "Loading possible game IDs for this day..."
      loaded_games = get_loaded_games('nba')
      loaded_games['gameday'] = loaded_games['gamedate'].apply(lambda d: d.date())
      todays_games = loaded_games[loaded_games['gameday'] == parsed_date.date()]
      #import IPython
      #IPython.embed()
      chosen_game = todays_games.iloc[0]
      fd_game_id = chosen_game['game_id']
      print 'Choosing game', chosen_game['title'], '(ID', fd_game_id, ')'
      print '  Starts at:', chosen_game['gameday']
      print '  Fee: $', chosen_game['fee']
    player_df = read_player_csv('nba', parsed_date, fd_game_id)
    player_df['Tm'] = player_df['Team'].apply(team_tla)
    player_df['Opp'] = player_df['Opponent'].apply(team_tla)
    player_df['date'] = pd.Series(parsed_date, index=player_df.index)
    player_df['bref_id'] = player_df.apply(lambda row: name2nbaid(row['First Name'] + ' ' + row['Last Name'], row['Tm']),
                                           axis=1)
    player_df['bref_id_confidence'] = player_df.apply(lambda row: name2nbaid(row['First Name'] + ' ' + row['Last Name'], row['Tm'], get_confidence=True)[1],
                                       axis=1)
    player_df.rename(columns={'Position': 'pos', 'Salary': 'salary'}, inplace=True)
    # Dump anyone we had low confidence in matching by team/name
    player_df = player_df[player_df['bref_id_confidence'] >= 75]
    # Finally, dump anyone who is injured (todo: keep GTD players?)
    safe_players = player_df[player_df['Injury Indicator'].isnull()].copy()
    return safe_players
  elif guess_historical and parsed_date.date() >= datetime.date.today():
    raise ValueError("Can't guess historical players for a game that hasn't happened yet...")
  else:
    # load historical players from our archive as if it were a game about to be played
    game_date = parsed_date.date()
    all_games = load_all_game_data()
    day_players = all_games[all_games['date'] == game_date]
    actual_players = day_players[day_players['MP'] > 0]
    return_df = actual_players[['bref_id', 'Opp', 'Tm']].copy()
    # set the date to the gameday everywhere
    return_df['date'] = pd.Series(parsed_date, index=return_df.index)
    # Grab salary information from our scraped numberfire cache
    return_df['salary'] = return_df.apply(lambda row: salary_fzr(row), axis=1)
    # Assemble player positional information from our gamelogs
    return_df['pos'] = return_df['bref_id'].apply(lambda pid: get_position(pid))
    return return_df

def dump_eligible_players_df(outfile, *args, **kwargs):
  """
  Get a dataframe of eligible NBA players on the target day for a particular FanDuel game ID and save to file
  Replaces dumping for the live pipeline.
  :param str outfile: save pickle dump to this file
  :param datetime.datetime game_day: day to grab players for
  :param int game_id: FanDuel game ID
  :param bool guess_historical: whether to guess who was an "eligible" player that day -- only include anyone who actually played
  :return pd.DataFrame: rows for eligible players
  """
  df = get_eligible_players_df(*args, **kwargs)
  df.to_pickle(outfile)