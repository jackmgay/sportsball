"""
Read a player CSV file that was downloaded from FD
"""
import os
import re
import pandas as pd
import pickle

from dfs import GLOBAL_ROOT
from dfs.extdata.common.io import combine_dataframe_into_csv

root_data_dir = os.path.join(GLOBAL_ROOT, 'db/{sport}/fanduel')
game_summary_file = 'summary_df.csv'

lineup_format = 'lineup-{datestr}-{game_id}.txt'


def get_csv_name(sport, datestr, game_id):
  if sport == 'mlb':
    sport_str = 'MLB'
  elif sport == 'nba':
    sport_str = 'NBA'
  else:
    raise NotImplementedError
  return 'FanDuel-{sport_str}-{datestr}-{game_id}-players-list.csv'.format(sport_str=sport_str,
                                                                           datestr=datestr,
                                                                           game_id=game_id)

def get_csv_file(sport, game_date, game_id):
  datestr = game_date.strftime('%Y-%m-%d')
  return os.path.join(root_data_dir.format(sport=sport),
                        get_csv_name(sport, datestr, game_id))

def read_day_csv(sport, game_date):
  """
  Get *some* player list info for this date (there may be more games with different player lists available as well).
  :param datetime.datetime game_date: date to pull list for
  :return tuple[int, pandas.DataFrame]: game id of the data, dataframe of player info
  """
  datestr = game_date.strftime('%Y-%m-%d')
  game_file_regex = re.compile(get_csv_name(sport=sport, datestr=datestr, game_id='(?P<game_id>\d+)'))
  for filename in os.listdir(root_data_dir):
    match = game_file_regex.match(filename)
    if match: # Reuse load CSV file code just in case
      return int(match.group('game_id')), read_player_csv(sport, game_date, match.group('game_id'))
  return None # No files for this game date

def read_player_csv(sport, game_date, game_id):
  """ Pull a MLB player csv downloaded from FanDuel and return it
  :param datetime.datetime game_date: game date
  :param int game_id: game id
  :return pandas.DataFrame: parsed player csv
  """
  path = get_csv_file(sport, game_date, game_id)
  return pd.read_csv(path)

def add_game_info_to_db(sport, fd_game_id, parsed_cap, parsed_fee, fd_gamedate, fd_table_id, game_title, lineup=None):
  # We use a pandas pickle file as a database...
  game_df = pd.DataFrame([[fd_game_id, parsed_cap, parsed_fee, fd_gamedate, fd_table_id, game_title]],
                         columns=['game_id', 'cap', 'fee', 'gamedate', 'table_id', 'title'])
  combine_dataframe_into_csv(game_df,
                             os.path.join(root_data_dir.format(sport=sport),
                                          game_summary_file),
                             date_cols=['gamedate'],
                             ignore_index=True)
  if lineup:
    save_lineup_to_file(lineup, get_lineup_filename('mlb', fd_gamedate, fd_game_id))

def get_loaded_games(sport):
  summary_csv = os.path.join(root_data_dir.format(sport=sport), game_summary_file)
  if os.path.exists(summary_csv):
    return pd.read_csv(summary_csv,
                       index_col=0,
                       parse_dates=['gamedate'])
  else:
    print 'No games loaded so far (!)'
    return pd.DataFrame([], columns=['game_id', 'cap', 'fee', 'gamedate', 'table_id', 'title'])

def get_game_info(sport, game_date, game_id):
  games = get_loaded_games(sport)
  games['gameday'] = games['gamedate'].apply(lambda date: date.date())
  print games
  print game_date.date()
  games = games[games['gameday'] == game_date.date()]
  print games
  games = games[games['game_id'] == game_id]
  if len(games):
    return games.loc[games.first_valid_index()]
  else:
    return None

def get_available_game_ids(sport, game_date, strict_time=True):
  """
  Get a list of fanduel games we know about on the target game date and time.
  :param basestr sport: cmon now
  :param datetime.datetime game_date: date / time of game
  :param bool strict_time: if false, return all game id's on the day regardless of start time
  :return set[int]: available game ids
  """
  games = get_loaded_games(sport)
  games = games[games['gamedate'] == game_date]
  if not strict_time:
    raise NotImplementedError("haven't built support for all-games-on-day yet")
  return set(games['game_id'].values)

def game_was_already_scraped(sport, game_date, game_id):
  return get_game_info(sport, game_date, game_id) is not None

def get_lineup_filename(sport, game_date, game_id):
  datestr = game_date.strftime('%Y-%m-%d')
  return os.path.join(root_data_dir.format(sport=sport),
                      lineup_format.format(datestr=datestr, game_id=game_id))

def save_lineup_to_file(lineup, filename):
  print 'Saving lineup info (loaded %d teams, %d players) to file: %s' \
        % (len(lineup.loaded_teams), len(lineup.parsed_players), filename)
  with open(filename, 'w') as outf:
    pickle.dump(lineup, outf)

def read_lineup_csv(sport, game_date, game_id):
  """ Pull lineup data downloaded from FanDuel and return it
  :param datetime.datetime game_date: game date
  :param int game_id: game id
  :return players:
  """
  with open(get_lineup_filename(sport, game_date, game_id), 'r') as inf:
    return pickle.load(inf)
