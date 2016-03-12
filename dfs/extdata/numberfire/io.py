import os
import pandas
import datetime

from .utils import get_histplayerinfo_filename, nf_data_dir, get_salary_filename, get_overview_file_dir
from dfs.extdata.common.io import combine_dataframe_into_pickle_file
from dfs.extdata.bbr.gamelogs import all_loaded_players

def save_nf_overview_data(sport, overview_data):
  """ Save a recently-scraped batch of overview data from Numberfire
  :param DataFrame overview_data: overview data from Numberfire to save
  """
  if not os.path.exists(get_overview_file_dir(sport)):
    os.makedirs(get_overview_file_dir(sport))
  overview_file = os.path.join(get_overview_file_dir(sport), datetime.date.today().isoformat())
  overview_data.to_pickle(overview_file)

def save_nf_histplayerinfo(sport, identifier_dict):
  """
  Save the pandas dataframes (numberfire prediction logs) from each player in the dict to a file.
  For MLB the keys in the dict are bref id's. See load_nf_histplayerinfo
  :param dict[str, DataFrame] identifier_dict: the dict of player name -> numberfire prediction dataframe
  :return:
  """
  saved_dataframes = 0
  for identifier, df in identifier_dict.iteritems():
    target_file = get_histplayerinfo_filename(sport, identifier)
    combine_dataframe_into_pickle_file(df, target_file)
    saved_dataframes += 1
  print 'Saved %d dataframes of predictions' % saved_dataframes

def save_nf_salary_info(sport, salary_dict):
  """
  Save salary data
  :param dict[str, DataFrame] player_dict: the dict of player name -> numberfire prediction dataframe
  :return:
  """
  saved_dataframes = 0
  for player, df in salary_dict.iteritems():
    target_file = get_salary_filename(sport, player)
    combine_dataframe_into_pickle_file(df, target_file)
    saved_dataframes += 1
  print 'Saved %d dataframes of player salaries' % saved_dataframes

def load_nf_overview_data(sport):
  """ Load the most recent overview data from Numberfire.
   Includes individual player-to-slug mappings so downloading history is feasible.
  :return DataFrame: overview data
  """
  overview_data_dir = get_overview_file_dir(sport)
  overview_files = [os.path.join(overview_data_dir, f) for f in os.listdir(overview_data_dir)]
  overview_file = max(overview_files, key=os.path.getctime)
  return pandas.read_pickle(overview_file)

def load_nf_histplayerinfo(sport, identifiers_to_load):
  """
  Load previously saved dataframes of numberfire prediction data.
  :param str sport: which sport!
  :param list[str] identifiers_to_load:  id of players to load
  :return dict[str, DataFrame]: dict of player -> prediction data for player
  """
  loaded = 0
  histplayerinfo_dict = {}
  for identifier in identifiers_to_load:
    target_file = get_histplayerinfo_filename(sport, identifier)
    if os.path.exists(target_file):
      histplayerinfo_dict[identifier] = pandas.read_pickle(target_file)
      # Attempt to convert the index to time based if possible
      if histplayerinfo_dict[identifier] is not None and 'date' in histplayerinfo_dict[identifier].columns:
        histplayerinfo_dict[identifier].set_index('date', inplace=True)
      loaded += 1
  return histplayerinfo_dict

def load_nf_salaryinfo(sport, players):
  """
  Load previously saved dataframes of numberfire salary data
  :param list[str] players: players to load
  :return dict[str, DataFrame]: dict of player -> salary data for player
  """
  loaded = 0
  player_dict = {}
  for player in players:
    target_file = get_salary_filename(sport, player)
    if os.path.exists(target_file):
      player_dict[player] = pandas.read_pickle(target_file)
      # Attempt to convert the index to time based if possible
      if player_dict[player] is not None and 'date' in player_dict[player].columns:
        player_dict[player].set_index('date', inplace=True)
      loaded += 1
  return player_dict

_cached_salary_dicts = None

def get_day_salary_df(sport, gamedate, game="FanDuel"):
  raise NotImplementedError("whoops -- NF salary data doesn't seem complete enough for us yet")