import os
import pandas
import dateutil
from dfs.extdata.common.io import combine_dataframe_into_pickle_file

sbr_data_dir = os.path.join(GLOBAL_ROOT, 'db/{sport}/odds/sportsbookreview/')

# Find the odds file for a specific day
def get_gameday_filename(sport, game_day):
  filename = os.path.join(sbr_data_dir.format(sport=sport), game_day.isoformat())
  return filename

def save_sbr_odds_info(sport, game_day, dataframe):
  """
  Save odds data from the game day to a file
  :param str sport: sport odds are from
  :param datetime.datetime game_day: day these odds are from
  :param pandas.DataFrame dataframe: dataframe of odds
  :return:
  """
  combine_dataframe_into_pickle_file(dataframe, get_gameday_filename(sport, game_day))

def load_sbr_odds_info(sport, game_day):
  """
  Load previously saved dataframes of odds information
  :param str game_day: day to load odds info for
  :return pandas.DataFrame: oddsinfo for that day
  """
  game_date = dateutil.parser.parse(game_day)
  df = pandas.read_pickle(get_gameday_filename(sport, game_date))
  return df
