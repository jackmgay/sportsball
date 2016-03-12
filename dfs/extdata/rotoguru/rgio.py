import os
import pandas
import dateutil

from dfs import GLOBAL_ROOT
from dfs.extdata.common.io import combine_dataframe_into_pickle_file

rg_data_dir = os.path.join(GLOBAL_ROOT, 'db/{sport}/salary_data/rotoguru/')

def get_gameday_filename(sport, game_day):
  filename = os.path.join(rg_data_dir.format(sport=sport), game_day.isoformat())
  return filename

def save_rg_salary_info(sport, game_day, dataframe):
  """
  Save salary and position data from the game day to a file
  :param str sport: sport salaries are from
  :param datetime.datetime game_day: day these salaries are from
  :param pandas.DataFrame dataframe: dataframe of salaries
  :return:
  """
  combine_dataframe_into_pickle_file(dataframe, get_gameday_filename(sport, game_day))

def load_rg_salary_info(sport, game_day):
  """
  Load previously saved dataframes of salary information
  :param str game_day: day to load salary info for
  :return pandas.DataFrame: salary info for that day
  """
  game_date = dateutil.parser.parse(game_day)
  df = pandas.read_pickle(get_gameday_filename(sport, game_date))
  # Manually parse salary row to integers here
  df["Salary"] = df["Salary"].apply(int)
  return df
