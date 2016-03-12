import datetime
import logging
import pandas as pd, numpy as np
import random
import os
import pickle

from dfs import GLOBAL_ROOT
from dfs.extdata.bbr.gamelogs import load_gamelogs, load_all_game_data
from dfs.extdata.numberfire.io import load_nf_histplayerinfo, load_nf_salaryinfo
from dfs.extdata.sportsbookreview.sbrio import load_sbr_odds_info

import IPython
logger = logging.getLogger(__name__)
logger.addHandler(logging.FileHandler('./pipeline.log'))
logger.setLevel(logging.DEBUG)

# Gigantic hack -- we just cache expansion results globally for now to speed things up.
# NOT BEST PRACTICE...

GLOBAL_CACHE_DICT = os.path.join(GLOBAL_ROOT, 'expansioncache')

if os.path.exists(GLOBAL_CACHE_DICT):
  expansion_cache = pickle.load(open(GLOBAL_CACHE_DICT, 'rb'))
  logger.info("Loaded %d keys from cache", len(expansion_cache))
else:
  expansion_cache = {}

nf_info_cache = {}
nf_salary_cache = {}
gamelogs = load_gamelogs()
all_game_data = load_all_game_data()
feature_generators = {}
distinct_nf_players_expanded = set()


def featurizer(name=None, output_columns=None, live=True):
  """
  Decorator for functions that expand data by creating new feature columns.
  The decorated expansion function should take as input a new game row from BBR and return
    one row for each value in 'columns', in order.
  A featurizer returns None if it can't load data. This wrapper also transforms that into a pandas.Series
    of np.nan the same length as the number of columns generated, so that unloaded values will
    successfully turn into NaNs.
  :param name: name of featurizer, prepended to each column name
  :param output_columns: list of features generated
  :param bool live: whether to use this featurizer when expanding live data
  :return:
  """
  def decorated(func):
    func_name = name or func.__name__
    assert func_name not in feature_generators
    def wrapper(*args, **kwargs):
      result = func(*args, **kwargs)
      if result is None:
        return pd.Series(data=np.nan, index=output_columns)
      else:
        #print 'result', result
        #print 'index', columns
        return pd.Series(data=result, index=output_columns)
    feature_generators[func_name] = (wrapper, output_columns, live)
    return wrapper
  return decorated

def cached_featurizer(name=None, input_columns=None):
  def decorated(func):
    def wrapper(*args, **kwargs):
      cache_key = name + '|' + str(list(args[0].loc[input_columns].values))
      if cache_key in expansion_cache:
        return expansion_cache[cache_key]
      else:
        result = func(*args, **kwargs)
        expansion_cache[cache_key] = result
        # Don't do this all the time
        if random.random() < 0.001:
          with open(GLOBAL_CACHE_DICT, 'wb') as outf:
            pickle.dump(expansion_cache, outf)
        return result
    return wrapper
  return decorated

def encode_names(feature_name, columns):
  return [feature_name + ': ' + col for col in columns]

scorer = pd.Series({'PTS': 1,
                    'TRB': 1.2,   # BBR calls rebounds (REB) as TRB (total rebounds)
                    'AST': 1.5,
                    'STL': 2,
                    'BLK': 2,
                    'TOV': -1})

# We use this scorer for hauling total fantasy point values out of Numberfire.
numberfire_scorer = pd.Series({'PTS': 1,
                               'REB': 1.2,
                               'AST': 1.5,
                               'STL': 2,
                               'BLK': 2,
                               'TOV': -1})

@featurizer(name="Target", output_columns=['FDFP'], live=False)
def fantasy_points_fzr(row):
  return row[scorer.index].dot(scorer)

@featurizer(name="Last5", output_columns=['PPG'])
@cached_featurizer(name="Last5", input_columns=['bref_id', 'date'])
def last5games_fzr(row):
  player_stats = gamelogs.get(row['bref_id'])
  if player_stats is not None and len(player_stats) > 0:
    player_stats = player_stats[player_stats.index < row['date']].tail(5)
    if len(player_stats) > 0:
      ppg = player_stats.apply(fantasy_points_fzr, axis=1)
      return ppg.mean().values
    # Otherwise no games in the last two weeks, so go ahead and return none.
  return None

_nf_columns = ['Minutes', 'PTS', 'REB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'FP']
@featurizer(name="NF", output_columns=_nf_columns)
@cached_featurizer(name="NF", input_columns=['bref_id', 'date'])
def nf_stats_fzr(row):
  """
  Look up and append Numberfire predictions for the game this row was for.
  """
  def load_nf_info_cached(player):
    """
    Load numberfire information for the player and cache it. Also adds fantasy points.
    :param str player: bref id of player
    :return pd.DataFrame: loaded stats data
    """
    if player in nf_info_cache:
      return nf_info_cache[player]
    else:
      nfdatadict = load_nf_histplayerinfo('nba', [player])
      player_data = nfdatadict.get(player)
      if player_data is not None:
        player_data['FP'] = player_data.apply(lambda row: row[numberfire_scorer.index].dot(numberfire_scorer), axis=1)
        nf_info_cache[player] = player_data
      return player_data
  bref_id = row['bref_id']
  nfdata = load_nf_info_cached(bref_id)
  if nfdata is not None and row['date'] in nfdata.index:
    distinct_nf_players_expanded.add(bref_id)
    loaded_stats = nfdata.loc[row['date'], _nf_columns].values
    if not isinstance(loaded_stats, np.ndarray) or len(loaded_stats) > len(_nf_columns):
      raise ValueError('Something is wrong loading numberfire data for (brefid=%s, date=%s)' % (bref_id, row['date']))
    return list(loaded_stats)
  else: # No data for this player on this date.
    return None

_salary_columns = ["FanDuel Salary"]
@featurizer(name="Salary", output_columns=_salary_columns, live=False)    # Turn this off for live scoring, we will grab salary elsewhere
@cached_featurizer(name="Salary", input_columns=['bref_id', 'date'])
def salary_fzr(row):  # load FanDuel salary from Numberfire
  def load_nf_salary_cached(player):
    # as load_nf_info_cached, load numberfire-derived fanduel salary information for the player and cache it
    if player in nf_salary_cache:
      return nf_salary_cache[player]
    else:
      nfsaldict = load_nf_salaryinfo('nba', [player])
      player_data = nfsaldict.get(player)
      if player_data is not None:
        nf_salary_cache[player] = player_data
      return player_data
  bref_id = row['bref_id']
  nf_salary_data = load_nf_salary_cached(bref_id)
  if nf_salary_data is not None and row['date'] in nf_salary_data.index:
    loaded_salaries = nf_salary_data.loc[row['date'], _salary_columns].values
    if not isinstance(loaded_salaries, np.ndarray) or len(loaded_salaries) > len(_salary_columns):
      raise ValueError('Something is wrong loading salary data for (brefid=%s, date=%s)' % (bref_id, row['date']))
    return list(loaded_salaries)
  else:
    return None

@featurizer(name='Vegas', output_columns=['Spread', 'OverUnder'])
@cached_featurizer(name="Vegas", input_columns=['Tm', 'date'])
def vegas_fzr(row):
  try:
    day_odds = load_sbr_odds_info('nba', row['date'].date().isoformat())
  except IOError:
    logger.warning("No odds at all found for game day %s" % (row['date'].date().isoformat()))
    return None
  try:
    odds = day_odds.loc[row['Tm']]
  except KeyError:
    logger.warning("No odds found for %s on %s" % (row['Tm'], row['date'].date().isoformat()))
    return None
  retval = [odds['spread'], odds['scoreline']]
  return retval

# Future work: see if we can carve this up by position too
opp_ffpg_cache = {}

@featurizer(name='OpponentLast2Weeks', output_columns=['AvgFPPG', 'MaxFPPG', 'FPPMP', 'AvgFPPMP', 'MaxFPPMP'])
@cached_featurizer(name="OpponentLast2Weeks", input_columns=['Opp', 'date'])
def opp_ffpg_fzr(row):
  if (row['Opp'], row['date']) in opp_ffpg_cache:
    return opp_ffpg_cache[(row['Opp'], row['date'])]
  opp = row['Opp']
  cutoff_time = row['date'] - datetime.timedelta(weeks=2)
  against_team = all_game_data[all_game_data['Opp'] == opp]
  before_date = against_team[against_team['date'] < row['date']]
  recent = before_date[before_date['date'] > cutoff_time]
  if len(recent) > 0:
    fps = recent.apply(fantasy_points_fzr, axis=1)['FDFP']
    fppmp = fps.divide(recent['MP'])
    overall_fppmp = sum(fps) / sum(recent['MP'])
    retval = [fps.mean(), fps.max(), overall_fppmp, fppmp.mean(), fppmp.max()]
  else:
    retval = None
  opp_ffpg_cache[(row['Opp'], row['date'])] = retval
  return retval
