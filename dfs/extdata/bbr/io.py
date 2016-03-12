"""
Consolidate save / load functions here to keep the other files readable
"""
import os
import sys
import simplejson as json
import cPickle as pickle
import logging

from dfs.extdata.common.io import combine_dataframe_into_pickle_file

from .common import bbr_id_regex, datadir

# because Pickle -- try to fix this later
sys.setrecursionlimit(50000)

def save_overview_dict(players):
  path = os.path.join(datadir, 'nba_player_data.dict')
  logging.info('save_overview_dict: saving overview information from player dict to %s', str(path))
  player_archive = {}
  for name, info_dict in players.items():
    player_archive[name] = {
      'gamelog_url_list': info_dict.get('gamelog_url_list', []),
      'overview_url': info_dict.get('overview_url', None),
      'overview_url_content': info_dict.get('overview_url_content', None),
      'info': info_dict.get('info', {})
    }
  json.dump(player_archive, open(path, 'wb'))

def load_overview_dict():
  fn = os.path.join(os.path.expandvars(datadir), 'nba_player_data.dict')
  if not os.path.exists(fn):
    logging.info("load_overview_dict: no player overview dict exists; returning {}")
    return {}
  with open(fn, 'rb') as f:
    json_string = f.read()
    player_archive = json.loads(json_string)
  logging.debug("load_overview_dict: loading and returning player overview dict from file")
  return player_archive

def get_player_filename(bref_id):
  fn = os.path.join(datadir, 'player_data', bref_id)
  return fn

def save_dataframes(players, overwrite=False):
  """
  Save the pandas dataframes (the gamelog_data) from each player in players to a file
  :param dict[str, dict] players: the player dict
  :return:
  """
  saved_dataframes = 0
  for bref_id, attrs in players.iteritems():
    if 'gamelog_data' in attrs:
      target_file = get_player_filename(bref_id)
      combine_dataframe_into_pickle_file(attrs['gamelog_data'], target_file, overwrite=overwrite)
      saved_dataframes += 1
  logging.debug('Saved %d dataframes to %s', saved_dataframes, datadir)

def load_dataframes(players):
  """
  Load previously saved dataframes of gamelog data
  :param dict[str, dict] players: the player dict (bref id -> dict)
  :return dict[str, dict]: the player dict, loaded from pickled data
  """
  loaded = 0
  for bref_id in players.keys():
    target_file = os.path.join(datadir, 'player_data', get_player_filename(bref_id))
    if os.path.exists(target_file):
      with open(target_file, 'r') as inf:
        players[bref_id]['gamelog_data'] = pickle.load(inf)
      loaded += 1
  logging.debug('loaded %d dataframes from %s', loaded, datadir)
  return players

def create_player_dict(pids_to_load):
  '''
  Initialize the player dictionary.
  :param dict [str, str] names: name to URL dictionary
  :return:
  '''
  players = {}
  for pid, url in pids_to_load.iteritems():
    players[pid] = {'overview_url':url}
    players[pid]['overview_url_content'] = None
    players[pid]['gamelog_url_list'] = []
    players[pid]['gamelog_data'] = None
    players[pid]['info'] = {}
  return players

def load_full_gamelogs():
  """
  Exportable loading function for using basketball reference data elsewhere in the project
  :return dict[str, dict]: The global player dict
  """
  players = load_overview_dict()
  players = load_dataframes(players)
  return players