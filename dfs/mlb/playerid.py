import warnings
from fuzzywuzzy import process as fzmatch
from dfs.extdata.crunchtime import load_latest_mapping

mapping_df = load_latest_mapping()
unset_index = mapping_df.reset_index()
unset_index.fillna('', inplace=True)
mapping_by_brefid = unset_index.set_index('bref_id')
mapping_by_brefname = unset_index.set_index('bref_name')
mapping_by_mlbname = unset_index.set_index('mlb_name')
mapping_by_espnid = unset_index.set_index('espn_id')

known_teams = set(mapping_by_mlbname['mlb_team'].values)

def name2mlbid(player_name, player_team=None, get_confidence=False):
  """ Consolidate all fuzzy-matching name lookups into this function alone.
  :param str player_name: player name
  :return:
  """
  if player_team is not None:
    if player_team not in known_teams:
      warnings.warn("UNKNOWN TEAM %s" % player_team)
      warnings.warn('known teams are %s' % ','.join(set(mapping_by_mlbname['mlb_team'].values)))
    usable_mapping = mapping_by_mlbname[mapping_by_mlbname['mlb_team'] == player_team]
  else:
    usable_mapping = mapping_by_mlbname
  choices = usable_mapping.index
  match, score = fzmatch.extractOne(player_name, choices)
  if score < 75:
    warnings.warn("Low confidence MLB name match: %s -> %s (confidence=%d)" % (player_name, match, score))
  if get_confidence:
    return score
  else:
    return usable_mapping.loc[match, 'mlb_id']

def name2brefid(player_name, player_team=None):
  # consider calling this 'lookupbyname' and asking for what you want back?
  mlb_id = name2mlbid(player_name, player_team)
  return mapping_df.loc[mlb_id, 'bref_id']

def name2brefid_confidence(player_name, player_team=None):
  # Get the confidence of a match
  return name2mlbid(player_name, player_team, get_confidence=True)

def name2position(player_name):
  mlb_id = name2mlbid(player_name)
  return mapping_df.loc[mlb_id, 'mlb_pos']

def brefid_is_pitcher(bref_id):
  return mapping_by_brefid.loc[bref_id, 'mlb_pos'] == 'P'

def brefid_is_starting_pitcher(bref_id):
  return mapping_by_brefid.loc[bref_id, 'espn_pos'] == 'SP'

def espnid2mlbid(pid):
  try:
    return mapping_by_espnid.loc[pid, 'mlb_id']
  except KeyError:
    return None

def brefid2mlbid(bref_id):
  try:
    return mapping_by_brefid.loc[bref_id, 'mlb_id']
  except KeyError:
    return None

def mlbid2brefid(mlb_id):
  try:
    return mapping_df.loc[mlb_id, 'bref_id']
  except KeyError:
    return None

def players_equal(name1, name2, team1=None, team2=None):
  """ Attempt to determine if player name 1 = player name 2.
  :param str name1: first p name
  :param str name2: second p name
  :param str team1: first team
  :param str team2: second team
  :return:
  """
  # TODO: actually use teams
  return name2mlbid(name1) == name2mlbid(name2)

def get_all_playerids():
  return mapping_df['bref_id'].values