"""
New NBA player ID strategy:

Keep table with active player bref ID's, names, teams, positions.
Update it on NBA scrape from BREF.
"""
import warnings
from fuzzywuzzy import process as fzmatch
from dfs.extdata.bbr.mapping_df import load_player_table

mapping_df = load_player_table()
unset_index = mapping_df.reset_index()
unset_index.fillna('', inplace=True)
mapping_by_name = unset_index.set_index('name')
mapping_by_id = unset_index.set_index('brefid')

def name2nbaid(player_name, player_team=None, get_confidence=False):
  """ Consolidate all fuzzy-matching name lookups into this function alone.
  :param str player_name: player name
  :return:
  """
  tla = None
  if player_team is not None:
    tla = team_tla(player_team)
    usable_mapping = mapping_by_name[mapping_by_name['team'] == tla]
  else:
    usable_mapping = mapping_by_name
  choices = usable_mapping.index
  match, score = fzmatch.extractOne(player_name, choices)
  if score < 75 and not get_confidence:
    # We only warn if we haven't been asked to give our confidence in the match.
    # If we are returning confidence, we assume the caller is dealing with it intelligently.
    # I think this will be useful for loading fewer garbage stats when matches don't happen.
    warnings.warn("Low confidence NBA name match: %s [%s] -> %s (confidence=%d)" % (player_name, tla, match, score))
  if get_confidence:
    return usable_mapping.loc[match, 'brefid'], score
  else:
    return usable_mapping.loc[match, 'brefid']

def id2name(player_id):
  return mapping_by_id.loc[player_id, 'name']

def team_tla(team, get_confidence=False):
  """
  Try to come up with a canonical TLA for the given team. The values we choose from are in mapping_df.
  :param str team:
  :return str: TLA
  """
  if team in _acceptable_tlas:  # We can't use _team_tlas here for a shortcut since we ban some bad TLAs.
    return team
  else:
    match, score = fzmatch.extractOne(team, _team_choices.keys())
    actual_team = _team_choices[match]
  if score < 90 and not get_confidence:
    warnings.warn("Low confidence NBA team match: %s -> %s -> %s (confidence=%d)" % (team, match, actual_team, score))
  if get_confidence:
    return actual_team, score
  else:
    return actual_team

_team_tlas = set(mapping_df['team'].values)

# add aliases in the form of (TLA, alias)
_team_alias_list = [
  ('ATL', "Atlanta Hawks"),
  ('BOS', "Boston Celtics"),
  ('BRK', "Brooklyn Nets"),
  ('BRK', "BKN"),
  ('CHI', "Chicago Bulls"),
  ('CHO', "Charlotte Hornets"),
  ('CLE', "CLV"),
  ('CLE', "Cleveland Cavaliers"),
  ('DAL', "Dallas Mavericks"),
  ('DEN', "Denver Nuggets"),
  ('DET', "Detroit Pistons"),
  ('GSW', "Golden State Warriors"),
  ('GSW', "Golden State"),
  ('HOU', "Houston Rockets"),
  ('IND', "Indiana Pacers"),
  ('LAC', "Los Angeles Clippers"),
  ('LAC', "L.A. Clippers"),
  ('LAL', "Los Angeles Lakers"),
  ('LAL', "L.A. Lakers"),
  ('MEM', "Memphis Grizzlies"),
  ('MIA', "Miami Heat"),
  ('MIL', "Milwaukee Bucks"),
  ('MIN', "Minnesota Timberwolves"),
  ('NJN', "New Jersey Nets"),
  ('NOP', 'NOH'),
  ('NOP', "New Orleans Hornets"),    # Force Hornets references to be understood as Pelicans refs
  ('NOP', "New Orleans Pelicans"),
  ('NYK', "New York Knicks"),
  ('OKC', "Oklahoma City Thunder"),
  ('ORL', "Orlando Magic"),
  ('PHI', "Philadelphia 76ers"),
  ('PHO', "Phoenix Suns"),
  ('POR', "Portland Trail Blazers"),
  ('SAC', "Sacramento Kings"),
  ('SAS', "SA"),  # SA = San Antonio
  ('SAS', "San Antonio Spurs"),
  ('SEA', "Seattle SuperSonics"),    # lol
  ('TOR', "Toronto Raptors"),
  ('UTA', "Utah Jazz"),
  ('WAS', "Washington Wizards"),
] + [(tla, tla) for tla in _team_tlas]  # also consider that the TLA may have just been misspelled.


# There are some old team TLA's with no active players that I am just banning here to make it easy.
_bad_tlas = ["NOH"]
_filtered_alias_list = filter(lambda (tla, _): tla not in _bad_tlas, _team_alias_list)
_team_choices = {alias: tla for tla, alias in _filtered_alias_list}
_acceptable_tlas = set(_team_choices.values())




def get_position(player_id):
  """
  Quick load a player's position by his bref ID
  :param str player_id: bref_id
  :return str: position
  """
  return mapping_by_id.loc[player_id]['pos']