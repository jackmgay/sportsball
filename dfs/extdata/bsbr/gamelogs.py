"""
Cleaned up way to load the gamelog data from BSBR scraping.
"""

from pandas import DataFrame
from .scrape_bsbr import load_full_gamelogs

prepared_bsbr_data = {}

def load_gamelogs(datatype='batting'):
  """
  Get the gamelogs we scraped from baseball-reference in a more usable format
  :param str datatype: type of gamelog to load -- only implemented for batting data at the moment
  :rtype: dict[str, DataFrame]
  :return: dict of player to gamelog data
  """
  global prepared_bsbr_data

  if datatype not in ['batting', 'pitching']:
    raise NotImplementedError("You tried to load some crazy type of data besides batting and pitching?")

  # Cache a single copy of the assembled dataframe dict so we never have to reload while the program runs
  if datatype in prepared_bsbr_data:
    return prepared_bsbr_data[datatype]
  raw_bsbr_data = load_full_gamelogs()
  gamelog_dict = {}
  # Create dict of actual dataframes keyed off of player ID
  for player in raw_bsbr_data:
    if datatype not in raw_bsbr_data[player]:
      pass
      #print datatype, 'data not found for', player
      #print 'keys for', player, raw_bsbr_data.get(player).keys()
    else:
      # We used to call dropna here as well, because there were NaT rows for player trades!
      # I think that we no longer pull those out, and this line now causes more harm then good by dropping
      # stat rows if, for example, there is no fantasy score data present for the player on baseball-reference.
      sorted_player_data = raw_bsbr_data[player][datatype].sort_index()
      # We don't include pitcher's hitting statistics. It messes up external data expansion, and I'm not sure
      # that you even *get* pitcher fantasy points? Certainly you don't get enough to make it worthwhile.
      if datatype == 'batting' and 'P' in sorted_player_data.loc[sorted_player_data.first_valid_index(), 'Pos']:
        continue
      gamelog_dict[player] = sorted_player_data
  prepared_bsbr_data[datatype] = gamelog_dict
  return gamelog_dict

def all_loaded_players():
  """
  Get a list of all BBR players with loaded data.
  :return list[str]:
  """
  return list(load_gamelogs().keys())