"""
Cleaned up way to load the gamelog data from BBR scraping.
"""

import pandas
from pandas import DataFrame
from .io import load_full_gamelogs

prepared_bbr_data = None
prepared_unindexed_data = None

def load_gamelogs():
  """
  Get the gamelogs we scraped from BBR in a more usable format
  :rtype: dict[str, DataFrame]
  :return: dict of player to gamelog data
  """
  global prepared_bbr_data

  def cleanup_dataframes(players):
    """
    Perform necessary maintenance & cleanup adjustments on the dataframes to prep them for stats work.
    - Transform minutes:seconds strings into floats
    - Sort all gamelogs by date of game played
    :param dict[str, DataFrame] players: gamelog data
    :return:
    """
    def xform_mins(minstr):
      """ Turn a minutes:seconds string into a float """
      if not isinstance(minstr, basestring):
          return minstr
      mins, secs = minstr.split(':')
      return int(mins) + int(secs) / 60.0
    for player in players:
      if players[player] is not None:
        # Map minutes-seconds transformation to update all dataframe gamelogs
        players[player]['MP'] = players[player]['MP'].map(xform_mins)
        # Sort all gamelogs by date of game played
        players[player] = players[player].sort_index()
    return players

  # Cache a single copy of the assembled dataframe dict so we never have to reload while the program runs
  if prepared_bbr_data is not None:
    return prepared_bbr_data
  raw_bbr_data = load_full_gamelogs()
  # Create dict of actual dataframes keyed off of player ID
  try:
    for player in raw_bbr_data:
      if 'gamelog_data' not in raw_bbr_data[player]:
        print 'gamelog data not found for ', player
    gamelog_dict = {player: raw_bbr_data[player]['gamelog_data'] for player in raw_bbr_data}
  except Exception as ex:
    import ipdb
    ipdb.set_trace()
  gamelog_dict = cleanup_dataframes(gamelog_dict)
  prepared_bbr_data = gamelog_dict

  return prepared_bbr_data

def load_all_game_data():
  """
  Return a gigantic dataframe containing every single game we have information for.
  :return pandas.DataFrame: all game rows
  """
  global prepared_unindexed_data
  if prepared_unindexed_data is not None:
    return prepared_unindexed_data
  unindexed_dfs = []
  for bref_id, dataframe in load_gamelogs().iteritems():
    # Dataframe is indexed by date -- delete existing date column and use this one
    dataframe.index.name = 'date'
    if 'Date' in dataframe.columns:
      dataframe.drop('Date', axis=1, inplace=True) # get rid of this to avoid confusion
    unindexed_df = dataframe.reset_index()       # moves current date index to a column
    # Add player ID as a column to the dataframe for future joining purposes!
    unindexed_df['bref_id'] = pandas.Series(data=bref_id, index=unindexed_df.index)
    unindexed_dfs.append(unindexed_df)
  prepared_unindexed_data = pandas.concat(unindexed_dfs, ignore_index=True)
  return prepared_unindexed_data

def all_loaded_players():
  """
  Get a list of all BBR players with loaded data.
  :return list[str]:
  """
  return list(load_gamelogs().keys())