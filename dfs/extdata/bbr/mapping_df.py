import os
import pandas
import logging

from dfs import GLOBAL_ROOT

player_list = os.path.join(GLOBAL_ROOT, 'db/nba/player_list.csv')

def update(active_player_dicts):
  """
  Add any unknown player dicts to the list.
  Also updates players teams (if there are trades)!
  :param list[dict[str,str]] active_player_dicts: list of player dicts. each dict has brefid/name/team keys
  :return:
  """
  if os.path.exists(player_list):
    df = pandas.read_csv(player_list, index_col=0)
  else:
    df = pandas.DataFrame(columns=['name', 'team', 'pos'])
  # Figure out which of these tuples are new
  unknown_players = []
  updated_players = []
  for player in active_player_dicts:
    if player['brefid'] in df.index:  # known player -- traded?
      if df.loc[player['brefid']]['team'] != player['team'] or df.loc[player['brefid']]['pos'] != player['pos']:
        updated_players.append(player['brefid'])
        df.loc[player['brefid']]['team'] = player['team']
        df.loc[player['brefid']]['pos'] = player['pos']
    else:
      df.loc[player['brefid']] = [player['name'], player['team'], player['pos']]
      unknown_players.append(player['brefid'])
  if unknown_players:
    logging.info("mapping_df added %d players: %s", len(unknown_players), ','.join(unknown_players))
  if updated_players:
    logging.info("mapping_df updated %d players: %s", len(updated_players), ','.join(updated_players))
  df.to_csv(player_list, index=True, index_label='brefid')
  return df

def load_player_table():
  if os.path.exists(player_list):
    df = pandas.read_csv(player_list)
    return df
  else:
    return pandas.DataFrame(columns=['name', 'team', 'pos'])