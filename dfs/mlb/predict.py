"""
Predict values (using a pre-built model) for a game which has not yet taken place
"""
from argparse import ArgumentParser
from dfs.extdata.rotoguru import rgio
from dfs.extdata.nbc.starting_pitcher import get_nbc_starting_pitchers
from dfs.mlb.expansion import expand_mlb_data
from dateutil import parser
import datetime
from .playerid import mapping_df, name2brefid, name2brefid_confidence
from dfs.fanduel2.io import read_day_mlb_csv, read_lineup_csv, read_mlb_player_csv, get_available_game_ids
from .attrs import read_attrs
import pandas as pd, numpy as np
import pickle
import IPython

fdteam2mlbteam = {
  'ARI': 'ARI',
  'ATL': 'ATL',
  'BAL': 'BAL',
  'BOS': 'BOS',
  'CHC': 'CHC',
  'CIN': 'CIN',
  'CLE': 'CLE',
  'COL': 'COL',
  'CWS': 'CWS',
  'DET': 'DET',
  'HOU': 'HOU',
  'KAN': 'KC',
  'KC': 'KC',
  'LAA': 'LAA',
  'LOS': 'LAD',
  'LAD': 'LAD',
  'MIA': 'MIA',
  'MIL': 'MIL',
  'MIN': 'MIN',
  'NYM': 'NYM',
  'NYY': 'NYY',
  'OAK': 'OAK',
  'PHI': 'PHI',
  'PIT': 'PIT',
  'SEA': 'SEA',
  'SDP': 'SD',
  'SD': 'SD',
  'SFG': 'SF',
  'STL': 'STL',
  'TAM': 'TB',
  'TB': 'TB',
  'TEX': 'TEX',
  'TOR': 'TOR',
  'WAS': 'WSH'}

def estimate_starting_pitchers(game_day, fd_data):
  sps = {}
  teams = set(fd_data['Tm'].values)
  nbc_starts = get_nbc_starting_pitchers(game_day)
  for team in teams:
    if team in nbc_starts.index:
      sps[team] = nbc_starts.loc[team, 'starter_bref_id']
  fd_sps = fd_data[fd_data['Probable Pitcher'] == 'Yes'].copy()
  fd_sps['spid'] = fd_sps.apply(lambda row: name2brefid(row['First Name'] + ' ' + row['Last Name'], player_team=row['Tm']),
                                axis=1)
  fd_sp_dict = dict(zip(fd_sps['Tm'], fd_sps['spid']))
  for team in teams:
    if sps.get(team) != fd_sp_dict.get(team):
      print 'Team: %s || NBC SP: %s || FD SP: %s' % (team, sps.get(team), fd_sp_dict.get(team))
      if team in fd_sp_dict:
        sps[team] = fd_sp_dict[team]
        print '  (using FD pitcher)'
    else:
      print 'Agreed: %s -> %s' % (team, sps[team])
  if len(sps) < len(teams) or any(not isinstance(x, basestring) for x in sps.itervalues()):
    print "Team starting pitcher prediction didn't work!"
    IPython.embed()
  return sps

def dump_and_expand_live(game_day, batter_target, pitcher_target, game_id=None):
  """
  Expand data for players scraped from FanDuel, and save to file
  :param datetime.datetime game_day: game day to prep FD data for
  :param str batter_target: filename
  :param str pitcher_target: filename
  :param int game_id: FD game ID to build model for
  :param LineupInfo lineup_info: parsed players
  :return:
  """
  try:
    if not game_id:
      available_ids = get_available_game_ids(game_day)
      if not available_ids:
        print 'No IDs known for %s, trying without strict start time involved...'
        available_ids = get_available_game_ids(game_day, strict_time=False)
      if not available_ids:
        print "No FanDuel game IDs available for %s! Maybe try rescraping..." % game_day.isoformat()
      if len(available_ids) > 1:
        print "Multiple game id's avilable, please specify one of these using the game-id flag:"
        print game_id
        raise ValueError("Pick one game ID!")
      game_id = available_ids.pop()
    print 'FanDuel game target: %d on %s' % (game_id, game_day)
    fd_data = read_mlb_player_csv(game_day, game_id)
    lineup_info = read_lineup_csv(game_day, game_id)
    print 'Loaded %d player salaries for this game.' % len(fd_data)
    print '  %d are confirmed in lineups.' % len(lineup_info.parsed_players)
  except TypeError:
    print 'Error loading FanDuel data!'
    raise
  # Make FanDuel data look like BREF data
  fd_data['fullname'] = fd_data.apply(lambda row: row['First Name'] + ' ' + row['Last Name'], axis=1)
  print 'Coercing FanDuel player list data to look like BREF training data...'
  print '  Tweaking team / opp / home / away columns...'
  fd_data['Tm'] = fd_data['Team'].apply(lambda fdteam: fdteam2mlbteam[fdteam])
  fd_data['Opp'] = fd_data['Opponent']
  fd_data['HomeAway'] = fd_data.apply(lambda row: '@' if row['Game'].startswith(row['Team']) else None, axis=1)
  print '  Inferring bref IDs...'
  fd_data['player_id'] = fd_data.apply(lambda row: name2brefid(row['fullname'], row['Tm']), axis=1)
  fd_data['player_id_confidence'] = fd_data.apply(lambda row: name2brefid_confidence(row['fullname'], row['Tm']), axis=1)
  # We expand using the day of the game... so we have to transform the datetime the DFS game starts at
  # into the day that the baseball games are played. If we start using multi-day matchups, we'll need to
  # adjust this code as well.
  fd_data['Date'] = pd.Series(data=datetime.datetime(year=game_day.year, month=game_day.month, day=game_day.day),
                              index=fd_data.index)
  print '  Dropping duplicate less-confident matches...'
  less_dupes = fd_data.sort('player_id_confidence', ascending=False).drop_duplicates(subset='player_id')
  print '  ...dropping %d duplicate bref ids!' % (len(fd_data) - len(less_dupes))
  fd_data = less_dupes

  print 'Restricting to the %d players confirmed to be in the lineup.' % len(lineup_info.parsed_players)
  fd_data = fd_data.loc[fd_data['fullname'].isin(lineup_info.parsed_players)].copy()

  # TODO: use the fanduel "Probable Pitcher" thing for opp pitchers here
  print '  Loading starting pitchers...'
  sps = estimate_starting_pitchers(game_day, fd_data)
  fd_data['OppStartingPitcher'] = fd_data.apply(lambda row: sps[row['Tm']], axis=1)
  pitchers = fd_data.loc[fd_data['Position'] == 'P']
  batters = fd_data.loc[fd_data['Position'] != 'P']
  # Restrict pitchers to only starters
  pitchers = pitchers.loc[pitchers["Probable Pitcher"] == 'Yes']
  # Expand preprocessed FanDuel Data
  expanded_batters = expand_mlb_data(batters, pitcher=False, live=True)
  expanded_pitchers = expand_mlb_data(pitchers, pitcher=True, live=True)
  expanded_batters.to_pickle(batter_target)
  expanded_pitchers.to_pickle(pitcher_target)

def predict_live(batterattrfile, battermodelfile, batterdatafile,
                 pitcherattrfile, pitchermodelfile, pitcherdatafile,
                 predictionfile,
                 na_treatment='zero'):
  # Apply model, save results
  batterattrs = read_attrs(batterattrfile)[1:]
  batter_model = pickle.load(open(battermodelfile, 'r'))
  batter_data = pd.read_pickle(batterdatafile)

  pitcherattrs = read_attrs(pitcherattrfile)[1:]
  pitcher_model = pickle.load(open(pitchermodelfile, 'r'))
  pitcher_data = pd.read_pickle(pitcherdatafile)

  if na_treatment == 'zero':
    usable_batter_data = batter_data[batterattrs].fillna(0)
    usable_pitcher_data = pitcher_data[pitcherattrs].fillna(0)
  elif na_treatment == 'drop':
    usable_batter_data = batter_data[batterattrs].dropna()
    usable_pitcher_data = pitcher_data[pitcherattrs].dropna()

  batter_data['prediction'] = pd.Series(batter_model.predict(usable_batter_data), index=usable_batter_data.index)
  pitcher_data['prediction'] = pd.Series(pitcher_model.predict(usable_pitcher_data), index=usable_pitcher_data.index)

  keep_cols = ['fullname', 'player_id', 'Position', 'Team', 'Salary', 'prediction']

  batter_output = batter_data[keep_cols]
  pitcher_output = pitcher_data[keep_cols]

  pd.concat([batter_output, pitcher_output]).to_pickle(predictionfile)


def expand_live_cli():
  p = ArgumentParser()
  p.add_argument("gameday", help="day to generate predictions for")
  p.add_argument("batterfilename", help="file name to save expanded batter data to")
  p.add_argument("pitcherfilename", help="file name to save expanded pitcher data to")
  p.add_argument("--game-id", type=int, default=None, help="FanDuel game ID to generate predictions for")
  cfg = p.parse_args()
  dump_and_expand_live(game_day=parser.parse(cfg.gameday),
                       batter_target=cfg.batterfilename,
                       pitcher_target=cfg.pitcherfilename,
                       game_id=cfg.game_id)

def predict_cli():
  p = ArgumentParser()
  p.add_argument("batterattrfile", help="attributes to feed into the batter model")
  p.add_argument("battermodelfile", help="serialized model to use for batter predictions")
  p.add_argument("batterdatafile", help="file name to load expanded batter data from")
  p.add_argument("pitcherattrfile", help="attributes to feed into the pitcher model")
  p.add_argument("pitchermodelfile", help="serialized model to use for pitcher predictions")
  p.add_argument("pitcherdatafile", help="file name to load expanded pitcher data from")
  p.add_argument("predictionfile", help="file name to save predictions to")
  p.add_argument("--na-treatment", default='zero')
  cfg = p.parse_args()
  predict_live(cfg.batterattrfile, cfg.battermodelfile, cfg.batterdatafile,
               cfg.pitcherattrfile, cfg.pitchermodelfile, cfg.pitcherdatafile,
               cfg.predictionfile, cfg.na_treatment)