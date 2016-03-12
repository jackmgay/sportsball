
from argparse import ArgumentParser
import pandas as pd, numpy as np
from contexttimer import Timer
from dfs.extdata.bsbr.gamelogs import load_gamelogs
from dfs.extdata.numberfire.io import load_nf_histplayerinfo
from dfs.extdata.bsbr.park_effects import get_park_effects
from dfs.extdata.sportsbookreview.sbrio import load_sbr_odds_info
import progressbar
import warnings

nf_info_cache = {}
batting_gamelogs = load_gamelogs('batting')
pitching_gamelogs = load_gamelogs('pitching')
feature_generators = {'pitcher': {}, 'batter': {}}

def featurizer(name=None, columns=None, who='pitcher', live=True):
  """
  Decorator for functions that expand data by creating new feature columns.
  The decorated expansion function should take as input a new game row from BBR and return
    one row for each value in 'columns', in order
  :param name: name of featurizer, prepended to each column name
  :param columns: list of features generated
  :param str who: is this a pitcher or batter featurizer?
  :param bool live: whether to use this featurizer when expanding live data
  :return:
  """
  def decorated(func):
    func_name = name or func.__name__
    if who == 'pitcher' or who == 'both':
      assert func_name not in feature_generators['pitcher']
      feature_generators['pitcher'][func_name] = (func, columns, live)
    if who == 'batter' or who == 'both':
      assert func_name not in feature_generators['batter']
      feature_generators['batter'][func_name] = (func, columns, live)
    return func
  return decorated

def encode_names(feature_name, columns):
  return [feature_name + ': ' + col for col in columns]

# We're already crediting them with bases per hit -- so reduce value of doubles, triples, & home runs.
# Then we give them another 0.25 points per hit to offset the out penalty -- Fanduel charges 0.25 points per
# atbat that does not result in a hit. So this is really "- 0.25 * (row['AB'] - row['HIT'])", just distributed.
batter_scorer = pd.Series({'H':1.25, '2B':1, '3B':2, 'HR':3, 'RBI': 1, 'R': 1, 'BB': 1, 'SB': 2, 'HBP': 1,
                            'AB': -0.25})
pitcher_scorer = pd.Series({'W':4, 'ER':-1, 'SO':1, 'IP':1})

@featurizer(name="BatterTarget", columns=['BatterFDFP'], who='batter', live=False)
def batter_fantasy_points_fzr(row):
  return row[batter_scorer.index].dot(batter_scorer)

@featurizer(name="PitcherTarget", columns=['Wins', 'PitcherFDFP'], who='pitcher', live=False)
def pitcher_fantasy_points_fzr(row):
  def pitcher_stat_is_win(decision):
    if decision and isinstance(decision, basestring):
      return 1 if decision.startswith('W') else 0
    else:
      return decision
  row['W'] = pitcher_stat_is_win(row['Dec'])
  return row['W'], row[pitcher_scorer.index].dot(pitcher_scorer)

@featurizer(name="YTD", columns=['BA', 'OPS', 'SLG'], who='batter')
def batter_ytd_stats(row):
  batter_stats = batting_gamelogs.get(row['player_id'])
  if batter_stats is not None:
    batter_stats = batter_stats[batter_stats.index < row['Date']]
    if len(batter_stats):
      last_stats = batter_stats.iloc[-1]
      return [last_stats['BA'], last_stats['OPS'], last_stats['SLG']]
  return []

distinct_nf_players_expanded = set()

# Generate Numberfire predictions
_nf_batter_columns = ['PA', 'AB', 'H', 'BB', '1B', '2B', '3B', 'HR', 'R', 'RBI', 'SB', 'HBP', 'SO', 'AVG', 'FP']
@featurizer(name="NFBatter", columns=_nf_batter_columns, who='batter')
def batter_nf_stats(row):
  def load_nf_info_cached(player):
    """
    Load numberfire information for the player, cache it, and also add implied columns that Numberfire doesn't
    explicitly calculate - like total hits and total fantasy points.
    :param str player: bref id of player
    :return pd.DataFrame: loaded stats data
    """
    if player in nf_info_cache:
      return nf_info_cache[player]
    else:
      nfdatadict = load_nf_histplayerinfo('mlb', [player])
      player_data = nfdatadict.get(player)
      if player_data is not None:
        if '1B' not in player_data:
          print 'wrong columns present for', player
          warnings.warn("Player is pitcher, not batter -- could be a serious problem.")
          return None
        hits_series = player_data['1B'] + player_data['2B'] + player_data['3B'] + player_data['HR']
        player_data['H'] = hits_series
        fanduel_pts = player_data[batter_scorer.index].dot(batter_scorer)
        player_data['FP'] = fanduel_pts
        nf_info_cache[player] = player_data
      return player_data
  bref_id = row['player_id']
  if not isinstance(bref_id, basestring):
    if isinstance(bref_id, float) and np.isnan(bref_id):  # No MLB ID for this player. We won't load numberfire stats.
      return []
    print 'playerid not basestring'
    import IPython
    IPython.embed()
  nfdata = load_nf_info_cached(bref_id)
  if nfdata is not None and row['Date'] in nfdata.index:
    distinct_nf_players_expanded.add(bref_id)
    loaded_stats = nfdata.loc[row['Date'], _nf_batter_columns].values
    if not isinstance(loaded_stats, np.ndarray) or len(loaded_stats) > 15:
      print 'Something is wrong loading numberfire data for (brefid=%s, date=%s)' % (bref_id, row['Date'])
      import IPython
      IPython.embed()
    loaded_stats = list(loaded_stats)
  else: # No data for this player on this date.
    loaded_stats = []
  return loaded_stats

_nf_pitcher_columns = ['W', 'L', 'IP', 'BF', 'H', 'R', 'ER', 'HR', 'SO', 'BB', 'ERA', 'WHIP', 'FP']
@featurizer(name="NFPitcher", columns=_nf_pitcher_columns, who='pitcher')
def pitcher_nf_stats(row):
  """
  Stitch together (expand) numberfire stats for pitchers for this gamelog.
  :param row: one gamelog row
  :return:
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
      nfdatadict = load_nf_histplayerinfo('mlb', [player])
      player_data = nfdatadict.get(player)
      if player_data is not None:
        if 'ER' not in player_data:
          print 'wrong columns present for', player
          warnings.warn("Player is batter, not pitcher -- could be a serious problem.")
          return None
        fanduel_pts = player_data[pitcher_scorer.index].dot(pitcher_scorer)
        player_data['FP'] = fanduel_pts
        nf_info_cache[player] = player_data
      return player_data
  bref_id = row['player_id']
  nfdata = load_nf_info_cached(bref_id)
  if nfdata is not None and row['Date'] in nfdata.index:
    distinct_nf_players_expanded.add(bref_id)
    loaded_stats = nfdata.loc[row['Date'], _nf_pitcher_columns].values
    if not isinstance(loaded_stats, np.ndarray) or len(loaded_stats) > 13:
      print 'Something is wrong loading numberfire data for (brefid=%s, date=%s)' % (bref_id, row['Date'])
      import IPython
      IPython.embed()
    loaded_stats = list(loaded_stats)
  else: # No data for this player on this date.
    loaded_stats = []
  return loaded_stats

def _get_starting_pitcher(team, date):
  # TODO(jgershen): handle the case here where we are predicting a game in the future!
  # Need to get projected starters!
  def lookup_pitcher(team, date):
    for pitcher, df in pitching_gamelogs.iteritems():
      if date in df.index and df.loc[date, 'Tm'] == team and df.loc[date, 'Entered'].strip().startswith('1'):
        return pitcher
  return lookup_pitcher(team, date)

def _calc_siera(past_games):
  totals = past_games.sum()
  totals['PA'] = totals['BF']
  totals['gb_pa'] = (totals['GB'] - totals['FB'] - totals['PU']) / totals["PA"]
  totals['so_pa'] = totals['SO'] / totals['PA']
  totals['bb_pa'] = totals['BB'] / totals['PA']
  # see http://www.fangraphs.com/blogs/new-siera-part-two-of-five-unlocking-underrated-pitching-skills/
  siera = 5.534 \
          - 15.518 * totals['so_pa']     \
          + 8.648 * totals['bb_pa']     \
          - 2.298 * totals['gb_pa']      \
          + 9.146 * totals['so_pa'] ** 2 \
          - 27.252 * totals['bb_pa'] ** 2 \
          - 4.920 * totals['gb_pa'] ** 2 \
          - 4.036 * totals['so_pa'] * totals['bb_pa'] \
          + 5.155 * totals['so_pa'] * totals['gb_pa'] \
          + 4.546 * totals['bb_pa'] * totals['gb_pa']
  return siera

@featurizer(name="OppPitcherYTD", columns=['ERA', 'FIP', 'SIERA'], who='batter')
def ytd_opp_pitcher(row):
  if 'OppStartingPitcher' not in row:
    # Get opposing pitcher
    opp_team = row['Opp']
    opp_pitcher = _get_starting_pitcher(team=opp_team, date=row['Date'])
  else:
    opp_pitcher = row['OppStartingPitcher']
  if not opp_pitcher or (type(opp_pitcher) is float and np.isnan(opp_pitcher)):
    print 'no starting pitcher for this row', row
    return []
  games = pitching_gamelogs.get(opp_pitcher)
  if games is None:
    return []
  past_games = games[games.index < row['Date']]
  if len(past_games) < 1:
    return []
  latest_game = past_games.loc[past_games.index.max()]
  curr_era = float(latest_game['ERA'])
  fip = 3.20 + (3 * sum(past_games['BB']) - 2 * sum(past_games['SO']) + 13 * sum(past_games['HR'])) / 9.0
  siera = _calc_siera(past_games)
  return [curr_era, fip, siera]

@featurizer(name="PitcherYTD", columns=['ERA', 'FIP', 'SIERA'], who='pitcher')
def ytd_pitcher(row):
  pitcher = row['player_id']
  if pitcher not in pitching_gamelogs:
    return []
  games = pitching_gamelogs[pitcher]
  past_games = games[games.index < row['Date']]
  if len(past_games) < 1:
    return []
  latest_game = past_games.loc[past_games.index.max()]
  try:
    curr_era = float(latest_game['ERA'])
  except ValueError: # You might not actually get a defined ERA all the time
    curr_era = np.nan

  fip = 3.20 + (3 * sum(past_games['BB']) - 2 * sum(past_games['SO']) + 13 * sum(past_games['HR'])) / 9.0
  siera = _calc_siera(past_games)
  return [curr_era, fip, siera]

@featurizer(name="Park", columns=['PPA'], who='pitcher')
def pitcher_adjustment(row):
  stadium = row['Opp'] if row["HomeAway"] == '@' else row['Tm']
  bpa, ppa = get_park_effects(stadium)
  return [ppa]

@featurizer(name="Park", columns=['BPA'], who='batter')
def batter_adjustment(row):
  stadium = row['Opp'] if row["HomeAway"] == '@' else row['Tm']
  try:
    bpa, ppa = get_park_effects(stadium)
  except:
    print 'park effect stadium problem'
    import IPython
    IPython.embed()
  return [bpa]

@featurizer(name='Vegas', columns=['WinProb', 'RunLine'], who='both')
def vegas_odds(row):
  day_odds = load_sbr_odds_info('mlb', row['Date'].date().isoformat())
  try:
    odds = day_odds.loc[row['Tm']]
  except KeyError:
    warnings.warn("No odds found for %s on %s" % (row['Tm'], row['Date'].date().isoformat()))
    # Default to 0.5 for the implied odds of winning the game if not found.
    # Default to 8.5 as the over/under on runs scored in the game
    # Source: http://www.sportingcharts.com/articles/mlb/what-is-the-average-number-of-runs-scored-in-an-mlb-game.aspx
    return [0.5, 8.5]
  # Handle doubleheaders (though not gracefully) by assuming the odds come in order of game time
  # and that the hour index on the game is the order in which the game was played. :/
  if isinstance(odds, pd.DataFrame):
    try:
      odds = odds.iloc[row['Date'].hour]
    except IndexError: # probably because of a known issue with bsbr scrape (games @ 2:00??)
      return []
  retval = [odds['odds'], odds['runline']]
  return retval

def get_expansion_targets(pitcher=False, expanding_live=False):
  for feature_name, (func, columns, live) in feature_generators['pitcher' if pitcher else 'batter'].iteritems():
    if (expanding_live and live) or not expanding_live:
      yield feature_name, (func, columns)

def expand_mlb_data(infile_data, pitcher=False, live=False):
  new_feature_dataframes = [infile_data]
  expanded_columns = []
  for feature_name, (func, columns) in get_expansion_targets(pitcher=pitcher, expanding_live=live):
    with Timer() as t:
      print 'Expanding', feature_name, '(' + ', '.join(columns) + ')...'
      raw_data = [func(row) for index, row in infile_data.iterrows()]
      raw_columns = encode_names(feature_name, columns)
      expanded_columns += raw_columns
      try:
        new_feature_data = pd.DataFrame(raw_data,
                                      index=infile_data.index,
                                      columns=raw_columns)
      except AssertionError as ex:
        print 'Debugging assertion error -- probably no data for some featurizer was loaded. ' \
              'I suspect Numberfire scraping needs to happen!'
        import IPython
        IPython.embed()
      except TypeError as ex:
        print 'Debugging type error'
        import IPython
        IPython.embed()

      new_feature_dataframes.append(new_feature_data)
    print '  took %d seconds' % t.elapsed
  expanded_data = pd.concat(new_feature_dataframes, axis=1)
  # After doing all of that concatenation the index is super weird so just reset it
  expanded_data.reset_index(drop=True, inplace=True)
  # Transform categorical variables to indicator variables -- but only for expanded discrete columns.
  # May need to tweak how this list is generated in the future.
  categorical_cols = [c for c in expanded_columns if expanded_data[c].dtype.name == 'object']
  expanded_discretized = pd.get_dummies(expanded_data, prefix_sep='=', columns=categorical_cols)
  return expanded_discretized

def expand_file_data(infile, outfile, pitcher):
  infile_data = pd.read_pickle(infile)
  outfile_data = expand_mlb_data(infile_data=infile_data, pitcher=pitcher)
  pd.to_pickle(outfile_data, outfile)
  return outfile_data

def expand_cli():
  p = ArgumentParser()
  p.add_argument("infile", default="player_stats.pickle", help="Raw player stats archive.")
  p.add_argument("outfile", default="expanded.pickle", help="Expanded pickle file targets.")
  p.add_argument("--end-date", default=None, help="Max date to include data from.")
  p.add_argument("--pitcher", action='store_true', help="Expand using pitcher features, not batter features")
  cfg = p.parse_args()

  outfile_data = expand_file_data(cfg.infile, cfg.outfile, cfg.pitcher)

  print 'Expansion statistics:'
  print '  Expanded %d rows total.' % len(outfile_data)
  print '  Total features (attrvals) including targets:', len(outfile_data.columns)
  print '  Expanded Numberfire data for', len(distinct_nf_players_expanded), 'players.'