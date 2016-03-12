
from argparse import ArgumentParser
import pandas
import progressbar
import random

from dfs.mlb.playerid import brefid_is_starting_pitcher
from dfs.extdata.bsbr.gamelogs import load_gamelogs

def dump_mlb_data(outfile, start_date=None, end_date=None, max_count=None, use_random=False, datatype='batting'):
  """
  Dump MLB statistical data to a file.
  :param str outfile: name of file to become pickled pandas datafile
  :param str start_date: don't include games from before this date when dumping data
  :param str end_date: don't include games from after this date when dumping data
  :param int max_count: maximum # of rows to dump
  :param bool use_random: whether to select rows at random (if False, choose most recent)
  :return:
  """
  print 'Dump MLB data for', datatype
  print 'loading data...'
  all_bsbr_logs = load_gamelogs(datatype=datatype)
  unindexed_dfs = []
  print 'reindexing data...'
  pbar = progressbar.ProgressBar(widgets=[progressbar.Percentage(), ' ', progressbar.Bar(), ' ', progressbar.ETA()])
  for player_id, dataframe in pbar(all_bsbr_logs.items()):
    uidf = dataframe.reset_index()
    # Add player ID as a column to the dataframe for future joining purposes!
    uidf['player_id'] = pandas.Series(data=player_id, index=uidf.index)
    unindexed_dfs.append(uidf)
  all_game_rows = pandas.concat(unindexed_dfs, ignore_index=True)

  # Filter by date
  if start_date is not None:
    all_game_rows = all_game_rows[all_game_rows['Date'] > start_date]
  if end_date is not None:
    all_game_rows = all_game_rows[all_game_rows['Date'] < end_date]

  # Don't use relief pitchers in our dataset
  if datatype == 'pitching':
    print 'restricting to starting pitchers only...'
    all_game_rows = all_game_rows[all_game_rows['player_id'].apply(brefid_is_starting_pitcher)]

  # Sample filtered data
  if max_count and max_count < len(all_game_rows):
    print 'sampling %d rows...' % max_count
    if use_random:
      kept_indices = random.sample(all_game_rows.index, max_count)
      selected = all_game_rows.iloc[kept_indices]
    else:
      all_game_rows.sort("Date")
      selected = all_game_rows.tail(max_count)
  else:
    selected = all_game_rows
  print 'saving...'
  pandas.to_pickle(selected, outfile)
  print 'Done!'
  return selected

def dump_cli():
  p = ArgumentParser()
  p.add_argument("outfile", default="player_stats.pickle", help="Pickle file to dump data to.")
  p.add_argument("--start-date", default=None, help="Don't include data before this date.")
  p.add_argument("--end-date", default=None, help="Don't include data after this date.")
  p.add_argument("--max-count", type=int, default=None, help="Max # of rows to dump.")
  p.add_argument("--use-random", action='store_true', help="If sampling, sample at random instead of by time.")
  p.add_argument("--pitchers", action='store_true', help="Dump stats for pitchers instead of batters.")

  cfg = p.parse_args()

  dump_mlb_data(outfile=cfg.outfile, start_date=cfg.start_date, end_date=cfg.end_date, max_count=cfg.max_count,
                use_random=cfg.use_random, datatype='pitching' if cfg.pitchers else 'batting')