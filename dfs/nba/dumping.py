
from argparse import ArgumentParser
import pandas
import progressbar
import random
import datetime
from dateutil import parser

import IPython

from dfs.extdata.bbr.gamelogs import load_all_game_data

def dump_nba_data(outfile, start_date=None, end_date=None, max_count=None, use_random=False):
  """
  Dump NBA statistical data to a file.
  :param str outfile: name of file to become pickled pandas datafile
  :param str start_date: don't include games from before this date when dumping data
  :param str end_date: don't include games from after this date when dumping data
  :param int max_count: maximum # of rows to dump
  :param bool use_random: whether to select rows at random (if False, choose most recent)
  :return:
  """
  if start_date:
    start_date = parser.parse(start_date)
  else:
    start_date = datetime.datetime(2010, 10, 1)
  if end_date:
    end_date = parser.parse(end_date)
  else:
    end_date = datetime.datetime.today()
  print 'Dump NBA data for %s to %s' % (start_date, end_date)
  print 'loading data...'
  all_game_rows = load_all_game_data()

  # Filter by date
  if start_date is not None:
    all_game_rows = all_game_rows[all_game_rows['date'] > start_date]
  if end_date is not None:
    all_game_rows = all_game_rows[all_game_rows['date'] < end_date]

  # Sample filtered data
  if max_count and max_count < len(all_game_rows):
    print 'sampling %d rows...' % max_count
    if use_random:
      # We seed to 0 when we call this from CLI to make sure that random splits are replicable.
      random.seed(0)
      kept_indices = random.sample(all_game_rows.index, max_count)
      selected = all_game_rows.loc[kept_indices]
    else:
      all_game_rows.sort("date")
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

  cfg = p.parse_args()

  dump_nba_data(outfile=cfg.outfile,
                start_date=cfg.start_date,
                end_date=cfg.end_date,
                max_count=cfg.max_count,
                use_random=cfg.use_random)