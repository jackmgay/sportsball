import os
from argparse import ArgumentParser
import pandas as pd
from dateutil import parser
from datetime import timedelta
import datetime

from dfs.extdata.bbr.gamelogs import load_gamelogs
from dfs.nba.featurizers import fantasy_points_fzr

from dfs.nba.pipeline import run_pipeline, run_live_postmodel_pipeline

gamedata = load_gamelogs()

import IPython

def eval_historical_teamgen(game_day, team_file):
  # how well did that team actually do
  team = pd.read_pickle(team_file)
  team['date'] = pd.Series(game_day, index=team.index)
  team['actual'] = team.apply(lambda row: fantasy_points_fzr(gamedata[row.name].loc[game_day]), axis=1)
  print team[['pos', 'salary', 'predicted', 'actual']]
  print team['predicted'].sum(), team['actual'].sum()
  return team['predicted'].sum(), team['actual'].sum()
  #IPython.embed()

def eval_historical_cli():
  p = ArgumentParser()
  p.add_argument("gameday", help="date this actually took place")
  p.add_argument("teamfile", help="pickled dataframe containing information for selected team")
  cfg = p.parse_args()

  eval_historical_teamgen(cfg.gameday, cfg.teamfile)

def run_everything_day(root_dir, day):
  modeldir = os.path.join(root_dir, day)
  dt = parser.parse(day)
  run_pipeline(stage='begin',
               modeldir=modeldir,
               start_date='2014-11-01',
               end_date=str(dt.date()), # ending on the actual date should be okay i think, because of midnight stuff
               algorithm='ridge'
               )
  run_live_postmodel_pipeline(stage='begin', modeldir=modeldir, game_day=day, gameday_historical=True)
  pred, actual = eval_historical_teamgen(game_day=day,
                                         team_file=os.path.join(modeldir, day + '_live', 'teams.pickle'))
  return day, pred, actual

def run_all_days(root_dir):
  start_day = datetime.datetime(2015, 10, 29)
  days = [start_day + datetime.timedelta(days=n) for n in range(27)]
  results = []
  for day in days:
    print 'Predicting result for', str(day.date())
    results.append(run_everything_day(root_dir, str(day.date())))
  with open(os.path.join(root_dir, 'results.txt'), 'w') as outf:
    for day, pred, actual in results:
      outf.write('%s,%.1f,%.1f\n' % (day,pred,actual))
      print '%10s %.1f %.1f' % (day, pred, actual)



p = ArgumentParser()
p.add_argument("rootdir", help="root directory")
cfg = p.parse_args()

run_all_days(root_dir=cfg.rootdir)