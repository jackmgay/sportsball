import datetime
from dateutil import parser
import os
from argparse import ArgumentParser

from .dumping import dump_mlb_data
from .expansion import expand_file_data
from .split import split_data
from .model import build_model

from .predict import predict_live, dump_and_expand_live
from .buildteams import build_teams

# Make directories
# Dump
# Expand
# Build attr File
# Split
# Build models
# Expand live

pipeline_stages = ['begin', 'dump', 'expand', 'attrs', 'split', 'model', 'finish']

batter_attrs = '''BatterTarget: BatterFDFP
Park: BPA
OppPitcherYTD: SIERA
Vegas: WinProb
Vegas: RunLine
NFBatter: PA
NFBatter: FP
YTD: OPS
'''

pitcher_attrs = '''PitcherTarget: PitcherFDFP
PitcherYTD: SIERA
Vegas: WinProb
Vegas: RunLine
Park: PPA
NFPitcher: FP
'''

global_na_strategy = 'drop'

def run_pipeline(stage, modeldir, start_date, end_date, algorithm='ridge'):
  batter_dir = os.path.join(modeldir, 'batting')
  pitcher_dir = os.path.join(modeldir, 'pitching')

  if stage == 'begin':
    if not os.path.exists(modeldir):
      os.mkdir(modeldir)
    if not os.path.exists(batter_dir):
      os.mkdir(batter_dir)
    if not os.path.exists(pitcher_dir):
      os.mkdir(pitcher_dir)
    stage = 'dump'
  if stage == 'dump':
    dump_mlb_data(os.path.join(batter_dir, 'dumped.pickle'), start_date=start_date, end_date=end_date,
                  datatype='batting')
    dump_mlb_data(os.path.join(pitcher_dir, 'dumped.pickle'), start_date=start_date, end_date=end_date,
                  datatype='pitching')
    stage = 'expand'
  if stage == 'expand':
    expand_file_data(os.path.join(batter_dir, 'dumped.pickle'),
                     os.path.join(batter_dir, 'expanded.pickle'),
                     pitcher=False)
    expand_file_data(os.path.join(pitcher_dir, 'dumped.pickle'),
                     os.path.join(pitcher_dir, 'expanded.pickle'),
                     pitcher=True)
    stage = 'attrs'
  if stage == 'attrs':
    with open(os.path.join(batter_dir, 'attrs.txt'), 'w') as outf:
      outf.write(batter_attrs)
    with open(os.path.join(pitcher_dir, 'attrs.txt'), 'w') as outf:
      outf.write(pitcher_attrs)
    stage = 'split'
  if stage == 'split':
    split_data(infile=os.path.join(batter_dir, 'expanded.pickle'),
               train=os.path.join(batter_dir, 'train.pickle'),
               test=os.path.join(batter_dir, 'test.pickle'),
               attrfile=os.path.join(batter_dir, 'attrs.txt'),
               na_strategy=global_na_strategy, trainpct=70, split_randomly=False)
    split_data(infile=os.path.join(pitcher_dir, 'expanded.pickle'),
               train=os.path.join(pitcher_dir, 'train.pickle'),
               test=os.path.join(pitcher_dir, 'test.pickle'),
               attrfile=os.path.join(pitcher_dir, 'attrs.txt'),
               na_strategy=global_na_strategy, trainpct=70, split_randomly=False)
    stage = 'model'
  if stage == 'model':
    build_model(train_file=os.path.join(batter_dir, 'train.pickle'),
                test_file=os.path.join(batter_dir, 'test.pickle'),
                attr_file=os.path.join(batter_dir, 'attrs.txt'),
                model_out=os.path.join(batter_dir, 'model.pickle'),
                predictions_out=os.path.join(batter_dir, 'predictions.pickle'),
                algorithm=algorithm)
    build_model(train_file=os.path.join(pitcher_dir, 'train.pickle'),
                test_file=os.path.join(pitcher_dir, 'test.pickle'),
                attr_file=os.path.join(pitcher_dir, 'attrs.txt'),
                model_out=os.path.join(pitcher_dir, 'model.pickle'),
                predictions_out=os.path.join(pitcher_dir, 'predictions.pickle'),
                algorithm=algorithm)
    stage = 'finish'
  assert stage == 'finish'

def run_pipeline_cli():
  p = ArgumentParser()
  p.add_argument("modeldir", help="Root model directory")
  p.add_argument("--stage", choices=pipeline_stages, help="Start / resume from this stage", default='begin')
  p.add_argument("--start-date", default=None, help="Don't include data before this date.")
  p.add_argument("--end-date", default=None, help="Don't include data after this date.")
  p.add_argument("--algorithm", default='ridge', help="Type of model to use.")
  cfg = p.parse_args()
  print 'Running pipeline from stage ', cfg.stage
  run_pipeline(stage=cfg.stage,
               modeldir=cfg.modeldir,
               start_date=cfg.start_date,
               end_date=cfg.end_date,
               algorithm=cfg.algorithm)

live_stages = ['begin', 'dump', 'project', 'teamgen', 'finish']

def run_live_prediction_pipeline(modeldir, gamedate, game_id=None, live_stage='begin'):
  day_dir = os.path.join(modeldir, 'predict' + gamedate.isoformat())
  batter_data_file = os.path.join(day_dir, 'livebatterdata.pickle')
  pitcher_data_file = os.path.join(day_dir, 'livepitcherdata.pickle')
  batter_dir = os.path.join(modeldir, 'batting')
  pitcher_dir = os.path.join(modeldir, 'pitching')
  projection_file = os.path.join(day_dir, 'projections.pickle')

  if live_stage == 'begin':
    if not os.path.exists(day_dir):
      os.mkdir(day_dir)
    live_stage = 'dump'
  if live_stage == 'dump':
    print 'Doing live data dump & expansion for', gamedate.isoformat(), '...'
    dump_and_expand_live(game_day=gamedate,
                         batter_target=batter_data_file,
                         pitcher_target=pitcher_data_file,
                         game_id=game_id)
    live_stage = 'project'
  if live_stage == 'project':
    print 'Generating player predictions...'
    predict_live(batterattrfile=os.path.join(batter_dir, 'attrs.txt'),
                 battermodelfile=os.path.join(batter_dir, 'model.pickle'),
                 batterdatafile=batter_data_file,
                 pitcherattrfile=os.path.join(pitcher_dir, 'attrs.txt'),
                 pitchermodelfile=os.path.join(pitcher_dir, 'model.pickle'),
                 pitcherdatafile=pitcher_data_file,
                 predictionfile=projection_file,
                 na_treatment=global_na_strategy)
    live_stage = 'teamgen'
  if live_stage == 'teamgen':
    print 'Generating teams...'
    build_teams(datafile=projection_file,
                salary_col='Salary',
                position_col='Position',
                prediction_col='prediction',
                num_pitchers=3,
                cap=35000)
    live_stage = 'finish'
  assert live_stage == 'finish'

def run_live_prediction_pipeline_cli():
  p = ArgumentParser()
  p.add_argument("modeldir", help="Root model directory")
  p.add_argument("--game-date", default=None, help="Date to generate predictions for.")
  p.add_argument("--game-id", type=int, default=None, help="FanDuel game ID to generate predictions for")
  p.add_argument("--stage", choices=live_stages, help="Start / resume from this stage", default='begin')

  cfg = p.parse_args()
  gamedate = parser.parse(cfg.game_date) if cfg.game_date else datetime.date.today()
  print 'Running pipeline [from:%s] for %s from the model at %s...' % \
        (cfg.stage, gamedate.isoformat(), cfg.modeldir)
  run_live_prediction_pipeline(cfg.modeldir,
                               gamedate,
                               game_id=cfg.game_id,
                               live_stage=cfg.stage)