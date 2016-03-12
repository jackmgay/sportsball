import os
from argparse import ArgumentParser
import logging
import pandas as pd

logger = logging.getLogger(__name__)
logger.addHandler(logging.FileHandler('./pipeline.log'))
logger.setLevel(logging.DEBUG)

from dfs import GLOBAL_ROOT
from dfs.nba.playerid import id2name

import smtplib
from email.mime.text import MIMEText


from .dumping import dump_nba_data
from .expansion import expand_file_data
from .split import strip_and_process_to_files, split_to_files
from .model import build_model, apply_model

from .predict import dump_eligible_players_df
from .buildteams import genteam_to_file

# Make directories
# Dump
# Expand
# Build attr File
# Split
# Build models
# Expand live

pipeline_stages = ['begin', 'dump', 'expand', 'attrs', 'stripna', 'split', 'buildmodel', 'applymodel', 'finish']

attrs = '''Target:FDFP
OpponentLast2Weeks:AvgFPPG
OpponentLast2Weeks:MaxFPPG
OpponentLast2Weeks:FPPMP
OpponentLast2Weeks:AvgFPPMP
OpponentLast2Weeks:MaxFPPMP
NF:Minutes
NF:PTS
NF:REB
NF:AST
NF:STL
NF:BLK
NF:TOV
NF:PF
NF:FP
Last5:PPG
Vegas:Spread
Vegas:OverUnder
'''

global_na_strategy = 'drop'

def run_pipeline(stage, modeldir, start_date, end_date, algorithm='ridge', sample_size=20000):
  data_dir = os.path.join(modeldir, 'data')
  dumped_pickle = os.path.join(data_dir, 'dumped.pickle')
  expanded_pickle = os.path.join(data_dir, 'expanded.pickle')
  attrs_txt = os.path.join(data_dir, 'attrs.txt')
  stripped_pickle = os.path.join(data_dir, 'stripped.pickle')
  train_pickle = os.path.join(data_dir, 'train.pickle')
  test_pickle = os.path.join(data_dir, 'test.pickle')

  model_pickle = os.path.join(data_dir, 'model.pickle')
  predictions_pickle = os.path.join(data_dir, 'predictions.pickle')

  if stage == 'begin':
    if not os.path.exists(modeldir):
      os.mkdir(modeldir)
    if not os.path.exists(data_dir):
      os.mkdir(data_dir)
    stage = 'dump'
  if stage == 'dump':
    logger.info("Pipeline Stage: %s", stage)
    dump_nba_data(dumped_pickle, start_date=start_date, end_date=end_date, max_count=sample_size)
    stage = 'expand'
  if stage == 'expand':
    logger.info("Pipeline Stage: %s", stage)
    expand_file_data(dumped_pickle, expanded_pickle, live=False)
    stage = 'attrs'
  if stage == 'attrs':
    logger.info("Pipeline Stage: %s", stage)
    with open(attrs_txt, 'w') as outf:
      outf.write(attrs)
    stage = 'stripna'
  if stage == 'stripna':
    logger.info("Pipeline Stage: %s", stage)
    strip_and_process_to_files(expanded_file=expanded_pickle,
                               stripped_file=stripped_pickle,
                               attrfile=attrs_txt,
                               na_strategy=global_na_strategy,
                               include_target=True)
    stage = 'split'
  if stage == 'split':
    logger.info("Pipeline Stage: %s", stage)
    split_to_files(trainfile=train_pickle,
                   testfile=test_pickle,
                   stripped=stripped_pickle,
                   trainpct=90,
                   split_randomly=False)
    stage = 'buildmodel'
  if stage == 'buildmodel':
    logger.info("Pipeline Stage: %s", stage)
    build_model(train_file=train_pickle,
                attr_file=attrs_txt,
                model_out=model_pickle,
                algorithm=algorithm)
    stage = 'applymodel'
  if stage == 'applymodel':
    logger.info("Pipeline Stage: %s", stage)
    apply_model(model_file=model_pickle,
                test_file=test_pickle,
                attr_file=attrs_txt,
                predictions_out=predictions_pickle)
    stage = 'finish'
  assert stage == 'finish'

def run_pipeline_cli():
  p = ArgumentParser()
  p.add_argument("modeldir", help="Root model directory")
  p.add_argument("--stage", choices=pipeline_stages, help="Start / resume from this stage", default='begin')
  p.add_argument("--start-date", default=None, help="Don't include data before this date.")
  p.add_argument("--end-date", default=None, help="Don't include data after this date.")
  p.add_argument("--algorithm", default='ridge', help="Type of model to use.")
  p.add_argument("--max-count", default=20000, help="Max records to include.")
  cfg = p.parse_args()
  logging.basicConfig(level=logging.DEBUG)
  logger.propagate = True
  run_pipeline(stage=cfg.stage,
               modeldir=cfg.modeldir,
               start_date=cfg.start_date,
               end_date=cfg.end_date,
               algorithm=cfg.algorithm,
               sample_size=cfg.max_count)

live_postmodel_stages = ['start', 'dumplive', 'expandlive', 'stripna', 'predict', 'teamgen', 'finish']

def run_live_postmodel_pipeline(stage, modeldir, game_day, gameday_historical=True, send_email=False):
  """
  Use a model we have already constructed to predict player scores (and select teams) for a specific gameday.
  :return:
  """
  # Load these files from the already-completed modeling pipeline
  model_pickle = os.path.join(modeldir, 'data', 'model.pickle')
  attrs_txt = os.path.join(modeldir, 'data', 'attrs.txt')

  # Get the place where we should put the output of live predictions
  data_dir = os.path.join(modeldir, game_day + "_live")
  dumped_pickle = os.path.join(data_dir, 'live.dumped.pickle')
  expanded_pickle = os.path.join(data_dir, 'live.expanded.pickle')
  live_pickle = os.path.join(data_dir, 'live.stripped.pickle')
  predictions_pickle = os.path.join(data_dir, 'live.predictions.pickle')
  teams_pickle = os.path.join(data_dir, 'teams.pickle')

  logger.info("Running live postmodel pipeline for %s", game_day)
  logger.info("  using model in %s", modeldir)
  if stage == 'begin':
    if not os.path.exists(data_dir):
      os.mkdir(data_dir)
    stage = 'dumplive'
  if stage == 'dumplive':
    logger.info("Pipeline Stage: %s", stage)
    dump_eligible_players_df(outfile=dumped_pickle, game_day=game_day, guess_historical=gameday_historical)
    stage = 'expandlive'
  if stage == 'expandlive':
    logger.info("Pipeline Stage: %s", stage)
    expand_file_data(infile=dumped_pickle, outfile=expanded_pickle, live=True)
    stage = 'stripna'
  if stage == 'stripna':
    logger.info("Pipeline Stage: %s", stage)
    strip_and_process_to_files(expanded_file=expanded_pickle,
                               stripped_file=live_pickle,
                               attrfile=attrs_txt,
                               na_strategy=global_na_strategy,
                               include_target=False)
    stage = 'predict'
  if stage == 'predict':
    logger.info("Pipeline Stage: %s", stage)
    apply_model(model_file=model_pickle,
                test_file=live_pickle,
                attr_file=attrs_txt,
                predictions_out=predictions_pickle,
                live=True)
    stage = 'teamgen'
  if stage == 'teamgen':
    logger.info("Pipeline Stage: %s", stage)
    genteam_to_file(outfile=teams_pickle,
                    datafile=predictions_pickle,
                    salary_col='salary',
                    position_col='pos',
                    prediction_col='predicted',
                    cap=60000)
    if not gameday_historical:
      teams = pd.read_pickle(teams_pickle)
      print 'Best team:'
      print teams[['name', 'Tm', 'pos', 'salary', 'predicted', 'Last5:PPG']]
      print 'Predicted total: %.1f' % teams['predicted'].sum()
    if send_email:
      email_predictions(game_day)
    stage = 'finish'
  assert stage == 'finish'

# Slurp up the generated team and send me the selections via email
# Obviously you wouldn't send email like this in a prod environment, but decent enough for a sisde project.
def email_predictions(current_day_str):
  data_dir = os.path.join(GLOBAL_ROOT, current_day_str, current_day_str + '_live')
  teams_pickle = os.path.join(data_dir, 'teams.pickle')
  teams = pd.read_pickle(teams_pickle)

  predicted_total_str = '%.1f' % teams['predicted'].sum()
  email_body = '\n'.join([
    'Best team:<br><br>',
    '<font face="Courier New, Courier, monospace">',
    str(teams[['name', 'Tm', 'pos', 'salary', 'predicted', 'Last5:PPG']].to_html()),
    '</font>',
    '<br><br>',
    'Predicted total: <b>%s</b>' % predicted_total_str
  ])

  email_cred_file = os.path.join(GLOBAL_ROOT, 'email_credential_file')
  if not os.path.exists(email_body):
    raise Exception("You'll need an email account with credentials to... send email... to do this")
  # Make your email cred file two lines: first line has username, second has password.
  user, passwd = map(lambda x: x.strip(), open(email_cred_file, 'r').readlines())
  email_subject = 'Sportsball {today} ({predicted})'.format(today=current_day_str, predicted=predicted_total_str)
  target = '' # your email address goes here
  msg = MIMEText('<html>' + email_body + '</html>',
                 'html')

  msg['Subject'] = email_subject
  msg['From'] = user
  msg['To'] = target

  # Send the mail. This works for GMail, but you may want something else here.
  s = smtplib.SMTP('smtp.gmail.com:587') # Or whatever, if you don't have gmail
  s.starttls()
  s.login(user, passwd)
  s.sendmail(user, [target], msg.as_string())
  s.quit()



def run_live_pipeline_cli():
  p = ArgumentParser()
  p.add_argument("modeldir", help="Root model directory (for already build model)")
  p.add_argument("gameday", help="Day to score")
  p.add_argument("--stage", choices=live_postmodel_stages, help="Start / resume from this stage", default='begin')
  p.add_argument("--historical", action='store_true', help="Run historical sim (not scoring live).")
  p.add_argument("--send-email", action='store_true', help="email me the results.")
  cfg = p.parse_args()
  logging.basicConfig(level=logging.DEBUG)
  logger.propagate = True
  run_live_postmodel_pipeline(stage=cfg.stage,
                              modeldir=cfg.modeldir,
                              game_day=cfg.gameday,
                              gameday_historical=cfg.historical,
                              send_email=cfg.send_email)