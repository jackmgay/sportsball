#!/usr/bin/env python

from setuptools import setup, find_packages
import platform

install_requires = [
  'requests==2.4.3',
  'beautifulsoup4==4.3.2',
  'py==1.4.29',
  'ipython==2.3.0',
  'PyVirtualDisplay==0.1.5',
  'selenium==2.44.0',
  'PyVirtualDisplay==0.1.5',
  'SQLAlchemy==0.9.8',
  'simplejson==3.6.5',
  'python-dateutil==2.2',
  'pandas==0.16.2',
  'pytz==2014.10',
  'numpy==1.10.1',
  'pyzmq==14.4.1',
  'tornado==4.0.2',
  'Jinja2==2.7.3',
  'matplotlib==1.4.2',
  'scikit-learn==0.17',
  'scipy==0.16.1',
  'fuzzywuzzy==0.4.0',
  'python-Levenshtein==0.12.0',
  'ipdb==0.8',  # for debugging purposes only
  'progressbar==2.3',
  'contexttimer==0.3.1',
  'lxml==3.4.4', # for parsing HTML tables efficiently into DataFrames?
  'ipy-progressbar==1.0.2', # version 1.1.1 is broken!
  'pytest==2.7.2',
  'parsedatetime==1.5',
  'mock==1.3.0'
]

if __name__ == '__main__':
  setup(
    name='sportsball',
    packages=find_packages(exclude=("test",)),
    install_requires=install_requires,
    entry_points={
      'console_scripts': [
        # MLB Player matching
        'mlb-update-mappings = dfs.extdata.crunchtime:download_latest_mapping',
        # MLB Player scraping from baseball-reference.com
        'bsbr-load-overviews = dfs.extdata.bsbr.scrape_bsbr:create_json_file',
        'bsbr-load-player = dfs.extdata.bsbr.scrape_bsbr:cli_load_player',
        'bsbr-update-players = dfs.extdata.bsbr.scrape_bsbr:cli_update_players',
        # MLB Player scraping from other data sources
        'mlb-scrape-nf = dfs.extdata.numberfire.mlb_scraper:scrape_cli',
        'mlb-scrape-sbr = dfs.extdata.sportsbookreview.scrape_mlb_odds:scrape_cli',

        # MLB pipeline
        'pl-mlb-dump = dfs.mlb.dumping:dump_cli',
        'pl-mlb-expand = dfs.mlb.expansion:expand_cli',
        'pl-mlb-split = dfs.mlb.split:split_cli',
        'pl-mlb-dumpattrs = dfs.mlb.attrs:dump_attrs_cli',
        'pl-mlb-model = dfs.mlb.model:build_model_cli',
        'pl-mlb-expand-live = dfs.mlb.predict:expand_live_cli',
        'pl-mlb-predict-live = dfs.mlb.predict:predict_cli',
        'pl-mlb-teamgen = dfs.mlb.buildteams:build_teams_cli',

        # Main MLB scripts
        'pl-mlb = dfs.mlb.pipeline:run_pipeline_cli',
        'pl-mlb-live = dfs.mlb.pipeline:run_live_prediction_pipeline_cli',

        # Load or update active NBA stats from basketball reference
        'bbr-update-players = dfs.extdata.bbr.scraper:cli_update_players',
        # NBA external data scraping
        'nba-scrape-nf = dfs.extdata.numberfire.nba_scraper:scrape_cli',
        'nba-scrape-sbr = dfs.extdata.sportsbookreview.scrape_nba_odds:scrape_cli',
        # NBA pipeline
        'pl-nba-dump = dfs.nba.dumping:dump_cli',
        'pl-nba-expand = dfs.nba.expansion:expand_cli',
        'pl-nba-dumpattrs = dfs.nba.attrs:dump_attrs_cli',
        'pl-nba-split = dfs.nba.split:split_cli',
        'pl-nba-model = dfs.nba.model:build_model_cli',
        'pl-nba-teamgen = dfs.nba.buildteams:build_teams_cli',

        # NBA full pipeline scripts
        'pl-nba = dfs.nba.pipeline:run_pipeline_cli',
        'pl-nba-live = dfs.nba.pipeline:run_live_pipeline_cli',
        # NBA evaluation pipelines / scripts
        'eval-nba-historical = dfs.nba.eval_pipeline:eval_historical_cli',

        # FanDuel connector v2
        'fd-scrape-mlb = dfs.fanduel2.connector:scrape_mlb',
        'fd-scrape-nba = dfs.fanduel2.connector:scrape_nba',

      ]
    },
    package_data = {
      '': ['*.txt'],
      '': ['*.html'],
    }
  )