import pandas as pd
import itertools
import datetime
from dateutil import parser
import progressbar
import logging
from argparse import ArgumentParser
import numpy as np

from dfs.extdata.common.scraper import getSoupFromURL
from .sbrio import save_sbr_odds_info, load_sbr_odds_info
from .common import moneyline_to_implied_odds
from dfs.nba.playerid import team_tla

import IPython, ipdb

logger = logging.getLogger(__name__)
logger.addHandler(logging.FileHandler("logs/scrapeSBR.log"))
logger.setLevel(logging.DEBUG)
nba_template = 'http://www.sportsbookreview.com/nba-basketball/odds-scores/{year}{month}{day}/'

def _get_scoreline_components(team_row):
  # Parse the scoreline overunder & odds component
  raw_ml = team_row.find('td', {'class': 'tbl-odds-c7'}).text
  raw_ml = raw_ml.replace(u'\xbd', '.5') # 1/2 character
  if 'N/A' in raw_ml:
    return [np.nan, np.nan]
  else:
    return map(float, raw_ml.split())

def _get_spread_components(team_row):
  # Parse the spread and vig.
  # this might be exactly the same as _get_scoreline_components, don't tell anyone
  raw_ml = team_row.find('td', {'class': 'tbl-odds-c5'}).text
  raw_ml = raw_ml.replace(u'\xbd', '.5') # 1/2 character
  if 'N/A' in raw_ml:
    return [np.nan, np.nan]
  else:
    return map(float, raw_ml.split())

def parse_odds_table(odds_table):
  rows = odds_table.findAll('tr')
  team1 = rows[2]  # also contains over bet
  team2 = rows[3]  # also contains under bet
  t1name = team1.find('td', {'class': 'tbl-odds-c2'}).text
  t2name = team2.find('td', {'class': 'tbl-odds-c2'}).text
  t1_current_spread = _get_spread_components(team1)
  t2_current_spread = _get_spread_components(team2)
  # Get scoreline over / under if present
  over_l = _get_scoreline_components(team1)
  under_l = _get_scoreline_components(team2)[1:]
  # Map sportsbook team names to three-letter abbreviations
  t1tla, conf1 = team_tla(t1name, get_confidence=True)
  t2tla, conf2 = team_tla(t2name, get_confidence=True)
  if conf1 >= 90 and conf2 >= 90:
    # Create a row for each team with
    # team, moneyline, run over/under, overML, underML
    return [[t1tla] + t1_current_spread + over_l + under_l,
            [t2tla] + t2_current_spread + over_l + under_l]
  else:
    logger.warning("Skipping odds for game: %s vs %s", t1name, t2name)
    return None

def load_odds_for_day(game_day):
  day_part = '%02d' % game_day.day
  month_part = '%02d' % game_day.month
  url = nba_template.format(year=game_day.year, month=month_part, day=day_part)
  soup = getSoupFromURL(url)
  odds_tables = soup.findAll('table', {'class': 'tbl-odds'})
  if len(odds_tables) < 1:
    print 'Hit some weird (intermittent?) bug with no odds tables being found. Needs more investigation!'
    IPython.embed()
  if odds_tables[0].text == u'No games to display':
    return None
  try:
    odds = list(itertools.chain.from_iterable(filter(lambda x: x is not None,
                                                     [parse_odds_table(ot) for ot in odds_tables])))
  except TypeError:
    IPython.embed()
  df = pd.DataFrame(odds, columns=['Team', 'spread', 'vig', 'scoreline', 'rl_over', 'rl_under'])
  df.set_index('Team', drop=True, inplace=True)
  return df

def scrape_nba_odds_range(min_date=None, max_date=None):
  min_date = min_date or datetime.datetime.today() - datetime.timedelta(days=1)
  max_date = max_date or datetime.datetime.today()

  if isinstance(min_date, basestring):
    min_date = parser.parse(min_date)
  if isinstance(max_date, basestring):
    max_date = parser.parse(max_date)

  date = min_date
  pbar = progressbar.ProgressBar(widgets=[progressbar.Percentage(), ' ', progressbar.Bar(), ' ', progressbar.ETA()],
                                 maxval=int((max_date-min_date).total_seconds() / (60*60*24)) + 1)
  pbar.start()
  saved = 0
  hit = 0
  while date <= max_date:
    day_odds = load_odds_for_day(date)
    if day_odds is not None and len(day_odds) > 0:
      save_sbr_odds_info('nba', date, day_odds)
      saved += 1
    hit += 1
    date += datetime.timedelta(days=1)
    pbar.update(value=hit)
  pbar.finish()
  return saved

def scrape_cli():
  p = ArgumentParser()
  p.add_argument("--min-date", default=None, help="First day to scrape (defaults to yesterday)")
  p.add_argument("--max-date", default=None, help="Last day to scrape (defaults to today)")
  cfg = p.parse_args()

  logging.basicConfig()
  print 'Loading Vegas odds for NBA games from sportsbookreview...'
  loaded_count = scrape_nba_odds_range(cfg.min_date, cfg.max_date)
  print 'Saved odds for %d gamedays' % loaded_count
