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

mlb_template = 'http://www.sportsbookreview.com/mlb-baseball/odds-scores/{year}{month}{day}/'

sbr_to_bsbr_tla = {
 u'Arizona': 'ARI',
 u'Atlanta': 'ATL',
 u'Baltimore': 'BAL',
 u'Boston': 'BOS',
 u'CH Cubs': 'CHC',
 u'CH White Sox': 'CHW',
 u'Cincinnati': 'CIN',
 u'Cleveland': 'CLE',
 u'Colorado': 'COL',
 u'Detroit': 'DET',
 u'Houston': 'HOU',
 u'Kansas City': 'KCR',
 u'L.A. Angels': 'LAA',
 u'L.A. Dodgers': 'LAD',
 u'Miami': 'MIA',
 u'Milwaukee': 'MIL',
 u'Minnesota': 'MIN',
 u'N.Y. Mets': 'NYM',
 u'N.Y. Yankees': 'NYY',
 u'Oakland': 'OAK',
 u'Philadelphia': 'PHI',
 u'Pittsburgh': 'PIT',
 u'San Diego': 'SDP',
 u'San Francisco': 'SFG',
 u'Seattle': 'SEA',
 u'St. Louis': 'STL',
 u'Tampa Bay': 'TBR',
 u'Texas': 'TEX',
 u'Toronto': 'TOR',
 u'Washington': 'WSN'
}

def _get_runline_components(team_row):
  # Parse the runline overunder & odds component
  raw_ml = team_row.find('td', {'class': 'tbl-odds-c7'}).text
  defractioned = raw_ml.replace(u'\xbd', '.5') # 1/2 character
  if 'N/A' in defractioned:
    return [np.nan, np.nan]
  parts = [float(x) for x in defractioned.split()]
  return parts

def _get_moneyline_component(team_row):
  raw_ml = team_row.find('td', {'class': 'tbl-odds-c5'}).text
  if 'N/A' in raw_ml:
    return np.nan
  else:
    return float(raw_ml)

def parse_odds_table(odds_table):
  rows = odds_table.findAll('tr')
  team1 = rows[2]  # also contains over bet
  team2 = rows[3]  # also contains under bet
  t1name = team1.find('td', {'class': 'tbl-odds-c2'}).text
  t2name = team2.find('td', {'class': 'tbl-odds-c2'}).text
  t1_current_ml = _get_moneyline_component(team1)
  t2_current_ml = _get_moneyline_component(team2)
  # Get runline over / under if present
  over_l = _get_runline_components(team1)
  under_l = _get_runline_components(team2)[1:]
  # Map sportsbook team names to three-letter abbreviations
  try:
    t1tla = sbr_to_bsbr_tla[t1name]
    t2tla = sbr_to_bsbr_tla[t2name]
  except KeyError:
    if t1name in ['American League', 'National League']: # odds for allstar game, just skip
      return []
    else: # something is wrong
      raise
  # Create a row for each team with
  # team, moneyline, run over/under, overML, underML
  return [[t1tla, t1_current_ml] + over_l + under_l,
          [t2tla, t2_current_ml] + over_l + under_l]

def load_odds_for_day(game_day):
  # TODO(jgershen): we should handle doubleheaders more gracefully here
  day_part = '%02d' % game_day.day
  month_part = '%02d' % game_day.month
  url = mlb_template.format(year=game_day.year, month=month_part, day=day_part)
  soup = getSoupFromURL(url)
  odds_tables = soup.findAll('table', {'class': 'tbl-odds'})
  if odds_tables[0].text == u'No games to display':
    return None
  odds = list(itertools.chain.from_iterable([parse_odds_table(ot) for ot in odds_tables]))
  df = pd.DataFrame(odds, columns=['Team', 'moneyline', 'runline', 'rl_over', 'rl_under'])
  df['odds'] = df['moneyline'].apply(moneyline_to_implied_odds)
  df['rl_over_odds'] = df['rl_over'].apply(moneyline_to_implied_odds)
  df['rl_under_odds'] = df['rl_under'].apply(moneyline_to_implied_odds)
  df.set_index('Team', drop=True, inplace=True)
  return df

def scrape_mlb_odds_range(min_date=None, max_date=None):
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
      save_sbr_odds_info('mlb', date, day_odds)
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

  logging.basicConfig(filename="logs/scrapeSBR.log", level=logging.INFO)
  print 'Loading Vegas odds for MLB games from sportsbookreview...'
  loaded_count = scrape_mlb_odds_range(cfg.min_date, cfg.max_date)
  print 'Saved odds for %d gamedays' % loaded_count
