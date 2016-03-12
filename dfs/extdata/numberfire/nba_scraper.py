
from argparse import ArgumentParser
from bs4 import BeautifulSoup
import pandas
import simplejson as json
import time
import re
import warnings
import logging
import progressbar
import numpy

from dfs.extdata.common.scraper import getSoupFromURL
from dfs.nba.playerid import name2nbaid
from .io import save_nf_histplayerinfo, save_nf_overview_data, load_nf_histplayerinfo, load_nf_overview_data, save_nf_salary_info
from .utils import parse_terminated_json, pickle_cache_html_helper

import ipdb
import IPython

sport = 'nba'  # for shared library functions. could probably do this better.

numberfire_url = 'http://www.numberfire.com/nba/fantasy/full-fantasy-basketball-projections'
nf_player_url = 'http://www.numberfire.com/nba/players/{slug}'

fantasy_sites = [u'fantasy_aces',
                 u'fantasy_feud',
                 u'draftster',
                 u'fantasy_score',
                 u'draft_kings',
                 u'draftday',
                 u'fanduel']

#<table cellspacing="0" cellpadding="0" border="0" class="player-table data-table small">

def load_player_history_table(div_soup):
  """Parse the HTML/Soup table for the numberfire predictions.
  Returns a pandas DataFrame
  """
  if not div_soup:
    return None
  rows = div_soup.findAll('tr')
  table_header = [x.getText() for x in rows[0].findAll('th')]
  table_data = [[x.getText() for x in row.findAll('td')] for row in rows[1:]]
  if not table_data:
    logging.debug("No predictions found!")
    return None
  table = pandas.io.parsers.TextParser(table_data,
                                       names=table_header,
                                       index_col=table_header.index('Date'),
                                       parse_dates=True).read()
  # Next we want to separate combined projection stats like FGM-A into separate columns for FGM and FGA
  dash_cols = [col for col in table.columns if '-' in col]
  for col in dash_cols:
      name_parts = col.split('-')
      series1name = name_parts[0]
      series2name = name_parts[0][:-1] + name_parts[1]
      series1data = table[col].apply(lambda data: float(data.split('-')[0]))
      series2data = table[col].apply(lambda data: float(data.split('-')[1]))
      table[series1name] = pandas.Series(data=series1data, name=series1name, index=table.index, dtype=numpy.dtype('float'))
      table[series2name] = pandas.Series(data=series2data, name=series2name, index=table.index, dtype=numpy.dtype('float'))
  table.drop(dash_cols, axis=1, inplace=True)
  return table

def load_player_salary_table(bsp):
  """ Load the historical player salaries from the NF_DATA variable on the beautiful soup page
  :param BeautifulSoup bsp: beautiful soup of player overview page
  :return:
  """
  # Extract javascript dict containing salary values, parse as JSON
  data = bsp.find_all("script")
  regex = re.compile('var NF_DATA = (.*?);')
  nf_data_dicts = []
  for d in data:
    if d.string:
      match = regex.search(d.string)
      if match:
        nf_data_dicts.append(parse_terminated_json(match.group(1)))
  if len(nf_data_dicts) != 1:
    warnings.warn("found multiple scripts with var NF_DATA... website probably changed")
  nf = nf_data_dicts[0]
  salaries_dict = nf['dfs_salary_charts']

  def parse_site_salary_dict(site_salary_dict):     # helper fn to unravel this super-nested json
    cols = site_salary_dict['data']['columns']  # the rest is useless graph metadata
    prepped = {c[0]: c[1:] for c in cols}
    prepped["Date"] = prepped['x']
    del prepped['x']
    del prepped['FP'] # we already know how many fantasy points they got
    df = pandas.DataFrame.from_dict(prepped)
    df['Date'] = pandas.to_datetime(df['Date'])
    df.set_index('Date', inplace=True)
    return df

  return pandas.concat([parse_site_salary_dict(salaries_dict[site]) for site in fantasy_sites], axis=1)

def load_stats_tables_from_history_page(url):
  """Load all the prediction tables from a Numberfire history page"""
  soup = getSoupFromURL(url)
  salary = load_player_salary_table(soup)
  projection_months = ['%s-schedule' % month for month in
                       ['October', 'November', 'December', 'January', 'February', 'March', 'April']]
  month_tables = []
  for month in projection_months:
    month_schedule = soup.find('div', attrs={'id': month})
    month_table = load_player_history_table(month_schedule)
    if month_table is not None:
      month_tables.append(month_table)
  if month_tables:
    all_predictions = pandas.concat(month_tables)
  else:
    all_predictions = None
  return all_predictions, salary

def scrape_numberfire_overview_page(cached_page=None, cache_target=None):
  """
  Return the information from the overview page. These should be all stats for current games.
  Additionally
  :param cached_page:
  :param cache_target:
  :return DataFrame: current statistics from the overview page
  """
  page = pickle_cache_html_helper(numberfire_url, cached_page, cache_target)
  bsp = BeautifulSoup(page)
  data = bsp.find_all("script")
  regex = re.compile('var NF_DATA = (.*?);')
  nf_data_dicts = []
  for d in data:
    if d.string:
      match = regex.search(d.string)
      if match:
          nf_data_dicts.append(parse_terminated_json(match.group(1)))
  if len(nf_data_dicts) != 1:
    warnings.warn("found multiple scripts with var NF_DATA... website probably changed")
  nf = nf_data_dicts[0]
  # need to stitch projection info to player / team tables
  proj_df = pandas.DataFrame(nf['daily_projections'])
  player_df = pandas.DataFrame.from_dict(nf['players'], orient='index')
  team_df = pandas.DataFrame.from_dict(nf['teams'], orient='index')
  # I'm still not sure what the stuff in the team analytics dataframe is, but, cool?
  team_an_df = pandas.DataFrame.from_dict(nf['team_analytics'], orient='index', dtype=float)
  joined_df = proj_df.join(player_df, on='nba_player_id')
  joined_df = joined_df.join(team_df, on='team_id', lsuffix="_player", rsuffix="_team")
  joined_df = joined_df.join(team_an_df, on='team_id', rsuffix="_") # the duplicate fields here are not necessary

  # Find the player slugs column. Use it to update our slug mappings
  #slug_dict = dict(zip(joined_df["name_player"], joined_df["slug_player"]))
  # Update the saved version of the slug dict with any new mappings we've found.
  #update_nf_playerslugs(slug_dict)
  # EDIT -- slug mappings will be pulled from the overview dataframe if needed

  return joined_df

def update_numberfire_history():
  # Start by updating our slug dict and overall numberfire player information
  overall_stats = scrape_numberfire_overview_page()
  save_nf_overview_data(sport, overall_stats)

  # We only load & update numberfire slug information for players appearing in the most recent batch of overview data
  # and only if we are also able to match this player to a BREF ID. A side effect of this is that we will make no
  # predictions for any NBA players who haven't played yet this year.
  pids_to_load = []
  for ix, row in overall_stats.iterrows():
    pid, confidence = name2nbaid(row['name_player'], player_team=row['name_team'], get_confidence=True)
    if confidence > 75:
      pids_to_load.append((pid, row['slug_player']))
  old_predictions = load_nf_histplayerinfo(sport, identifiers_to_load=pids_to_load)
  scraped_salaries = {}

  new_dataframes, updated_dataframes = 0, 0
  print "Scraping updated player predictions from Numberfire..."
  pbar = progressbar.ProgressBar(widgets=[progressbar.Percentage(), ' ', progressbar.Bar(), ' ', progressbar.ETA()])
  for pid, slug in pbar(pids_to_load):
    time.sleep(1)
    player_df, salary_df = load_stats_tables_from_history_page(nf_player_url.format(slug=slug))
    old_player_df = old_predictions.get(pid)
    if old_player_df is None:
      old_predictions[pid] = player_df
      new_dataframes += 1
    else:
      try:
        new_data = old_player_df.combine_first(player_df)
        old_predictions[pid] = new_data
      except ValueError as ex:
        ipdb.set_trace()
      updated_dataframes += 1
    scraped_salaries[pid] = salary_df
  logging.info('Saving scraped predictions (%d updated, %d added)', updated_dataframes, new_dataframes)
  save_nf_histplayerinfo(sport, old_predictions)
  save_nf_salary_info(sport, scraped_salaries)

def scrape_cli():
  p = ArgumentParser()
  p.add_argument("-v", "--verbose", help="verbose-ish, just print to console")
  cfg = p.parse_args()
  if cfg.verbose:
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.basicConfig(level=logging.INFO)
  else:
    logging.basicConfig(filename="logs/scrapenf.log", level=logging.INFO)
  update_numberfire_history()