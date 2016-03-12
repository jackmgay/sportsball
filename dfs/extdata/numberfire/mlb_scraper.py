
from argparse import ArgumentParser
from bs4 import BeautifulSoup
import datetime
import pandas
import simplejson as json
import time
import re
import warnings
import logging
import progressbar
import numpy

from dfs.mlb import playerid
from dfs.extdata.common.scraper import getSoupFromURL
from .io import save_nf_histplayerinfo, save_nf_overview_data, load_nf_histplayerinfo, load_nf_overview_data, save_nf_salary_info
from .utils import parse_terminated_json, pickle_cache_html_helper

import ipdb

sport = 'mlb'  # for shared library functions. could probably do this better.

batter_url = 'http://www.numberfire.com/mlb/fantasy/fantasy-baseball-projections'
nf_player_url = 'http://www.numberfire.com/mlb/players/{slug}'

pitcher_url = 'https://www.numberfire.com/mlb/fantasy/fantasy-baseball-projections/pitchers'

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

  # Adjust the date-times in the index to handle doubleheader games the way we do for baseball-reference.com.
  duplicates = table.index.duplicated()
  if duplicates.any():
    doubleheader_index = table.index.to_series()
    for i in range(len(table)):
      if duplicates[i]:
          doubleheader_index[i] = doubleheader_index[i] + datetime.timedelta(hours=1)
    table = table.reset_index(drop=True).set_index(doubleheader_index)
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
  #salary = load_player_salary_table(soup)
  projection_months = ['%s-schedule' % month for month in
                       ['March', 'April', 'May', 'June', 'July', 'August', 'September', 'October']]
  month_tables = []
  for month in projection_months:
    month_schedule = soup.find('div', attrs={'id': month})
    month_table = load_player_history_table(month_schedule)
    if month_table is not None:
      month_tables.append(month_table)
  if month_tables:
    all_predictions = pandas.concat(month_tables)
    all_predictions.sort_index(inplace=True)
    if all_predictions.index.duplicated().any():
      print 'Duplicate games scraped!'
      import IPython
      IPython.embed()
  else:
    all_predictions = None
  return all_predictions

def scrape_overview_page(page_type='batter', cached_page=None, cache_target=None):
  """
  Return the information from the overview page. These should be all stats for current games.
  Additionally
  :param cached_page:
  :param cache_target:
  :return DataFrame: current statistics from the overview page
  """
  if page_type == 'batter':
    target_url = batter_url
  elif page_type == 'pitcher':
    target_url = pitcher_url
  else:
    raise NotImplementedError
  page = pickle_cache_html_helper(target_url, cached_page, cache_target)
  bsp = BeautifulSoup(page)
  data = bsp.find_all("script")
  regex = re.compile('var NF_DATA = (.*?);\s+var GAQ_PUSH')
  nf_data_dicts = []
  for d in data:
    if d.string:
      match = regex.search(d.string)
      if match:
          nf_data_dicts.append(json.loads(match.group(1)))
  if len(nf_data_dicts) != 1:
    warnings.warn("found multiple scripts with var NF_DATA... website probably changed")
  nf = nf_data_dicts[0]

  # need to stitch projection info to player / team tables
  proj_df = pandas.DataFrame(nf['projections'])
  player_df = pandas.DataFrame.from_dict(nf['players'], orient='index')

  combined_df = pandas.merge(proj_df, player_df, left_on='mlb_player_id', right_index=True, sort=False)
  # set index to baseball reference ID
  combined_df['bref_id'] = combined_df['sports_reference_id']
  combined_df.drop('sports_reference_id', axis=1, inplace=True)
  combined_df.set_index('bref_id', inplace=True)
  return combined_df

def update_numberfire_history(bref_id=None, save_predictions=True):
  """
  Scrape numberfire history pages for predictions for bref_id (or all players if unspecified).
  :param bref_id: baseball reference id of player to return info for
  :param save_predictions: whether to save the predictions to files
  :return: if player_name was specified, returns the stats for that player.
  """
  # Start by updating our slug dict and overall numberfire player information
  print 'Scraping overview stats...'
  batter_stats = scrape_overview_page('batter')
  pitcher_stats = scrape_overview_page('pitcher')
  overall_stats = pandas.concat([batter_stats, pitcher_stats])
  save_nf_overview_data(sport, overall_stats)
  print 'done.'

  # We only load & update numberfire slug mappings for players appearing in the most recent batch of overview data
  # (and for whom we can load bref id <-> numberfire slug mappings)
  bref_ids_to_slugs = dict(zip(overall_stats.index, overall_stats["slug"]))
  if bref_id:
    bref_ids_to_slugs = {bref_id: bref_ids_to_slugs[bref_id]}
  bref_ids_to_slugs = {bref_id: slug for bref_id, slug in bref_ids_to_slugs.iteritems() if bref_id and slug}

  old_predictions = load_nf_histplayerinfo(sport, identifiers_to_load=bref_ids_to_slugs.keys())
  scraped_salaries = {}

  new_dataframes, updated_dataframes = 0, 0
  print "Scraping updated player predictions from Numberfire..."
  pbar = progressbar.ProgressBar(widgets=[progressbar.Percentage(), ' ', progressbar.Bar(), ' ', progressbar.ETA()])
  for bref_id in pbar(bref_ids_to_slugs):
    time.sleep(1)
    player_df = load_stats_tables_from_history_page(nf_player_url.format(slug=bref_ids_to_slugs[bref_id]))
    old_player_df = old_predictions.get(bref_id)
    if old_player_df is None:
      old_predictions[bref_id] = player_df
      new_dataframes += 1
    else:
      try:
        new_data = old_player_df.combine_first(player_df)
        assert not new_data.index.duplicated().any()
        old_predictions[bref_id] = new_data
      except ValueError as ex:
        ipdb.set_trace()
      updated_dataframes += 1
  if save_predictions:
    logging.info('Saving scraped predictions (%d updated, %d added)', updated_dataframes, new_dataframes)
    save_nf_histplayerinfo(sport, old_predictions)
    save_nf_salary_info(sport, scraped_salaries)
  if bref_id:
    return old_predictions[bref_id]

def scrape_cli():
  p = ArgumentParser()
  p.add_argument("--brefid", default=None, help="If provided, only scrape & update data for this baseball reference id")
  cfg = p.parse_args()

  logging.basicConfig(filename="logs/scrapenf.log", level=logging.INFO)
  update_numberfire_history(bref_id=cfg.brefid)