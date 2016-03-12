
# Thanks to http://nbviewer.ipython.org/gist/andrewgiessel/5066789 from which this was adapted

import pandas
import bs4
from bs4 import BeautifulSoup
import time
import datetime
import json
import os
import re
import requests
from dateutil import parser
import cPickle as pickle
import sys
import argparse
import logging
import progressbar
import numpy as np

from dfs import GLOBAL_ROOT

from dfs.mlb.playerid import name2brefid, brefid_is_pitcher

from dfs.extdata.common.scraper import getSoupFromURL, soupTableToTable, parsedTableToDF
from dfs.extdata.common.io import combine_dataframe_into_pickle_file

# because Pickle -- try to fix this later
sys.setrecursionlimit(50000)

datadir = os.path.join(GLOBAL_ROOT, 'db/mlb/')

# holy crap a baseball-reference id can even have an apostrophe in it
bsbr_id_regex = re.compile("http://www\.baseball-reference\.com/players/\w/(?P<pid>[\w\.\'\d]+)\.shtml")

batting_stats_template = 'http://www.baseball-reference.com/players/gl.cgi?id={playerid}&t=b&year={year}'
pitching_stats_template = 'http://www.baseball-reference.com/players/gl.cgi?id={playerid}&t=p&year={year}'
fielding_stats_template = 'http://www.baseball-reference.com/players/gl.cgi?id={playerid}&t=f&year={year}'

def quicksave_playerdict(players, picklefn):
  with open(picklefn, 'wb') as outf:
    pickle.dump(players, outf)

def quickload_playerdict(picklefn):
  with open(picklefn, 'rb') as inf:
    return pickle.load(inf)

def json_save_playerdict(players):
  path = os.path.join(datadir, 'mlb_player_data.dict')
  logging.info('saving JSON player dict to %s', str(path))
  player_archive = {}
  for name, k in players.items():
    player_archive[name] = {'gamelog_url_list':k['gamelog_url_list'], 'overview_url':k['overview_url'], 'overview_url_content':k['overview_url_content']}
  json.dump(player_archive, open(path, 'wb'))

def json_load_playerdict():
  with open(os.path.join(os.path.expandvars(datadir), 'mlb_player_data.dict'), 'rb') as f:
    json_string = f.read()
    player_archive = json.loads(json_string)
  for player in player_archive:
    if 'overview_url' in player_archive[player] and 'bref_id' not in player_archive[player]:
      player_archive[player]['bref_id'] = bsbr_id_regex.match(player_archive[player]['overview_url']).group('pid')
  return player_archive

def create_json_file(argv=sys.argv):
  ap = argparse.ArgumentParser(description='create JSON dict of player overview data from baseball-reference.com')
  ap.add_argument("--picklefile", default=None, help="pickle file to save/load from if necessary")
  cfg = ap.parse_args()

  logging.basicConfig(filename="logs/scrapebsbr.log", level=logging.INFO)

  if cfg.picklefile and os.path.exists(cfg.picklefile):
    players = quickload_playerdict(cfg.picklefile)
  else:
    logging.info('Getting bref IDs of active MLB players from baseball-reference.com...')
    bref_ids = get_active_players()
    logging.info('Initializing player dictionary...')
    players = create_player_dict(bref_ids)
    if cfg.picklefile:
      quicksave_playerdict(players, cfg.picklefile)
    logging.info('Loading overview pages...')
    players = load_overview_pages(players)
    if cfg.picklefile:
      quicksave_playerdict(players, cfg.picklefile)
  json_save_playerdict(players)

# We save separate files for batting / pitching data
player_data_types = ['batting', 'pitching']

def get_attr_filename(bref_id, data_type):
  fn = os.path.join(datadir, 'player_data', bref_id + '_' + data_type)
  return fn

def save_dataframes(players, overwrite=False):
  """
  Save the pandas dataframes (the gamelog_data) from each player in players to a file
  :param dict[str, dict] players: the player dict
  :param bool overwrite: if True, delete old data
  :return:
  """
  saved_dataframes = 0
  for bref_id, attrs in players.iteritems():
    for data_type in player_data_types:
      if data_type in attrs and attrs[data_type] is not None:
        combine_dataframe_into_pickle_file(attrs[data_type], get_attr_filename(bref_id, data_type), overwrite=overwrite)
        saved_dataframes += 1
  logging.debug('Saved %d dataframes to %s', saved_dataframes, datadir)

def load_dataframes(players):
  """
  Load previously saved dataframes of gamelog data
  :param dict[str, dict[str, list]] players: the player dict
  :return:
  """
  loaded = 0
  for bref_id in players.keys():
    for data_type in player_data_types:
      target_file = get_attr_filename(bref_id, data_type)
      if os.path.exists(target_file):
        with open(target_file, 'r') as inf:
          players[bref_id][data_type] = pickle.load(inf)
      loaded += 1
  logging.debug('loaded %d dataframes from %s', loaded, datadir)
  return players

def _parse_bsbr_prefix_section(prefix_section):
  # sample contents:
  '''
    [u'\n',
   <b><a href="/players/b/badenbu01.shtml">Burke Badenhop</a>                    2008-2015</b>,
   u'\n',
   <a href="/players/b/baderar01.shtml">Art Bader</a>,
   u'                         1904-1904\n',
   <a href="/players/b/baderlo01.shtml">King Bader</a>,
   u'                        1912-1918\n',
   <a href="/players/b/badgrre01.shtml">Red Badgro</a>,
   u'                        1929-1930\n']
  '''
  # the structure of the data makes this a little annoying to parse; we need state-based parsing
  last_player = None
  last_url = None
  curr_year = datetime.date.today().year
  is_playing_regex = re.compile('\d+-%d' % curr_year) #regex to find if years player was playing include this year
  child_list = prefix_section.children
  for element in child_list:
    is_tag = isinstance(element, bs4.element.Tag)
    if is_tag and element.name == 'b':
      # Currently active player -- wrapped in <b> tag
      for pl in element.findAll('a'):
        if pl.attrs['href'].startswith('/players/'):
          player_url = 'http://www.baseball-reference.com' + pl.attrs['href']
          bref_id = bsbr_id_regex.match(player_url).group('pid')
          yield (bref_id, player_url)
      continue
    elif is_tag and element.name != 'a':
      # I have no idea what this is. Skip it.
      continue
    elif is_tag: # we know that this is an <a> tag
      # Player not wrapped in <b> tag. BSBR doesn't think he's active but maybe he was this year.
      # We will parse it and wait to see what years he played
      last_url = 'http://www.baseball-reference.com' + element.attrs['href']
      last_player = bsbr_id_regex.match(last_url).group('pid')
      continue
    elif last_player is None: # this is not a tag
      # Not currently parsing a player and encountered a string; just skip it
      continue
    else:
      if is_playing_regex.search(element):
        yield (last_player, last_url)
      last_player = None
      last_url = None
      continue

def get_active_players():
  letters = list('abcdefghijklmnopqrstuvwxyz')
  player_and_url_list = []
  print 'Checking currently active players on baseball-reference.com...'
  pbar = progressbar.ProgressBar(widgets=[progressbar.Percentage(), ' ', progressbar.Bar(), ' ', progressbar.ETA()])
  for letter in pbar(letters):
    letter_page = getSoupFromURL('http://www.baseball-reference.com/players/%s/' % (letter))
    # we don't just need active players (<b> tags), we need anyone who played in 2015!
    prefix_sections = letter_page.findAll('pre')
    for section in prefix_sections:
      player_and_url_list += list(_parse_bsbr_prefix_section(section))
  bref_id_dict = dict(player_and_url_list)
  return bref_id_dict

def create_player_dict(bref_ids):
  '''
  Initialize the player dictionary.
  :param dict [str, str] bref_ids: bref_id to URL dictionary
  :return:
  '''
  players = {}
  for bref_id, url in bref_ids.iteritems():
    players[bref_id] = {'overview_url':url}
    players[bref_id]['overview_url_content'] = None
    players[bref_id]['gamelog_url_list'] = []
    players[bref_id]['gamelog_data'] = None
  return players

def load_overview_pages(players):
  pbar = progressbar.ProgressBar(widgets=[progressbar.Percentage(), ' ', progressbar.Bar(), ' ', progressbar.ETA()])
  print 'Accessing and parsing overview pages...'
  for i, (bref_id, player_dict) in pbar(list(enumerate(players.items()))):
    if players[bref_id]['overview_url_content'] is None:
      overview_soup = getSoupFromURL(players[bref_id]['overview_url'])
      players[bref_id]['overview_url_content'] = overview_soup.text
      # the links to each year's game logs are in <li> tags, and the text contains 'Game Logs'
      # so we can use those to pull out our urls.
      game_log_links = []
      for li in overview_soup.find_all('li'):
        if 'Game Logs' in li.getText():
          game_log_links =  li.findAll('a')
      for game_log_link in game_log_links:
        players[bref_id]['gamelog_url_list'].append('http://www.baseball-reference.com' + game_log_link.get('href'))
  return players

def dfFromGameLogURLList(gamelogs):
  """Takes a list of game log urls and returns a concatenated DataFrame"""
  return pandas.concat([dfFromGameLogURL(g) for g in gamelogs])

def dfFromGameLogURL(url):
  """Takes a url of a player's game log for a given year, returns a DataFrame"""
  glsoup = getSoupFromURL(url)
  if not glsoup: #Hmm, this really shouldn't happen?
    logging.warning("No soup parsed from %s", url)
    return None
  stats_table = glsoup.findAll('table', attrs={'class': 'stats_table'})  # id for reg season table
  # parse the table header.  we'll use this for the creation of the DataFrame
  header = []
  if not stats_table:
    return None
  for th in stats_table[0].find("thead").findAll('th'):
    if not th.getText() in header:
      header.append(th.getText())
  # add in headers for home/away and w/l columns. a must to get the DataFrame to parse correctly
  header[5] = u'HomeAway'
  year = url[-4:] if re.search('(?P<year>\d+)$', url) else datetime.datetime.today().year
  date_column = header.index("Date")
  # turn soup of the table into a list o' lists
  stats_table = soupTableToTable(stats_table)
  # Run cleanup for MLB tables on baseball-reference.com -- turn dates into actual dates.
  for row_ix in range(len(stats_table)):
    raw_date = stats_table[row_ix][date_column]
    # Remove non-ASCII characters from the date str and replace with single spaces
    # (sometimes the space between month and day is a whacky unicode char; thx baseball-reference.)
    raw_date = re.sub(r'[^\x00-\x7F]+',' ', raw_date)
    # Ignore if the game was suspended and resumed later
    raw_date = re.sub(r'susp','',raw_date)
    if '(' not in raw_date and len(raw_date):
      stats_table[row_ix][date_column] = parser.parse(raw_date + ' ' + str(year))
    elif raw_date:
      # This is a doubleheader! Assign doubleheaders to "hours".
      # This doesn't do anything smart, except keep the data indexed by separate values so that
      # it could conceivably be retrieved later.
      dateparts = re.match("(?P<month>\w+) (?P<day>\d+) ?\((?P<gameno>\d+)\)", raw_date)
      assembled_date = parser.parse(dateparts.group("month") + " " + dateparts.group("day") + " " +
                                    dateparts.group("gameno") + ":00" + " " + str(year))
      stats_table[row_ix][date_column] = assembled_date
    else:
      # There's not a date here -- it's probably the EOY summary row.
      # It could also be a trade notification? Either way, ignore it.
      continue
  # Discard EOY summary row
  stats_table = stats_table[:-1]
  # Remove any rows which contain "Player went from" -- trade notifications sneaking in there
  stats_table = filter(lambda row: not any(isinstance(cell, basestring) and cell.startswith('Player went from') for cell in row), stats_table)
  # Use common function to turn our cleaned-up stats table into a dataframe
  parsed_df = parsedTableToDF(stats_table, header, date_index=date_column)
  return parsed_df

def get_active_years(player_url_list):
  ''' We're not doing the old-style scraping of each URL we find on the player page; instead we'll
   use load_player_dataframes and manually pass in which years to look up. But rather than rewrite
   the entire scraper I'm just going to hack the years off of the previous URLs.
   New: it's also possible/likely that one of the player urls ends without a year.
   To avoid scraping the same year twice, I used to remove this (and only keep years which are explicitly called out
    in the player_url_list).
   There seem to be some players for whom this doesn't work, though -- I suspect this is b/c they played their
   first current year game after the most recent update to their gamelog URL list. So we will always include the
   current year as well, if there is a link without a year included

  :param list[basestring] player_url_list: list of urls for the player
  :return list[basestring]: list of years that the player was active
  '''
  # Is this actually a player url?
  actual_player_url_re = 'http://www.baseball-reference.com/players/gl.cgi\?.+'
  valid_player_urls = filter(lambda x: re.match(actual_player_url_re, x) is not None, player_url_list)
  year_re = re.compile('http://www.baseball-reference.com/players/gl.cgi\?id=([\w\.\d]+)&t=\w&year=(?P<year>\d+)')
  def get_year(url):
    match = year_re.match(url)
    return match.group('year') if match else None
  year_url_parts = map(get_year, valid_player_urls)
  if None in year_url_parts:
    year_url_parts.remove(None)
    year_url_parts.append(str(datetime.date.today().year))
  return list(set(year_url_parts))

def load_player_dataframes(bref_id, year='', player_is_pitcher=False):
  batting_log_page = batting_stats_template.format(playerid=bref_id, year=year)
  batting_df = dfFromGameLogURL(batting_log_page)
  pitching_df = None
  if player_is_pitcher:
    pitching_log_page = pitching_stats_template.format(playerid=bref_id, year=year)
    pitching_df = dfFromGameLogURL(pitching_log_page)
  return batting_df, pitching_df

def cli_load_player():
  ap = argparse.ArgumentParser(description='scrape detailed player game log from baseball-reference.com & save')
  ap.add_argument("--player", default=None, help="player to load (if unspecified, load someone unloaded at random")
  cfg = ap.parse_args()

  logging.basicConfig(filename="logs/scrapebsbr.log", level=logging.INFO)

  players = json_load_playerdict()
  players = load_dataframes(players)

  if not cfg.player:  # unspecified? OK find someone we haven't loaded yet
    for p in players.keys():
      # Is there any data type unloaded for this guy
      if not set(player_data_types).intersection(players[p].keys()):
        cfg.player = p
        break
    else:
      logging.info('Data loaded for all players.')
      return
  # Did we actually find the player?
  if cfg.player not in players:
    logging.warning("Couldn't find player '%s'", cfg.player)
    return
  players = scrape_player(players, cfg.player)
  save_dataframes(players)

def scrape_player(players, bref_id):
  """
  Scrape all games for a player and add the parsed dataframes to the player dict provided.
  :param players: player dict
  :param bref_id: player bref_id of a player
  :return:
  """
  # found name, load player data
  logging.info('Loading player data for %s...', bref_id)
  # Load bref id for player
  try:
    player_is_pitcher = brefid_is_pitcher(bref_id)
  except KeyError:
    # player bref id not present in our lookup mapping
    # probably no one cares about this guy anyway right.
    logging.info('Player info for %s not found; not loading him' % bref_id)
    return players
  dataframes = []
  for year in get_active_years(players[bref_id]['gamelog_url_list']):
    dataframes.append(load_player_dataframes(bref_id, year, player_is_pitcher))
  # Combine the dataframes for each year.
  dataframes_by_type = zip(*dataframes)
  # Strip out "None" dataframes, where the player recorded no stats of this type this year.
  dataframes_by_type = map(lambda df_list: filter(lambda df: df is not None, df_list), dataframes_by_type)
  # Concatenate the dataframes (when we have stats for the given type)
  # If the player's not a pitcher they're all None for pitching, so return None if we have no stats of that type for
  # any year at all!
  built_dataframes = map(lambda l: pandas.concat(l) if len(l) else None, dataframes_by_type)

  for df in built_dataframes:
    assert df is None or not df.duplicated().any()

  # TODO(jgershen): if we start pulling stats for, say, fielding, need to update here and in load_player_dataframes
  players[bref_id]['batting'] = built_dataframes[0]
  players[bref_id]['pitching'] = built_dataframes[1]
  batting_games_loaded = len(built_dataframes[0]) if built_dataframes[0] is not None else 0
  pitching_games_loaded = len(built_dataframes[1]) if built_dataframes[1] is not None else 0
  logging.info('Loaded %d games batting, %d games pitching for %s.' % (batting_games_loaded, pitching_games_loaded, bref_id))
  return players

def add_new_players_to_playerdict():
  """
  Look for players new to baseball-reference.
  :return:
  """
  print 'Checking for players we have never loaded before...'
  active_players = get_active_players()
  players = json_load_playerdict()
  # Find out which players are new!
  new_players = {player: overview_page for player, overview_page in active_players.iteritems() if player not in players}
  if new_players:
    logging.info('Found %d new MLB players since last update!', len(new_players))
    new_playerdict = create_player_dict(new_players)
    # Load overview pages for those players!
    new_playerdict = load_overview_pages(new_playerdict)
    # Update the stats page locations for new players
    players.update(new_playerdict)
  # Save the new player page information to disk
  json_save_playerdict(players)

def update_player(players, player_id, year):
  """
  Update ONLY the current year's stats for a player
  :param dict[str, dict[str, pandas.DataFrame]] players: player dict
  :param str player_name: player to refresh stats for
  :return dict[str, dict]: players
  """
  active_years = get_active_years(players[player_id]['gamelog_url_list'])
  assert '2015' in active_years
  try:
    is_pitcher = brefid_is_pitcher(player_id)
  except KeyError:
    # still don't care about someone we can't load *any* data for
    return players
  year_stats = load_player_dataframes(player_id, year, is_pitcher)
  year_stats_by_attr = zip(player_data_types, year_stats)
  for data_type, stats_df in year_stats_by_attr:
    old_data = players[player_id].get(data_type)
    if old_data is None and stats_df is not None:
      players[player_id][data_type] = stats_df
    elif stats_df is not None:
      new_data = old_data.combine_first(stats_df)
      players[player_id][data_type] = new_data
    if players[player_id].get(data_type) is not None:
      # Drop NaT values in the index (from e.g. trades) here, if applicable
      df = players[player_id][data_type]
      if df.index.hasnans:
        df = df.loc[df.index.drop_duplicates().drop(np.nan)]
      players[player_id][data_type] = df
  return players

def cli_update_players():
  ap = argparse.ArgumentParser(description='update cached stats from baseball-reference.com')
  ap.add_argument("--year", type=int, help="Override stats for the target year instead of this one")
  ap.add_argument("--max-players", type=int, help="Only load this many players at most (for testing only!)",
                  default=0)
  ap.add_argument("--specific-player", type=str, help="Load/update only this brefid (also for debugging)")
  ap.add_argument("--skip-update", action='store_true', help="Skip the update existing players step")
  cfg = ap.parse_args()
  logging.basicConfig(filename="logs/scrapebsbr.log", level=logging.INFO)

  add_new_players_to_playerdict()

  players = json_load_playerdict()
  players = load_dataframes(players)

  players_to_load = [p for p in players if not len(set(player_data_types).intersection(players[p].keys()))]
  if cfg.max_players:
    players_to_load = players_to_load[:cfg.max_players]
  if cfg.specific_player:
    players_to_load = [cfg.specific_player]

  if players_to_load:
    print 'We have no stats for %d players -- trying to load in full.' % len(players_to_load)
    pbar = progressbar.ProgressBar(widgets=[progressbar.Percentage(), ' ', progressbar.Bar(), ' ', progressbar.ETA()])
    for player in pbar(players_to_load):
      logging.info('Never loaded player data for %s (crawling now)', player)
      scrape_player(players, player)
    print 'Done loading players, saving progress!'
    save_dataframes(players)

  year_to_update = cfg.year or datetime.date.today().year

  players_to_update = [x for x in list(players.keys()) if x not in players_to_load]
  if cfg.max_players:
    players_to_update = players_to_update[:cfg.max_players]

  for player in players:
    if players[player]:
      assert players[player]

  if cfg.skip_update:
    save_dataframes(players)
    return

  print 'Updating player stats for %d previously scraped players from baseball-reference.com...' % len(players_to_update)
  pbar = progressbar.ProgressBar(widgets=[progressbar.Percentage(), ' ', progressbar.Bar(), ' ', progressbar.ETA()])
  for player in pbar(players_to_update):
    # found name, load player data
    logging.info('Updating player data for %s...', player)
    players = update_player(players, player, year=year_to_update)
  save_dataframes(players)

def load_full_gamelogs():
  """
  Exportable loading function for using baseball reference data elsewhere in the project
  :return:
  """
  players = json_load_playerdict()
  players = load_dataframes(players)
  return players