"""
Flow and control logic for the BBR scraper. Includes CLIs and main updating functions.
"""

import datetime
import sys
import argparse
import logging
import progressbar

from dfs.extdata.common.scraper import getSoupFromURL
from . import mapping_df
from .common import bbr_id_regex
from .webio import dfFromGameLogURLList, dfFromGameLogURL, dfFromOverviewPage, get_active_players
from .io import load_overview_dict, save_overview_dict, create_player_dict, load_dataframes, save_dataframes

def load_overview_pages(players):
  """
  Hit the overview page and load gamelog_url_list for each of the players in the player dict.
  Maybe this should be in the webio submodule? I am leaving it here since it controls scraping program flow.
  :param players: player dict
  :return dict: player dict
  """
  # Helper function to guess which position a player plays from the overview table of stats.
  # Just grab the position from the most recent year in which it was defined, and return that.
  def quick_position_guess(overview_table):
    return overview_table.dropna(subset=['Pos'])['Pos'].iloc[-1]
  pbar = progressbar.ProgressBar(widgets=[progressbar.Percentage(), ' ', progressbar.Bar(), ' ', progressbar.ETA()])
  print 'Accessing and parsing overview pages...'
  for i, (bref_id, player_dict) in pbar(list(enumerate(players.items()))):
    overview_soup = getSoupFromURL(players[bref_id]['overview_url'])
    players[bref_id]['overview_url_content'] = overview_soup.text
    # the links to each year's game logs are in <li> tags, and the text contains 'Game Logs'
    # so we can use those to pull out our urls.
    for li in overview_soup.find_all('li'):
      if 'Game Logs' in li.getText():
        game_log_links = li.findAll('a')
        for game_log_link in game_log_links:
          players[bref_id]['gamelog_url_list'].append('http://www.basketball-reference.com' + game_log_link.get('href'))
    player_name = overview_soup.find('h1').text
    players[bref_id]['info']['name'] = player_name
    # Read (guess?) player's position
    overview_table = dfFromOverviewPage(overview_soup)
    if len(overview_table.dropna(subset=['Pos'])) > 0:
      players[bref_id]['info']['pos'] = quick_position_guess(overview_table)
    else:
      players[bref_id]['info']['pos'] = '?'  # this will only happen for chumps but by defining a value we should block exceptions
  return players

def load_player(players, bref_id):
  """
  Scrape all games for a player and add the parsed dataframes to the player dict provided.
  This reloads all years, not just the most recent
  :param players: player dict
  :param bref_id: player bref_id of a player
  :return:
  """
  pandas_df = dfFromGameLogURLList(players[bref_id]['gamelog_url_list'])
  players[bref_id]['gamelog_data'] = pandas_df
  return players

def update_player(players, bref_id, year):
  """
  Update the current year's stats for a player. Does not scrape prior years.
  :param dict[str, dict[str, pandas.core.frame.DataFrame]] players: player dict
  :param bref_id: player bref_id of a player
  :return dict[str, dict]: players
  """
  applicable_urls = [url for url in players[bref_id]['gamelog_url_list'] if str(year) in url]
  if not applicable_urls:
    logging.info('  No games played in %s', str(year))
    return players
  df_url = applicable_urls[0]
  df = dfFromGameLogURL(df_url)
  old_data = players[bref_id].get('gamelog_data')
  if old_data is None:
    players[bref_id]['gamelog_data'] = df
  else:
    new_data = old_data.combine_first(df)
    players[bref_id]['gamelog_data'] = new_data
  return players

def scrape_overview_for_new_players():
  logging.info('scrape_overview_for_new_players: polling for new entrants to the NBA')
  active_players = get_active_players()
  players = load_overview_dict()
  # Find out which players are new!
  new_players = {player: overview_page for player, overview_page in active_players.iteritems() if player not in players}
  if new_players:
    logging.info('scrape_overview_for_new_players: found %d new NBA players since last update!', len(new_players))
    new_playerdict = create_player_dict(new_players)
    # Load overview pages for those players!
    new_playerdict = load_overview_pages(new_playerdict)
    # Add the new players to the global player dict
    players.update(new_playerdict)
    # Save the new player dict to disk - now including any new players
    save_overview_dict(players)

def update_mapping_df(players):
  """
  Call this as a last step after scraping new BBR data. Updates our list of names (for player ID matching purposes)
   to
  each dict has brefid/name/team/pos keys
  :return:
  """
  player_info_dicts = []
  for player, data_dict in players.iteritems():
    gamelogs = data_dict['gamelog_data']
    team = gamelogs.loc[gamelogs.last_valid_index()]['Tm']
    player_info_dict = {'brefid': player,
                        'name': data_dict['info']['name'],
                        'team': team,
                        'pos': data_dict['info']['pos'],}
    player_info_dicts.append(player_info_dict)
  mapping_df.update(player_info_dicts)

def update_players(year=None):
  year_to_update = year
  if not year_to_update:
    year_to_update = datetime.date.today().year
    if datetime.date.today().month > 8: # it's really the 201X-201(x+1) season, we should use x+1 as year
      year_to_update += 1
  logging.info("update_players: Loading all stats for new players and re-examining stats from %d" % year_to_update)
  scrape_overview_for_new_players()
  players = load_overview_dict()
  players = load_dataframes(players)
  # Identify players we know of, but haven't loaded full stats for.
  # This will include any players we just found with scrape_overview_for_new_players.
  players_to_load = [p for p in players if 'gamelog_data' not in players[p]]
  if players_to_load:
    logging.info("update_players: loading first-time stats for %d players", len(players_to_load))
    pbar = progressbar.ProgressBar(widgets=[progressbar.Percentage(), ' ', progressbar.Bar(), ' ', progressbar.ETA()])
    for player in pbar(players_to_load):
      logging.info('update_players: loading first-time stats for %s', player)
      players = load_player(players, player)
  pbar = progressbar.ProgressBar(widgets=[progressbar.Percentage(), ' ', progressbar.Bar(), ' ', progressbar.ETA()])
  for player in pbar(players.keys()):
    # found name, load player data
    logging.info('update_players: updating player data for %s...', player)
    players = update_player(players, player, year=year_to_update)
  save_dataframes(players)
  update_mapping_df(players)

def cli_update_players(argv=sys.argv):
  ap = argparse.ArgumentParser(description='update player stats from basketball-reference.com')
  ap.add_argument("--year", type=int, help="Re-crawl stats from the specified year", default=None)
  cfg = ap.parse_args()
  logging.basicConfig(level=logging.INFO)
  logging.getLogger("requests").setLevel(logging.WARNING)
  update_players(year=cfg.year)
