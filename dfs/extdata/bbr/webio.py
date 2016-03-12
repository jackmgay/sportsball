"""
Contains all the functions related to hitting the basketball reference web site and parsing the data there.
"""
import pandas
import progressbar
from dfs.extdata.common.scraper import getSoupFromURL, soupTableToDF

from .common import bbr_id_regex

def get_gamelog_url(bref_id, year=None):
  # Get the URL for which a player's games for a given year are stored
  # We don't do this as a simple string formatting (the way we do for baseball) since the first letter of the brefid
  # is used in the URL as well, and it's easier to take only 1 parameter and contain that logic here.
  # Another difference from baseball: year is required
  return 'http://www.basketball-reference.com/players/{first}/{pid}/gamelog/{year}/'.format(first=bref_id[0],
                                                                                           pid=bref_id,
                                                                                           year=year)

def get_active_players(letters=list('abcdefghijklmnopqrstuvwxyz')):
  players = []
  print 'Loading currently active players from basketball-reference.com...'
  pbar = progressbar.ProgressBar(widgets=[progressbar.Percentage(), ' ', progressbar.Bar(), ' ', progressbar.ETA()])
  for letter in pbar(letters):
    letter_page = getSoupFromURL('http://www.basketball-reference.com/players/%s/' % (letter))
    # we know that all the currently active players have <strong> tags, so we'll limit our names to those
    current_names = letter_page.findAll('strong')
    for n in current_names:
      name_data = n.children.next()
      full_url = 'http://www.basketball-reference.com' + name_data.attrs['href']
      bref_id = bbr_id_regex.match(full_url).group('pid')
      players.append((bref_id, full_url))
  players = dict(players)
  return players

def dfFromGameLogURLList(gamelogs):
  """Takes a list of game log urls and returns a concatenated DataFrame"""
  return pandas.concat([dfFromGameLogURL(g) for g in gamelogs])

def dfFromGameLogURL(url):
  """Takes a url of a player's game log for a given year, returns a DataFrame"""
  glsoup = getSoupFromURL(url)
  reg_season_table = glsoup.findAll('table', attrs={'id': 'pgl_basic'})  # id for reg season table
  playoff_table = glsoup.findAll('table', attrs={'id': 'pgl_basic_playoffs'}) # id for playoff table
  # parse the table header.  we'll use this for the creation of the DataFrame
  header = []
  # use the playoff table to get the header if the guy never played in the regular season this year.
  header_table = reg_season_table if len(reg_season_table) else playoff_table
  for th in header_table[0].findAll('th'):
    if not th.getText() in header:
      header.append(th.getText())
  # add in headers for home/away and w/l columns. a must to get the DataFrame to parse correctly
  header[5] = u'HomeAway'
  header.insert(7, u'WinLoss')
  reg = soupTableToDF(reg_season_table, header)
  playoff = soupTableToDF(playoff_table, header)

  if reg is None:
    return playoff
  elif playoff is None:
    return reg
  else:
    return pandas.concat([reg, playoff])

def dfFromOverviewPage(soup):
  """Grab the 'totals' overview table from the player page"""
  table = soup.findAll('table', attrs={'id': 'totals'})
  header = [el.getText() for el in table[0].findAll('th')]
  df = soupTableToDF(table, header)
  return df

