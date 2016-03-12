import logging, requests
import pandas
import time
from dateutil import parser
from bs4 import BeautifulSoup

def getSoupFromURL(url):
  """
  This function grabs the url and returns and returns the BeautifulSoup object
  """
  logging.debug('    visiting %s', url)
  try:
    r = requests.get(url)
    time.sleep(1)  # Be nice!
  except:
    return None
  return BeautifulSoup(r.text)

def soupTableToTable(table_soup):
  """Parse a table from BeautifulSoup and turn it into a list of lists for pandas parsing
    Also worth noting out that we past in a LIST of soup elements (tables) here, and assume the first one is correct
  """
  if not table_soup:
    return None
  else:
    rows = table_soup[0].findAll('tr')[1:]  # all rows but the header
    # remove blank rows
    rows = [r for r in rows if len(r.findAll('td')) > 0]
    # remove inactive or did not play games from datadump -- or suspended!
    def filter_inactive_players(current_row):
      s = str(current_row)
      return 'Inactive' not in s and 'Did Not Play' not in s and 'Player Suspended' not in s
    rows = filter(filter_inactive_players, rows)
    parsed_table = [[col.getText() for col in row.findAll('td')] for row in rows] # build 2d list of table values
  return parsed_table

def parsedTableToDF(parsed_table, header, date_index=2):
  """Turn a list of lists parsed from a reference site into a Pandas dataframe
  :rtype pandas.DataFrame
  :return parsed dataframe
  """
  if parsed_table:
    return pandas.io.parsers.TextParser(parsed_table, names=header, index_col=date_index, parse_dates=True).read()
  else:
    # This is usually a bug.
    logging.debug('No games parsed. (If not inactive for regular season or playoffs, there is a bug!)')
    df = pandas.DataFrame(columns=header)
    df.set_index('Date')  #not convinced this is really necessary
    return df

def soupTableToDF(table_soup, header):
  """Parses the HTML/Soup table for the gamelog stats. Basically does what we expect to need for BBR.
  Split these parts up so that we can do scutwork inbetween for BSBR.
  Returns a pandas DataFrame
  """
  parsed_table = soupTableToTable(table_soup)
  parsed_df = parsedTableToDF(parsed_table, header)
  return parsed_df