"""
Yank stuff out of the FanDuel pages currently displayed.
"""
import datetime
import re
import os
from parsedatetime import Calendar
from bs4 import BeautifulSoup
from collections import namedtuple
from dateutil import parser
from .io import game_was_already_scraped

LineupInfo = namedtuple('LineupInfo', ['loaded_teams', 'unloaded_teams', 'parsed_players'])

def _salary_cap_to_int(salary_cap_str):
  # fairly lazy way to parse Salary Cap string $xxK
  salary_cap_str = salary_cap_str.strip().lower()
  if salary_cap_str.startswith('salary cap'):
    salary_cap_str = salary_cap_str[10:].strip()
  if ':' in salary_cap_str:
    salary_cap_str = salary_cap_str.split(":")[1].strip()
  if salary_cap_str.startswith('$'):
    salary_cap_str = salary_cap_str[1:]
  if salary_cap_str.endswith('k'):
    return int(salary_cap_str[:-1]) * 1000
  else:
    return int(salary_cap_str)

def _parse_entry_fee(entry_fee_str):
  entry_fee_str = entry_fee_str.strip().lower()
  if entry_fee_str.startswith('entry fee'):
    entry_fee_str = entry_fee_str[9:].strip()
  if entry_fee_str.startswith('$'):
    entry_fee_str = entry_fee_str[1:]
  return int(entry_fee_str)

def _parse_table_format_str(table_format_str):
  table_format_str = table_format_str.strip()
  if table_format_str.startswith("Format:"):
    table_format_str = table_format_str[7:].strip()
  print 'parsing table'
  regex = '\w+ Salary Cap (?P<cap>\d+)k (?P<datestr>\w+ \w+ \w+)'
  match = re.match(regex, table_format_str)
  salary_cap = int(match.group('cap')) * 1000
  date = match.group('datestr')
  return salary_cap, date

def parse_game_info(salary_cap_str, entry_fee_str, table_format_str):
  parsed_cap = _salary_cap_to_int(salary_cap_str)
  parsed_fee = _parse_entry_fee(entry_fee_str)
  cap_check, datestr = _parse_table_format_str(table_format_str)
  assert cap_check == parsed_cap
  game_date = parser.parse(datestr)
  return parsed_cap, parsed_fee, game_date

entrybutton_re = re.compile('https://www.fanduel.com/games/(?P<gameid>\d+)/contests/\d+-(?P<tableid>\d+)/enter')
game_element_re = re.compile('Enter\n(?P<title>[\w\d/ \(\)]+)\n(?P<datetime>\w* ?\d+:\d\d [ap]m\.) \d+ of \d+ entries. \$\d+k cap.\n\$\d+ ENTRY \$\d+ PRIZES\ni')

def _guess_game_date(datetime_str):
  # Try to turn a datetime string from FD into an actual datetime
  datetime_str.replace('Sun', 'Sunday')
  datetime_str.replace('Mon', 'Monday')
  datetime_str.replace('Tues', 'Tuesday')
  datetime_str.replace('Wed', 'Wednesday')
  datetime_str.replace('Thurs', 'Thursday')
  datetime_str.replace('Fri', 'Friday')
  datetime_str.replace('Sat', 'Saturday')
  cal = Calendar()
  dt, ret_code = cal.parseDT(datetime_str)
  return dt

def old_select_game_element(entry_buttons):
  """
  We used to use this function to parse old lobby buttons
  :return:
  """
  # First try to find a game with an unseen gameid
  for eb in entry_buttons:
    eb_parent = eb.find_element_by_xpath('..')
    match = game_element_re.match(eb_parent.text)
    fd_game_date = _guess_game_date(match.group('datetime'))
    game_title = match.group('title')

    # parse game and table id's
    table_link_txt = eb.get_attribute("href")
    matches = re.match(entrybutton_re, table_link_txt)
    if not matches:
      continue
    fd_game_id = int(matches.group('gameid'))
    fd_table_id = int(matches.group('tableid'))

    # Has this game been loaded before?
    if not game_was_already_scraped(fd_game_date, fd_game_id):
      return eb, fd_game_id, fd_table_id, fd_game_date, game_title
  return None

contest_element_id_regex = re.compile(r"contest_(?P<gameid>\d+)-(?P<tableid>\d+)")

def parse_contest_element(browser_elem):
  """
  Parse contest element for entry button, FanDuel game ID, FD table ID, game start as datetime, game name, and entry cost
  :param selenium.webdriver.remote.webelement.WebElement browser_elem: contest div element
  :return tuple[selenium.webdriver.remote.webelement.WebElement, int, int, datetime.datetime, str, int]: see description
  """
  match_groups = contest_element_id_regex.match(browser_elem.get_attribute("id"))
  fd_game_id = int(match_groups.group("gameid"))
  fd_table_id = int(match_groups.group("tableid"))
  game_title = browser_elem.find_element_by_xpath("div[1]/a/span[1]").text
  game_fee = browser_elem.find_element_by_xpath("div[2]/span").text.replace('$','').replace(',','')
  game_time = browser_elem.find_element_by_xpath("div[2]/time").text
  cal = Calendar()
  fd_game_date, ret_code = cal.parseDT(game_time)
  entry_button_element = browser_elem.find_element_by_xpath("a")
  return entry_button_element, fd_game_id, fd_table_id, fd_game_date, game_title, game_fee

def parse_lineup_page(lineup_html):
  """
  Figure out which teams have submitted lineups, and which players are confirmed in.
  :param lineup_html: HTML of page
  :return LineupInfo: teams with lineups, teams w/o lineups, confirmed starters
  """
  nonwordpattern = re.compile('[\W_]+')
  numpattern = re.compile('\d+')
  soup = BeautifulSoup(lineup_html)
  # No idea why the class is mytruncate weather, but, these players are confirmed in the lineup.
  # At least as of 8/14/2015.
  # We could potentially do some more work here to find out where *in* the lineup they're batting, as well
  player_spans = soup.findAll('span', {'class': 'mytruncate weather'})
  parsed_players = set([span.text.strip() for span in player_spans])
  # Determine which teams have sent in lineups
  loaded = set()
  unloaded = set()
  parsed_teams = soup.findAll('table', {'class': 'cats'})
  for team in parsed_teams:
    for td in team.findAll('td'):
      if len(td.findAll('i', {'class': 'icon'})):
        # This is a loaded team with a checkmark box -- lineup is in
        loaded.add(nonwordpattern.sub('', td.text))
      else:
        if not re.search(numpattern, td.string): # if there are digits, this is the game time
          unloaded.add(nonwordpattern.sub('', td.text))
  return LineupInfo(loaded_teams=loaded,
                    unloaded_teams=unloaded,
                    parsed_players=parsed_players)

def parse_nba_player_list(page_html):
  import IPython
  # This... doesn't really work. Leaving it to maybe-update in the future.
  soup = BeautifulSoup(page_html)
  player_rows = soup.findAll('tr', {'class': 'vs-repeat-repeated-element'})
  print len(player_rows)
  for i, row in enumerate(player_rows):
    print i
    player_pos = row.find('td', {'class': 'player-position'}).text.strip()
    player_name = row.find('span', {'class': 'player-first-name'}).text.strip() + ' ' + row.find('span', {'class': 'player-last-name'}).text.strip()
    flag_span = row.find('player')
    injured = row.findAll('abbr', {'class': 'player-badge'})
    if injured:
      injury_status = injured[0].attr('data-injury-status')
    else:
      injury_status = 'OK'
    print player_pos, player_name, injury_status
  IPython.embed()
