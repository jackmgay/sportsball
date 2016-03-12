from __future__ import absolute_import
import bs4
import datetime
import time
import pandas
import progressbar
import logging
import io
from argparse import ArgumentParser
from dateutil import parser
from dfs.extdata.common.scraper import getSoupFromURL

from .rgio import save_rg_salary_info

# MLB parses from CSV -- we should really switch NBA around to doing it this way!
mlb_url_template = 'http://rotoguru1.com/cgi-bin/byday.pl?date={month}{day}&game={game_code}&scsv=1'
nba_url_template = 'http://rotoguru1.com/cgi-bin/hyday.pl?mon={month}&day={day}&game={game_code}'

game_code_dict = {'FanDuel': 'fd'}

nba_position_list = ['PG', 'PF', 'SG', 'SF', 'C']
mlb_position_list = ['P', 'C', '1B', '2B', '3B', 'SS', 'OF']
mlb_position_key = {1: 'P',
                    2: 'C',
                    3: '1B',
                    4: '2B',
                    5: '3B',
                    6: 'SS',
                    7: 'OF'}

def is_actual_player_row(sport, row):
  if not isinstance(row.contents[0], bs4.element.Tag):
    return False
  position_list = nba_position_list if sport == 'nba' else mlb_position_list
  for pos in position_list:
    if row.contents[0].text == pos:
      return True
  return False

def parse_player_row(row):
  position = row.contents[0].text
  name_parts = row.contents[1].text.strip().strip('^').split(',')
  name = name_parts[1] + ' ' + name_parts[0]
  raw_salary = row.contents[3].text
  parsed_salary = raw_salary.strip('$').replace(',', '')
  return name, position, parsed_salary

def parse_mlb_csv_from_soup(soup):
  magic_header_string = 'Date;GID;MLB_ID;Name;Starter;Bat order;FD posn;FD pts;FD sal;Team;Oppt;dblhdr;'
  csv_containing_element = filter(lambda element: magic_header_string in element.text, soup.findAll('p'))[0]
  df = pandas.DataFrame.from_csv(io.StringIO(csv_containing_element.text), sep=';', index_col="MLB_ID")
  df = df[["Name", "FD posn", "FD sal"]].dropna()
  df["Position"] = df["FD posn"].apply(lambda x: mlb_position_key[x])
  df["Salary"] = df["FD sal"]
  ret = df[["Name", "Position", "Salary"]]
  return ret

def load_positions_for_day(sport, game_date, game='FanDuel'):
  ''' get salaries and positions for eligible players for the given day / fantasy site
  :param datetime.datetime game_date:
  :param basestring game:
  :return:
  '''
  month, day = game_date.month, game_date.day
  url_template = nba_url_template if sport == 'nba' else mlb_url_template
  day_part = '%02d' % day
  url = url_template.format(month=month, day=day_part, game_code=game_code_dict[game])
  soup = getSoupFromURL(url)
  if sport == 'nba':
    all_rows = soup.findAll('tr')
    player_rows = filter(lambda r: is_actual_player_row('nba', r), all_rows)
    parsed_rows = map(parse_player_row, player_rows)
    day_salaries = pandas.DataFrame.from_records(parsed_rows, columns=['Player', 'Position', 'Salary'])
    day_salaries["Salary"] = day_salaries["Salary"].apply(int)
    day_salaries["Player"] = day_salaries["Player"].apply(lambda x: x.strip())
    day_salaries["Position"] = day_salaries["Position"].apply(lambda x: x.strip())
    day_salaries.set_index("Player", inplace=True)
  else:
    day_salaries = parse_mlb_csv_from_soup(soup)
  return day_salaries

def update_salary_history(sport, min_date=None, max_date=None):
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
    time.sleep(1)
    day_salaries = load_positions_for_day(sport, date)
    if len(day_salaries) > 0:
      save_rg_salary_info(sport, date, day_salaries)
      saved += 1
    hit += 1
    date += datetime.timedelta(days=1)
    pbar.update(value=hit)
  pbar.finish()
  return saved

def scrape_cli():
  p = ArgumentParser()
  p.add_argument('sport', help='one of mlb or nba')
  p.add_argument("--min-date", default=None, help="First day to scrape (defaults to yesterday)")
  p.add_argument("--max-date", default=None, help="Last day to scrape (defaults to today)")
  cfg = p.parse_args()

  logging.basicConfig(filename="logs/scraperotoguru.log", level=logging.INFO)
  print 'Loading salaries for FanDuel games...'
  updated_games = update_salary_history(cfg.sport, cfg.min_date, cfg.max_date)
  print 'Saved salaries for %d FanDuel gamedays' % updated_games

