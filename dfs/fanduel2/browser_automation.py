"""
Encapsulate browser-automation methods
"""
import IPython
import os, re
import time
import random
from parsedatetime import Calendar
import datetime

import selenium
from selenium import webdriver
from pyvirtualdisplay import Display
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.by import By

from .fanduel_parsing import parse_game_info, parse_contest_element, parse_lineup_page, parse_nba_player_list
from .io import add_game_info_to_db, root_data_dir, get_csv_file

USERAGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.104 Safari/537.36"
#USERAGENT = "Mozilla/5.0 (iPhone; CPU iPhone OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) CriOS/39.0.2171.45 Mobile/11D257 Safari/9537.53"
#USERAGENT = "Mozilla/5.0 (iPhone; CPU iPhone OS 8_4 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) CriOS/44.0.2403.67 Mobile/12H143 Safari/600.1.4"

def get_browser(sport, debug=False):
  """
  Use selenium and chromedriver to do our website getting.
  Might as well go all the way.
  :param debug: whether to set the browser to debug mode
  :param headless: go headless
  :return:
  """
  chrome_options = webdriver.ChromeOptions()
  chrome_options.add_argument('--user-agent=%s' % USERAGENT)
  webdriver.DesiredCapabilities.CHROME["userAgent"] = "ignore"
  prefs = {"download.default_directory" : root_data_dir.format(sport=sport)}
  chrome_options.add_experimental_option("prefs",prefs)
  browser = webdriver.Chrome(chrome_options=chrome_options)
  browser.implicitly_wait(10)  # wait up to 10s for an element if it's not instantly available
  return browser

def login(browser, auth):
  """
  Get browser logged into FanDuel
  :param webdriver.Chrome browser: mechanize Browser instance
  :return: None
  """
  browser.get('https://www.fanduel.com/p/login#login')
  time.sleep(5)
  browser.find_element(by=By.NAME, value="email").send_keys(auth[0])
  browser.find_element(by=By.NAME, value="password").send_keys(auth[1])
  browser.find_element(by=By.NAME, value="login").click() # hit login button
  return

def login_mobile(browser, auth):
  """
  Get browser logged into FanDuel if our useragent is mobile
  :param webdriver.Chrome browser: mechanize Browser instance
  :return: None
  """
  browser.get('https://www.fanduel.com/p/login#login')
  browser.find_elements(by=By.NAME, value="email")[1].send_keys(auth[0])
  browser.find_elements(by=By.NAME, value="password")[2].send_keys(auth[1])
  browser.find_element(by=By.NAME, value="login").click() # hit login button
  return

def switch_to_mobile(browser):
  mobile_site_link = browser.find_element_by_link_text("Mobile site")
  mobile_site_link.click()
  return

def select_sport_games(browser, sport):
  if sport == 'mlb':
    mlb_link = browser.find_element_by_link_text("MLB")
    mlb_link.click()
  elif sport == 'nba':
    nba_link = browser.find_element_by_link_text("NBA")
    nba_link.click()
  return

def select_5050_games(browser):
  l50 = browser.find_element_by_partial_link_text("50/50s & Multipliers")
  l50.click()
  return

def sort_by_ascending_fees(browser):
  """
  It will make me feel better to sort the entries in ascending order by fees so we click on the cheapest one here.
  :param selenium.webdriver.chrome.webdriver.WebDriver browser:
  :return:
  """
  el = browser.find_element_by_class_name("entry-fee-header")
  link = el.find_element_by_tag_name("a")
  while link.get_attribute('class') != 'sorted-asc':
    link.click()
  return

def scrape_games(sport, browser):
  """
  For each upcoming game start time, try to scrape at least one game from the 50/50 list
  Browser should start on homepage.
  :param webdriver.Chrome browser: mechanize Browser instance
  :return:
  """
  select_sport_games(browser, sport)
  select_5050_games(browser)

  # Figure out how many game starts there are.
  # We will need to grab these elements again each time since the elements expire.
  game_start_list = filter(lambda t: ':' in t.text, browser.find_elements_by_class_name('game-start-date'))
  game_starts_open_count = len(game_start_list)
  if not game_starts_open_count:
    print "Couldn't read start list of game times"
    IPython.embed()
  print 'Found %d distinct start times... attempting to scrape each.' % game_starts_open_count
  # Attempt to scrape one game for each of these
  for i in range(game_starts_open_count):
    # we have to refresh the elements to make sure we have up-to-date references (in case we came back to this page)
    game_start_list = filter(lambda t: ':' in t.text, browser.find_elements_by_class_name('game-start-date'))
    game_start_menu_element = game_start_list[i]
    print 'Parsing a game from start time category:', game_start_menu_element.text
    game_start_menu_element.click()
    look_human()
    sort_by_ascending_fees(browser)
    look_human()
    scrape_single_game(sport, browser)

def scrape_single_game(sport, browser, debug=False):
  # Since we are separating contests by their start time now, we don't currently try to choose a
  # best contest to scrape. We should certainly upgrade this in the future!
  contest_element_id_regex = re.compile(r"contest_(?P<gameid>\d+)-(?P<tableid>\d+)")
  browser_elem = browser.find_element_by_class_name('contest-list-item')
  match_groups = contest_element_id_regex.match(browser_elem.get_attribute("id"))
  fd_game_id = int(match_groups.group("gameid"))
  fd_table_id = int(match_groups.group("tableid"))
  fd_game_title = browser_elem.find_element_by_class_name("contest-name-text").text
  entry_fee = browser_elem.find_element_by_class_name("entry-fee-cell").text.replace('$','').replace(',','')
  game_time = browser_elem.find_element_by_class_name("startdate-cell").text
  cal = Calendar()
  new_parsed_dt, ret_code = cal.parseDT(game_time)

  # If it is between 9 PM and midnight locally (Pacific), we'll have an issue:
  # FD shows us the game time as e.g. "7 PM" but we'll interpret that as today.
  # Workaround: if the game time is in the past, its probably tomorrow. Add 1 day.
  # Break if it's not between 9PM and midnight so we know this happened
  if new_parsed_dt < datetime.datetime.now():
    if datetime.datetime.now().hour < 21:
      print 'Parsed time of FanDuel game start is in the past, but this isnt a TZ issue!'
      IPython.embed()
    else:
      new_parsed_dt += datetime.timedelta(days=1)
      assert new_parsed_dt > datetime.datetime.now()

  browser_elem.click()

  print 'Scraping this game:'
  print '  ', fd_game_title
  print '  on', new_parsed_dt.isoformat()
  game_entry_url = 'https://www.fanduel.com/games/{game_id}/contests/{game_id}-{table_id}/enter'.format(game_id=fd_game_id, table_id=fd_table_id)

  # Go to the details for the game, to find list of eligible players for this game.
  browser.get(game_entry_url)

  time.sleep(1)

  # Get the salary cap for the game directly
  salary_text = browser.find_element_by_xpath('//*[@id="ui-skeleton"]/div/section/div[2]/div[4]/div[2]/section/header/'
                                              'remaining-salary/div/div[1]/figure').text
  cap = int(salary_text.replace('$','').replace(',',''))

  # Download the master player / salary list
  player_list_link = browser.find_element_by_link_text("Download players list")
  # Sometimes this doesn't work the first time...

  download_filename = get_csv_file(sport, new_parsed_dt, fd_game_id)
  attempts = 1
  while not os.path.exists(download_filename) and attempts <= 10:
    print '...trying to download file (attempt %d)' % attempts
    time.sleep(3)
    player_list_link.click()
    time.sleep(3)
    attempts += 1
  if attempts > 5:
    print "Problem downloading player list -- not saving anything"
    return False
  else:
    print '...success!'

  # Get player lineups
  if sport == 'mlb':
    print "Accessing lineups..."
    lineups_link = browser.find_element_by_link_text("Lineup Info")
    # You can't just click it, it opens in a new tab :( So we visit it with get() and then go back()
    lineup_url = lineups_link.get_attribute('href')
    browser.get(lineup_url)
    lineup_info = parse_lineup_page(browser.page_source)
    browser.back()
    print "  %d / %d lineups submitted (%d players)" % (len(lineup_info.loaded_teams),
                                                        len(lineup_info.loaded_teams) + len(lineup_info.unloaded_teams),
                                                        len(lineup_info.parsed_players))
  else:
    # Should consider grabbing injury / GTD status here
    lineup_info = None
    # Except that doesn't work too well
    # parse_nba_player_list(browser.page_source)


  add_game_info_to_db(sport,
                      fd_game_id,
                      cap,
                      entry_fee,
                      new_parsed_dt,
                      fd_table_id,
                      game_title=fd_game_title,
                      lineup=lineup_info)
  # Return to the main lobby page with back()
  browser.back()
  return True

def look_human():
  print 'looking human'
  time.sleep(random.normalvariate(0.8, .2))
