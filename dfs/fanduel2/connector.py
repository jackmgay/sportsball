import datetime
import re
import sys
import argparse
import selenium
import IPython

import os
from dfs import GLOBAL_ROOT, YOUR_EMAIL

from .browser_automation import get_browser, login, look_human, scrape_games

def get_creds():
  '''
  Reads a file to get our fanduel pass so that this is never accidentally
  committed to a repo somewhere.

  Add a file with the fanduel password to the directory specified in GLOBAL_ROOT.
  '''
  passwd = open(os.path.join(GLOBAL_ROOT, ".fanduelpass"), 'r').read().strip()
  return YOUR_EMAIL, passwd

def scrape_mlb():
  auth = get_creds()
  browser = get_browser(sport='mlb', debug=True)
  login(browser, auth=auth)
  look_human()
  look_human()
  scrape_games('mlb', browser)
  browser.close()

def scrape_nba():
  auth = get_creds()
  browser = get_browser(sport='nba', debug=True)
  login(browser, auth=auth)
  look_human()
  #switch_to_mobile(browser)
  look_human()
  #data = get_mlb_game_info(browser, debug=True)
  scrape_games(sport='nba', browser=browser)
  browser.close()