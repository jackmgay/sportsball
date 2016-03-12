"""
Run all pipeline scripts to build a new model.
You could do this with a shell script too -- perhaps better.
"""

import os
import datetime
import pandas as pd

from dfs import GLOBAL_ROOT

# prepare some simple strings in an inefficient manner
current_day = str(datetime.datetime.today().date())
yesterday = str(datetime.datetime.today().date() - datetime.timedelta(days=1))
week_ago = str(datetime.datetime.today().date() - datetime.timedelta(days=7))

# Scrape basketball reference for new stats
os.system("bbr-update-players")
# Scrape numberfire for new predictions
os.system("nba-scrape-nf")
# Scrape sbr for new odds (from the past week, for redundancy)
os.system("nba-scrape-sbr --min-date=%s" % week_ago)

# Scrape fanduel for upcoming games
os.system("fd-scrape-nba")

# Build a model using all available information
os.system("pl-nba {datadir} --start-date=2015-01-01 --end-date={today}".format(
    today=current_day,
    datadir=os.path.join(GLOBAL_ROOT, 'nbamodels', current_day),
))

# Build predictions for today's games using that information
os.system("pl-nba-live {datadir} {today} --send-email".format(
    today=current_day,
    datadir=os.path.join(GLOBAL_ROOT, 'nbamodels', current_day),
))