"""
Basic definitions used in multiple files for the scraper
"""
import os
import re
from dfs import GLOBAL_ROOT

# Location where all the basketball reference files should be saved
datadir = os.path.join(GLOBAL_ROOT, 'db/nba/')

# Regex for turning a basketball reference player overview URL into that player's bref ID.
bbr_id_regex = re.compile("http://www\.basketball-reference\.com/players/\w/(?P<pid>[\w\.\'\d]+)\.html")