"""
Parse baseball reference team pages for park effects.
Cache
"""

import os
import simplejson as json
from . import team_disambiguations

from dfs import GLOBAL_ROOT
from dfs.extdata.common.scraper import getSoupFromURL, soupTableToTable, parsedTableToDF

url = 'http://www.baseball-reference.com/teams/{team}/attend.shtml'
cache_file = os.path.join(GLOBAL_ROOT, 'db/mlb/park_effects.json')
cached_effects = None

def _load_parkeffect_from_url(team):
  soup = getSoupFromURL(url.format(team=team))
  stats_table = soup.findAll(id='franchise_years')
  parsed_table = soupTableToTable(stats_table)
  header = []
  if not stats_table:
    print 'stats table not loaded for this team'
    import IPython
    IPython.embed()
    return None
  for th in stats_table[0].find("thead").findAll('th'):
    if not th.getText() in header:
      header.append(th.getText())
  df = parsedTableToDF(parsed_table, header, date_index=0) # Use "Rk" as index so we can talk about most recent year
  return df.loc[1, 'BPF'], df.loc[1, 'PPF']

def get_park_effects(team):
  ''' Get park adjustments for a team's park
  :param str team: team to get park effect data for
  :return tuple[str]: batter effect, pitcher effect
  '''
  global cached_effects

  if team in team_disambiguations:
    team = team_disambiguations[team]

  if not cached_effects:
    if os.path.exists(cache_file):
      cached_effects = json.load(open(cache_file, 'r'))
    else:
      cached_effects = {}
  if team in cached_effects:
    return cached_effects[team]
  else:
    effects = _load_parkeffect_from_url(team)
    cached_effects[team] = effects
    with open(cache_file, 'w') as outf:
      json.dump(cached_effects, outf)
    return effects