
import datetime
import pandas as pd
import re
from dfs.extdata.common.scraper import getSoupFromURL
from dfs.mlb.playerid import name2brefid

url = 'http://scores.nbcsports.msnbc.com/mlb/stats.asp?file=probables'

_first_part_re = '(?P<team>\w+) - (?P<starter>\w. [\w ]+)\s*(\*\s*)?'
_second_part_re = '(?P<record>\(\d+\-\d+, (\d+\.\d+)?\-?\))'

pitcher_re = re.compile(_first_part_re + _second_part_re)

nbcteam2mlbteam = {
 'ARI': 'ARI',
 'ATL': 'ATL',
 'BAL': 'BAL',
 'BOS': 'BOS',
 'CHC': 'CHC',
 'CIN': 'CIN',
 'CLE': 'CLE',
 'COL': 'COL',
 'CWS': 'CWS',
 'DET': 'DET',
 'HOU': 'HOU',
 'KAN': 'KC',
 'LAA': 'LAA',
 'LAD': 'LAD',
 'MIA': 'MIA',
 'MIL': 'MIL',
 'MIN': 'MIN',
 'NYM': 'NYM',
 'NYY': 'NYY',
 'OAK': 'OAK',
 'PHI': 'PHI',
 'PIT': 'PIT',
 'SEA': 'SEA',
 'SD': 'SD',
 'SFG': 'SF',
 'STL': 'STL',
 'TB': 'TB',
 'TEX': 'TEX',
 'TOR': 'TOR',
 'WAS': 'WSH'}

def fix_team(team):
  return nbcteam2mlbteam.get(team, team)

def parse_game_row(game_row):
  'NYM - N. Syndergaard (4-5, 3.05)'
  home_cell = game_row.find('td', {'class': 'shsNamD shsProbHome'})
  away_cell = game_row.find('td', {'class': 'shsNamD shsProbAway'})
  home_match = pitcher_re.search(home_cell.text)
  away_match = pitcher_re.search(away_cell.text)
  if not home_match or not away_match:
    print 'Problem with RE matching!'
    import IPython
    IPython.embed()
  home_groups = home_match.groupdict()
  away_groups = away_match.groupdict()
  home_team = fix_team(home_groups['team'])
  away_team = fix_team(away_groups['team'])
  # Return teams, projected starters, @ if away, and opponents
  team_rows = [[home_team, home_groups['starter'], None, away_team],
               [away_team, away_groups['starter'], '@', home_team]]
  return team_rows

def get_nbc_starting_pitchers(game_day):
  # TODO(jgershen): we should handle doubleheaders more gracefully here
  soup = getSoupFromURL(url)
  main_div = soup.find('div', id='shsMLBprobables')
  main_div_children = list(main_div.children)
  game_day_header_re = game_day.strftime('%b\. %-d, %Y')
  for i, child in enumerate(main_div_children):
    if hasattr(child, 'text') and re.search(game_day_header_re, child.text):
      break
  else:
    print "Couldn't find stats for " + game_day.strftime('%b. %-d, %Y')
    import IPython
    IPython.embed()
    return None

  actual_div = main_div_children[i+1]
  game_rows = actual_div.findAll('tr', {'class': "shsRow0Row"}) + actual_div.findAll('tr', {'class': "shsRow1Row"})
  teams = []
  for row in game_rows:
    teams += parse_game_row(row)
  team_df = pd.DataFrame(teams, columns=['Tm', 'starter', 'HomeAway', 'Opp'])

  # Scraped it succesfully. Now, map the names/teams of the scraped pitchers to bref ID's
  team_df['starter_bref_id'] = team_df.apply(lambda team_row: name2brefid(team_row['starter'], team_row['Tm']),
                                             axis=1)
  team_df.set_index(['Tm'], inplace=True)
  return team_df
