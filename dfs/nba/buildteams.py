from __future__ import print_function
from argparse import ArgumentParser
import pandas as pd, numpy as np

from collections import Counter

from dfs.nba.playerid import id2name
from dfs.knapsack.knapsack import solve_all
from dfs.knapsack.heuristics import best_vorp

MAX_PLAYERS_PER_TEAM = 3

import IPython

positions = {'PG': 2, 'SG': 2, 'SF': 2, 'PF': 2, 'C': 1}

def build_team(datafile, salary_col, position_col, prediction_col, cap=60000, legal_teams=None):
  """
  Construct teams from a set of prediction data
  :param str datafile: saved prediction data (pickle file)
  :param str salary_col: name of salary column
  :param str position_col: name of position column
  :param str prediction_col: name of prediction column to use
  :param list[str] legal_teams: an optional list of legal NBA teams for the game
  :return pd.DataFrame: prediction data for chosen team
  """
  player_data = pd.read_pickle(datafile)
  # Load real names for later use
  player_data['name'] = player_data['bref_id'].apply(id2name)

  if legal_teams:
    player_data = player_data[player_data['Tm'].isin(legal_teams)]

  # Ditch any undefined rows for salary / position / prediction as they will break the solver
  player_data.dropna(subset=[salary_col, position_col, prediction_col], inplace=True)
  # Cast player cost column to integers; this will also break the solver! :)
  player_data[salary_col] = player_data[salary_col].astype(int)

  # an optimization: speed up computation by only keeping the best-projected two players at each (position, salary).
  # this should mean we only keep players we could potentially use
  # it is hypothetically true that this could burn us if we get hit by the "too many players from team X" consideration
  #grouped_player_data = player_data.groupby([salary_col, position_col], sort=False)
  # this actually figures out how many players we need at the given position and keeps only that many at each salary level
  #candidates = grouped_player_data.apply(lambda group: group.sort(prediction_col).tail(positions[group[position_col].iloc[0]]))
  #

  # more detailed, even more aggressive sketchier optimization: remove all players which are strictly worse than others
  # (all players for whom two players are better and at least as cheap -- or one for centers. I hard coded that to save time)
  # this could burn us pretty hard if we run into a team constraint in the end
  def dominators(row):
    return len(player_data[(player_data['predicted'] > row['predicted'])
                           & (player_data['salary'] <= row['salary'])
                           & (player_data['pos'] == row['pos'])])
  player_data['dominators'] = player_data.apply(dominators, axis=1)
  candidates = player_data[(player_data['dominators'] == 0) |
                           ((player_data['pos'] != 'C') & (player_data['dominators'] <= 1))]
  candidates.set_index('bref_id', inplace=True)

  while True:   # because python doesn't have do... while
    best_team = best_vorp(data=candidates,
                          cost_column=salary_col,
                          value_column=prediction_col,
                          type_column=position_col,
                          required_types=positions,
                          cap=cap,
                          debug_print_fn=print)
    # Implement an additional constraint -- we can't have more than 4 players from the same team.
    # We'll actually be a little stricter and try to restrict it at 3 (see MAX_PLAYERS_PER_TEAM).
    teams_of_selection = Counter(candidates.loc[best_team, 'Tm'].values)
    most_common_team, count = teams_of_selection.popitem()
    if count <= MAX_PLAYERS_PER_TEAM:
      return candidates.loc[best_team]
    else:
      # Nope, this is an illegal team. Try to help us generate a real one by dropping the lowest-valued player
      # on the team from the list of possible candidates.
      players_on_most_common_team = [c for c in best_team if candidates.loc[c, 'Tm'] == most_common_team]
      team_players = candidates.loc[players_on_most_common_team].copy()
      team_players['value'] = team_players[prediction_col].divide(team_players[salary_col])
      team_players.sort('value', inplace=True)
      worst_player = team_players.iloc[0].name
      print('Ideal team had %d players from %s. Banning player: %s' % (count, most_common_team, worst_player))
      candidates = candidates.drop([worst_player])

def genteam_to_file(outfile, datafile, salary_col, position_col, prediction_col, cap=60000, legal_teams=None):
  best_team = build_team(datafile, salary_col, position_col, prediction_col, cap, legal_teams)
  best_team.to_pickle(outfile)

def build_teams_cli():
  p = ArgumentParser()
  p.add_argument("data", help="pickled dataframe containing players positions, salaries, and predictions")
  p.add_argument("outfile", help="output: pickled dataframe containing information for selected players")
  p.add_argument("--salary", default="salary", help="name of salary column")
  p.add_argument("--position", default="pos", help="name of position column")
  p.add_argument("--prediction", default="predicted", help="name of predicted column")
  p.add_argument("--cap", default=60000, help="salary cap amount")
  p.add_argument("--teams", default=None, nargs='+', help="legal NBA teams for game")
  cfg = p.parse_args()

  genteam_to_file(outfile=cfg.outfile,
                  datafile=cfg.data,
                  salary_col=cfg.salary,
                  position_col=cfg.position,
                  prediction_col=cfg.prediction,
                  cap=cfg.cap,
                  legal_teams=cfg.teams)

