from argparse import ArgumentParser
import pandas as pd, numpy as np
from dfs.knapsack.knapsack import solve_all
import IPython

field_positions = {'C': 1, 'SS': 1, '1B': 1, '2B': 1, '3B': 1, 'OF': 3}

def build_teams(datafile, salary_col, position_col, prediction_col, num_pitchers, cap=35000,
                legal_teams=None):
  """
  Construct teams from a set of prediction data
  :param str datafile: saved prediction data (pickle file)
  :param str salary_col: name of salary column
  :param str position_col: name of position column
  :param str prediction_col: name of prediction column
  :param int num_pitchers: number of pitchers to built teams around
  :param list[str] legal_teams: an optional list of legal MLB teams for the game
  :return:
  """
  player_data = pd.read_pickle(datafile)
  player_data.set_index('player_id', inplace=True)

  if legal_teams:
    player_data = player_data[player_data['Team'].isin(legal_teams)]

  pitchers = player_data[player_data[position_col] == 'P']
  batters = player_data[player_data[position_col] != 'P']
  def get_team(playerids):
    return player_data.loc[playerids, [position_col, 'fullname', 'Team', salary_col, prediction_col]].sort(position_col)
  def print_team(playerids):
    print get_team(playerids)

  #pitchers['value'] = pitchers.apply(lambda row: row[prediction_col] / row[salary_col], axis=1)
  #pitchers.dropna(inplace=True, subset=['value', salary_col, position_col, prediction_col])
  best_pitchers = pitchers.sort(prediction_col, ascending=0).head(num_pitchers)

  cheapest_pitcher = best_pitchers.sort(salary_col, ascending=1)[salary_col].iloc[0]
  max_batter_sal = cap - cheapest_pitcher

  # the following drop is not inplace since batters is a slice
  batters = batters.dropna(subset=[salary_col, position_col, prediction_col])
  best_batters = solve_all(data=batters,
                           cost_column=salary_col,
                           value_column=prediction_col,
                           type_column=position_col,
                           required_types=field_positions,
                           cap=max_batter_sal)

  for pitcher_id, pitcher_row in best_pitchers.iterrows():
    remaining_salary = cap - pitcher_row[salary_col]
    other_players = best_batters[remaining_salary]
    team = pd.concat([get_team([pitcher_id]), get_team(other_players)])
    print "TOTAL PROJ %.2f with pitcher %s ($%d, PROJ %.2f):" % (team[prediction_col].sum(),
                                                                 team.loc[pitcher_id, 'fullname'],
                                                                 team.loc[pitcher_id, salary_col],
                                                                 team.loc[pitcher_id, prediction_col])
    print team



def build_teams_cli():
  p = ArgumentParser()
  p.add_argument("data", help="pickled dataframe containing players positions, salaries, and predictions")
  p.add_argument("--num-pitchers", type=int, default=3, help="how many starting pitchers to build teams around")
  p.add_argument("--salary", default="Salary", help="name of salary column")
  p.add_argument("--position", default="Position", help="name of position column")
  p.add_argument("--prediction", default="prediction", help="name of predicted column")
  p.add_argument("--teams", default=None, nargs='+', help="legal MLB teams for game")
  cfg = p.parse_args()

  build_teams(datafile=cfg.data,
              salary_col=cfg.salary,
              position_col=cfg.position,
              prediction_col=cfg.prediction,
              num_pitchers=cfg.num_pitchers,
              legal_teams=cfg.teams)

