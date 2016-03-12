'''
Heuristics for the restricted knapsack problem.
'''

import pandas as pd
import numpy as np
from .common import get_gcd_cost

def best_vorp(data, cost_column, value_column, type_column, required_types, cap, debug_print_fn=None):
  # Mark the lowest-cost n players at each position as in the team
  grouped = data.groupby(type_column)
  parts = []
  for pos, df in grouped:
    df = df.sort(cost_column)
    its = pd.Series(False, index=df.index)
    for i in range(required_types[pos]):
      its.iloc[i] = True
    df['inteam'] = its
    parts.append(df)
  data = pd.concat(parts)
  solution = list(data[data['inteam'] == True].index.values)
  cap_so_far = data[data['inteam'] == True][cost_column].sum()

  # Function to determine how much value over the replacement level -- at this position --
  # is offered by each player? Per unit of additional money spent?
  # Assumes we have already filtered out dominated players
  # We also know that the min salary has the min cost at each position b/c of our dominated-player preprocessing
  # TODO: do that here, or copy it into commmon
  def calc_valper(possible_players):
    # There's a shortcut I'm trying here which may be bad later. We are only considering upgrading our cheapest player
    # at a given position to the next level. It might be best to upgrade the more expensive guy. This is fixable but
    # makes this function pretty complicated, I think? so lets get this working first.
    min_costs = possible_players[possible_players['inteam'] == True].groupby(type_column)[cost_column].min()
    min_values = possible_players[possible_players['inteam'] == True].groupby(type_column)[value_column].min()
    def reduced_value(row):
      return row[value_column] - min_values[row[type_column]]
    def reduced_cost(row):
      return row[cost_column] - min_costs[row[type_column]]
    possible_players['vorp'] = possible_players.apply(reduced_value, axis=1)
    possible_players['corp'] = possible_players.apply(reduced_cost, axis=1)
    # set value for replacements
    possible_players['valper'] = possible_players['vorp'].divide(possible_players['corp']).replace(np.inf, 0).replace(np.nan, 0)
    return possible_players

  # Function to remove all players who are worse options than all players selected at that position
  def remove_downgrades(possible_players):
    parts = []
    for ptype, options in possible_players.groupby(type_column):
      min_value = options[options['inteam'] == True][value_column].min()
      parts.append(options[options[value_column] >= min_value])
    reduced = pd.concat(parts)
    #print 'stripped out %d dominated options' % (len(possible_players) - len(reduced))
    return reduced

  #print 'initial valper:'
  #for ptype, options in data.groupby(type_column):
  #  print ptype
  #  print options[[cost_column, value_column, 'vorp', 'corp', 'valper', 'inteam']]

  # assemble list of our upgrade options
  remaining_candidates = calc_valper(data)

  # now upgrade where we get the best remaining value per cost -- until the only candidates remaining are our selected ones
  while len(remaining_candidates) > len(solution):
    # Remove strictly dominated options
    # Copy and sort list of remaining candidates
    remaining_candidates = remaining_candidates.sort('valper', ascending=False)
    # find the best-looking guy we haven't already selected
    ix = 0
    while remaining_candidates.iloc[ix]['inteam']:
      ix += 1
      continue
    new_upgrade = remaining_candidates.iloc[ix]
    # figure out which of the previous guys to replace
    try:
      old_candidates = [pid for pid in solution if remaining_candidates.loc[pid][type_column] == new_upgrade[type_column]]
    except ValueError as ex:
      import IPython
      IPython.embed()
    old_candidates.sort(key=lambda pid: remaining_candidates.loc[pid][value_column])
    worst = old_candidates[0]
    old_cost = remaining_candidates.loc[worst, cost_column]
    # can we afford to replace him with new_upgrade?
    if (new_upgrade[cost_column] - old_cost) > (cap - cap_so_far):
      #print 'Want to replace %s %s ($%d, %.2f) with %s ($%d, %.2f, %.4f valper) but %d/%d spent' % \
      #      (new_upgrade[type_column], worst, old_cost, remaining_candidates.loc[worst][value_column],
      #       new_upgrade.name, new_upgrade[cost_column], new_upgrade[value_column], new_upgrade['valper'],
      #       cap_so_far, cap)
      # remove this candidate from consideration
      remaining_candidates.drop(new_upgrade.name, inplace=True)
    else:
      # yes: update solution, cap_so_far.
      solution.remove(worst)
      cap_so_far -= old_cost
      cap_so_far += new_upgrade[cost_column]
      solution.append(new_upgrade.name)
      remaining_candidates.loc[new_upgrade.name, 'inteam'] = True
      #print 'Replaced %s %s ($%d, %.2f) with %s ($%d, %.2f, %.4f valper). Now %d/%d spent' % \
      #      (new_upgrade[type_column], worst, old_cost, remaining_candidates.loc[worst][value_column],
      #       new_upgrade.name, new_upgrade[cost_column], new_upgrade[value_column], new_upgrade['valper'],
      #       cap_so_far, cap)
      # remove the old candidate from consideration. technically suboptimal but... this is a heuristic anyway!
      remaining_candidates.drop(worst, inplace=True)
    remaining_candidates = remove_downgrades(remaining_candidates)
    #print remaining_candidates[[type_column, cost_column, value_column, 'vorp', 'corp', 'valper', 'inteam']]
    remaining_candidates = calc_valper(remaining_candidates)
    #print 'Recalculating valper:'
    #for ptype, options in remaining_candidates.groupby(type_column):
      #print ptype
      #print options[[cost_column, value_column, 'vorp', 'corp', 'valper', 'inteam']]
    #assert False
  #print remaining_candidates[[type_column, cost_column, value_column, 'inteam']]
  #print 'total value', remaining_candidates[value_column].sum()
  return list(remaining_candidates.index)