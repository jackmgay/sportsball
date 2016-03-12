'''
Restricted knapsack problem solver.
'''
from copy import copy
from .common import get_gcd_cost

class NotSolvableError(ValueError):
  pass

def _get_cheapest_satisfying_set(data, cost_column, value_column, type_column, required_types):
  """
  Get the base-case: cheapest possible satisfying set for the inputs.
  If multiple possible sets of entries can be made with the same minimum cost, selects the maximum-value set
  of those possible sets.
  :param pandas.DataFrame data: items (indexed by unique ID) and data about them
  :param str cost_column: column of data containing cost
  :param str value_column: column of data containing value
  :param str type_column: column of data containing the type of item (e.g. player position)
  :param dict[str, int] required_types: which types we need and how many
  :return:
  """
  selected = []
  entries_by_type = data.groupby(type_column)
  for entry_type, entries in entries_by_type:
    required_entries_of_type = required_types.get(entry_type, 0)
    if len(entries) < required_entries_of_type:
      raise NotSolvableError("Not enough entries of type %s (required: %d, present: %d)" %
                             (entry_type, required_entries_of_type, len(entries)))
    elif required_entries_of_type > 0:
      cheapest = entries.sort([cost_column, value_column], ascending=[1, 0]).head(required_entries_of_type)
      selected += list(cheapest.index.values)
  return selected

def solve_exact(data, cost_column, value_column, type_column, required_types, cap, debug_print_fn=None):
  """
  Find the best solution to restricted knapsack problem for cost == cap.
  In this restricted problem, each item has a type, and we are given a certain count of each type of
  item. We must include exactly that count of the type of item in the solution.
  :param pandas.DataFrame data: items (indexed by unique ID) and data about them
  :param str cost_column: column of data containing cost. Costs must be ints!
  :param str value_column: column of data containing value. Value must be numeric!
  :param str type_column: column of data containing the type of item (e.g. player position)
  :param dict[str, int] required_types: which types we must use, and how many of each
  :param int cap: maximum total cost
  :param function debug_print_fn: used to print a candidate solution when debugging
  :return list: indices of included items (players)
  """
  return solve_all(data, cost_column, value_column, type_column, required_types, cap, debug_print_fn=None)[cap]

def solve_all(data, cost_column, value_column, type_column, required_types, cap, debug_print_fn=None):
  """
  Find all good solutions to restricted knapsack problem for costs <= cap.
  In this restricted problem, each item has a type, and we are given a certain count of each type of
  item. We must include exactly that count of the type of item in the solution.
  :param pandas.DataFrame data: items (indexed by unique ID) and data about them
  :param str cost_column: column of data containing cost. Costs must be ints!
  :param str value_column: column of data containing value. Value must be numeric!
  :param str type_column: column of data containing the type of item (e.g. player position)
  :param dict[str, int] required_types: which types we must use, and how many of each
  :param int cap: maximum total cost
  :param function debug_print_fn: used to print a candidate solution when debugging
  :return list: indices of included items (players)
  """
  gcd_cost = get_gcd_cost(data[cost_column].values, cap)
  data_cpy = data.copy()
  data_cpy[cost_column] = data_cpy[cost_column] / gcd_cost
  reduced_cap = cap / gcd_cost
  def solution_cost(solution):
    return int(data_cpy.loc[solution, cost_column].sum())
  def solution_value(solution):
    return data_cpy.loc[solution, value_column].sum()

  cheapest_set = _get_cheapest_satisfying_set(data_cpy, cost_column, value_column, type_column, required_types)
  cheapest = solution_cost(cheapest_set)

  entries_by_type = {etype: data_cpy[data_cpy[type_column] == etype] for etype in set(data_cpy[type_column].values)}

  best_solutions = {cheapest: cheapest_set}
  for current_cost in range(cheapest + 1, reduced_cap + 1):
    best_value = solution_value(cheapest_set)
    best_soln = best_solutions.get(current_cost - 1, cheapest_set)
    tried = set()
    # Look through all previous solutions. The best solution must be some previous solution, but (possibly) with
    # some single entry replaced by an entry of the same type
    for previous_solution_cost, previous_soln in best_solutions.iteritems():
      if str(previous_soln) in tried:
        continue
      tried.add(str(previous_soln))
      for entry in previous_soln:
        soln_base = copy(previous_soln)
        soln_base.remove(entry)
        entry_type = data_cpy.loc[entry, type_column]
        remaining_money = current_cost - solution_cost(soln_base)
        # Consider not filtering in the following line. I don't know that it speeds anything up.
        try:
          alternative_entries = entries_by_type[entry_type][entries_by_type[entry_type][cost_column] <= remaining_money]
        except TypeError:
          print 'type error'
          import IPython
          IPython.embed()
        for ae in alternative_entries.index:
          if ae in soln_base:
            continue
          candidate_soln = soln_base + [ae]
          #print 'candidate soln costs:', solution_cost(candidate_soln), 'value:', solution_value(candidate_soln)
          if solution_cost(candidate_soln) <= current_cost and solution_value(candidate_soln) > best_value:
            best_soln = candidate_soln
            best_value = solution_value(candidate_soln)
    if debug_print_fn:
      print 'best solution for $%d has cost %d, value:' % (current_cost, solution_cost(best_soln)), best_value
      debug_print_fn(best_soln)
    best_solutions[current_cost] = best_soln

  # Undo cap reduction
  real_solutions = {reduced * gcd_cost: soln for reduced, soln in best_solutions.iteritems()}
  return real_solutions