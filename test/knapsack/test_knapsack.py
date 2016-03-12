from unittest import TestCase
from dfs.knapsack.knapsack import solve_exact, _get_cheapest_satisfying_set, _get_gcd_cost, NotSolvableError
import pandas as pd

class KnapsackTest(TestCase):
  def setUp(self):
    self.df1 = pd.DataFrame([[1, 1, 'A'],
                             [2, 2, 'A'],
                             [1, 1, 'B'],
                             [3, 3, 'B'],
                             [1, 1, 'C'],
                             [4, 5, 'C'],],
                            columns=['cost', 'value', 'color'])
    self.df2 = pd.DataFrame([[1, 1, 'A'],
                             [1, 2, 'A'],
                             [1, 1, 'B'],
                             [1, 3, 'B'],
                             [1, 1, 'C'],
                             [2, 5, 'C'],],
                            columns=['cost', 'value', 'color'])
    self.gcd_df = pd.DataFrame([[10, 1, 'A'],
                                [20, 2, 'A'],
                                [10, 1, 'B'],
                                [30, 3, 'B'],
                                [10, 1, 'C'],
                                [40, 5, 'C'],],
                               columns=['cost', 'value', 'color'])

  def test_unsatisfiable_types(self):
    required_colors = {'A': 3}
    self.assertRaises(NotSolvableError, solve_exact, self.df1, 'cost', 'value', 'color', required_colors, 3)

  def test_cheapest_set(self):
    required_colors = {}
    self.assertItemsEqual(_get_cheapest_satisfying_set(self.df1, 'cost', 'value', 'color', required_colors),
      [])
    required_colors = {'A': 1}
    self.assertItemsEqual(_get_cheapest_satisfying_set(self.df1, 'cost', 'value', 'color', required_colors),
                          [0])
    required_colors = {'A': 2}
    self.assertItemsEqual(_get_cheapest_satisfying_set(self.df1, 'cost', 'value', 'color', required_colors),
                          [0, 1])
    required_colors = {'C': 2}
    self.assertItemsEqual(_get_cheapest_satisfying_set(self.df1, 'cost', 'value', 'color', required_colors),
                          [4, 5])
    required_colors = {'A': 1, 'B': 1, 'C': 1}
    self.assertItemsEqual(_get_cheapest_satisfying_set(self.df1, 'cost', 'value', 'color', required_colors),
                          [0, 2, 4])
    required_colors = {'A': 2, 'B': 2, 'C': 2}
    self.assertItemsEqual(_get_cheapest_satisfying_set(self.df1, 'cost', 'value', 'color', required_colors),
                          [0, 1, 2, 3, 4, 5])

    required_colors = {'A': 1, 'B': 1, 'C': 1}
    self.assertItemsEqual(_get_cheapest_satisfying_set(self.df2, 'cost', 'value', 'color', required_colors), [1, 3, 4])

  def test_get_gcd_cost(self):
    self.assertEquals(_get_gcd_cost(self.gcd_df['cost'], 50), 10)
    self.assertEquals(_get_gcd_cost(self.gcd_df['cost'], 55), 5)
    self.assertEquals(_get_gcd_cost(self.gcd_df['cost'], 56), 2)
    self.assertEquals(_get_gcd_cost(self.gcd_df['cost'], 51), 1)

  def test_increasing_space(self):
    required_colors = {'A': 1, 'B': 1, 'C': 1}
    self.assertItemsEqual(solve_exact(self.df1, 'cost', 'value', 'color', required_colors, 3), [0, 2, 4])
    self.assertItemsEqual(solve_exact(self.df1, 'cost', 'value', 'color', required_colors, 4), [1, 2, 4])
    self.assertItemsEqual(solve_exact(self.df1, 'cost', 'value', 'color', required_colors, 5), [0, 3, 4])
    self.assertItemsEqual(solve_exact(self.df1, 'cost', 'value', 'color', required_colors, 6), [0, 2, 5])
    self.assertItemsEqual(solve_exact(self.df1, 'cost', 'value', 'color', required_colors, 7), [1, 2, 5])
    self.assertItemsEqual(solve_exact(self.df1, 'cost', 'value', 'color', required_colors, 8), [0, 3, 5])
    self.assertItemsEqual(solve_exact(self.df1, 'cost', 'value', 'color', required_colors, 9), [1, 3, 5])
    self.assertItemsEqual(solve_exact(self.df1, 'cost', 'value', 'color', required_colors, 10), [1, 3, 5])

  def test_cheap_good_items(self):
    required_colors = {'A': 1, 'B': 1, 'C': 1}
    self.assertItemsEqual(solve_exact(self.df2, 'cost', 'value', 'color', required_colors, 4), [1, 3, 5])


