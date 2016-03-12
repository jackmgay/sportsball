
import tempfile
import shutil
import os
import pandas
import numpy as np
import datetime
import pkg_resources
from unittest import TestCase
from dfs.nba.featurizers import feature_generators
from dfs.nba.featurizers import fantasy_points_fzr, last5games_fzr, nf_stats_fzr, vegas_fzr, \
  opp_ffpg_fzr, salary_fzr

class FeaturizersTest(TestCase):
  def setUp(self):
    # A little test data from the past few years, useful for testing BREF data
    testfn = pkg_resources.resource_filename(__name__, 'test.pickle')
    self.data = pandas.read_pickle(testfn)
    # More recent test data -- necessary for testing external data
    recentfn = pkg_resources.resource_filename(__name__, 'recent.pickle')
    self.recentdata = pandas.read_pickle(recentfn)

  def testDataIntegrity(self):
    assert len(self.data) == 10
    assert self.data.iloc[0]['bref_id'] == 'gallola01'
    assert self.data.iloc[9]['bref_id'] == 'dunlemi02'
    assert len(self.recentdata) == 10
    assert self.recentdata.iloc[0]['bref_id'] == 'barnema02'
    assert self.recentdata.iloc[9]['bref_id'] == 'lawsoty01'

  def testDecorator(self):
    # Make sure the decorator is properly wrapping functions and turning their list outputs into pandas.Series
    for func_name in feature_generators:
      assert isinstance(func_name, basestring)
      wrapper, columns, live = feature_generators[func_name]
      output = wrapper(self.data.iloc[0])
      self.assertTrue(isinstance(output, pandas.Series))
      self.assertItemsEqual(columns, output.index)

  def applyFeaturizer(self, fzr_function, expected_output, use_recent=False):
    data = self.recentdata if use_recent else self.data
    for integer_index, (_, row) in enumerate(data.iterrows()):
      actual_output = fzr_function(row)
      for i in range(len(expected_output[integer_index])):
        # First check if they're both NaN
        if np.isnan(expected_output[integer_index][i]) and np.isnan(actual_output.iloc[i]):
          continue
        self.assertAlmostEqual(expected_output[integer_index][i],
                               actual_output.iloc[i],
                               places=3,
                               msg="Error in row %d item %d of %s. Reference %s, actual output %s." % (
                                 integer_index,
                                 i,
                                 'recentdata' if use_recent else 'data',
                                 expected_output[integer_index][i],
                                 actual_output.iloc[i]
                               ))

  def test_fantasy_points_fzr(self):
    self.applyFeaturizer(fantasy_points_fzr, [[20.1],
                                              [4.0],
                                              [17.3],
                                              [4.2],
                                              [22.5],
                                              [36.3],
                                              [27.9],
                                              [31.3],
                                              [17.8],
                                              [11.7]])

  def test_last5games_fzr(self):
    self.applyFeaturizer(last5games_fzr, [[25.1],
                                          [6.78],
                                          [18.78],
                                          [6.26],
                                          [19.24],
                                          [29.56],
                                          [30.74],
                                          [31.36],
                                          [13.94],
                                          [23.72]])

  def test_nf_stats_fzr(self):
    self.applyFeaturizer(nf_stats_fzr,
                         [[23.76,6.0,2.7,1.4,0.6,0.2,0.8,1.9,12.14],
                          [35.97,19.0,6.1,4.0,1.1,0.2,2.1,2.9,32.82],
                          [23.58,12.9,2.7,1.7,0.7,0.2,1.2,2.4,19.29],
                          [np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan],
                          [27.23,10.4,4.4,2.9,0.6,0.3,1.8,2.3,20.03],
                          [23.39,7.10,3.0,1.0,0.5,0.3,0.6,2.1,13.2],
                          [24.62,8.1,4.2,1.6,0.6,0.2,1.4,2.4,15.74],
                          [18.26,9.2,3.0,1.1,0.5,0.4,0.7,1.4,15.55],
                          [23.38,8.1,3.5,0.9,0.6,0.2,0.8,1.7,14.45],
                          [35.62,18.8,4,7.5,1.5,0.1,2.8,2.4,35.25]],
                         use_recent=True)

  def test_vegas_fzr(self):
    self.applyFeaturizer(vegas_fzr,
                         [[10.5, 189.5],
                          [6.5, 199.5],
                          [9.5, 194.5],
                          [4.5, 194.0],
                          [8.5, 195.5],
                          [-1, 190.5],
                          [-5, 198],
                          [2.5, 196.5],
                          [-19, 200.0],
                          [-9, 181.0]
                          ])
    self.applyFeaturizer(vegas_fzr,
                         [[9.0, 204.5],
                          [-6.0, 200.5],
                          [4.5, 217.5],
                          [-5.5, 202.5],
                          [-5.5, 202.5],
                          [2.0, 195],
                          [13.0, 195],
                          [-4.0, 203.5],
                          [-6.0, 200.5],
                          [4.5, 217.5]],
                         use_recent=True)

  def test_opp_ffpg_fzr(self):
    self.applyFeaturizer(opp_ffpg_fzr,
                         [[18.389285714285712, 48.0, 0.85816666666666663, 1.1187666671058538, 20.0],
                          [17.040909090909093, 67.2, 0.76771331058020498, 0.76122548332443785, 2.0055710306406684],
                          [20.261666666666667, 42.4, 0.85140009104385328, 0.80628334990429773, 1.5840597758405979],
                          [15.684848484848485, 35.3, 0.71887224832758501, 0.67037347774416234, 1.3499043977055449],
                          [20.426530612244896, 52.4, 0.83409491798497215, 0.81556700238463165, 1.9865319865319866],
                          [17.885365853658534, 51.8, 0.7638541666666665, 0.69248549436529994, 1.3061224489795917],
                          [18.26969696969697, 66.2, 0.83735141954375503, 0.89284459636178026, 10.105263157894738],
                          [19.694339622641515, 54.6, 0.86982125248260445, 0.80132994567677285, 1.7091633466135459],
                          [17.863636363636363, 46.4, 0.81874052383653018, 0.80001770931620431, 1.5218658892128281],
                          [16.608974358974361, 56.2, 0.77021403091557705, 0.7193626173392953, 1.3805774278215222]],
                         use_recent=False)

  def test_salary_fzr(self):
    self.applyFeaturizer(salary_fzr, [[3500],
                                      [8200],
                                      [3700],
                                      [np.nan],
                                      [4100],
                                      [3500],
                                      [3500],
                                      [4000],
                                      [3700],
                                      [7100]],
                         use_recent=True)