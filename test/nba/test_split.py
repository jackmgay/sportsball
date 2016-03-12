import pandas
import pkg_resources
from unittest import TestCase
import tempfile, shutil
import os
from dfs.nba.split import strip_and_process_na, split_data
from dfs.nba.attrs import read_attrs

class SplitTestCase(TestCase):
  def setUp(self):
    # Expanded recent test data -- will use this
    self.expandedfn = pkg_resources.resource_filename(__name__, 'expanded.pickle')
    self.expandedrecent = pandas.read_pickle(self.expandedfn)
    self.attrfile = pkg_resources.resource_filename(__name__, 'attrs.all.txt')
    self.realattrfile = pkg_resources.resource_filename(__name__, 'attrs.txt')
    self.attrcols = read_attrs(self.attrfile)
    self.realattrcols = read_attrs(self.realattrfile)


  def test_strip_and_process_na(self):
    # tell it all columns are important, it'll try to drop... basically all the rows :(
    naive_drop = strip_and_process_na(self.expandedrecent, attrfile=self.attrfile, na_strategy='drop')
    self.assertEqual(len(naive_drop), 0)
    not_na_counts = naive_drop.count()
    for i in range(len(not_na_counts)):
      self.assertEqual(not_na_counts.iloc[i], 0)
    # if we tell it we only care about a few recent-ish columns the set looks a lot better
    real_drop = strip_and_process_na(self.expandedrecent, attrfile=self.realattrfile, na_strategy='drop')
    # (importantly, as we only look at the columns we care about)
    real_drop = real_drop[self.realattrcols]
    self.assertEqual(len(real_drop), 5)
    not_na_counts = real_drop.count()
    for i in range(len(not_na_counts)):
      self.assertEqual(5, not_na_counts.iloc[i])
    # fill NA columns with zeros
    fill = strip_and_process_na(self.expandedrecent, attrfile=self.attrfile, na_strategy='zero')
    self.assertEqual(len(fill), 10)
    not_na_counts = fill.count()
    for i in range(len(fill)):
      self.assertEqual(not_na_counts.iloc[i], 10)

  def test_split_data(self):
    train, test = split_data(self.expandedrecent, trainpct=60, split_randomly=False)
    self.assertListEqual(['derozde01', 'ginobma01', 'diawbo01', 'hairspj02', 'rosste01', 'barnema02'],
                         list(train['bref_id']))
    self.assertListEqual(['thornma01', 'mccontj01', 'muhamsh01', 'lawsoty01'],
                         list(test['bref_id']))
    train, test = split_data(self.expandedrecent, trainpct=70, split_randomly=False)
    self.assertListEqual(['derozde01', 'ginobma01', 'diawbo01', 'hairspj02', 'rosste01', 'barnema02', 'thornma01'],
                         list(train['bref_id']))
    self.assertListEqual(['mccontj01', 'muhamsh01', 'lawsoty01'],
                         list(test['bref_id']))
    train, test = split_data(self.expandedrecent, trainpct=60, split_randomly=True)
    assert len(train) == 6
    assert len(test) == 4
    train, test = split_data(self.expandedrecent, trainpct=100, split_randomly=True)
    assert len(train) == 10
    assert len(test) == 0