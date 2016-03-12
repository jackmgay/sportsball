
import tempfile
import shutil
import os
import pandas
import datetime
from unittest import TestCase
from dfs.nba.dumping import dump_nba_data

class DumpTestCase(TestCase):
  def setUp(self):
    self.tmpdir = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.tmpdir)

  def testDump(self):
    dumpfile = os.path.join(self.tmpdir, 'dump.pickle')
    dump_nba_data(dumpfile, start_date='2015-10-31', end_date='2015-11-3')
    df = pandas.read_pickle(dumpfile)
    self.assertEqual(len(df), 309)

  def testDumpSample(self):
    dumpfile = os.path.join(self.tmpdir, 'dump.pickle')
    dump_nba_data(dumpfile, start_date='2015-10-31', end_date='2015-11-3', max_count=25)
    df = pandas.read_pickle(dumpfile)
    self.assertEqual(len(df), 25)
    self.assertEqual(df.iloc[0]['bref_id'], 'marjabo01')
    self.assertEqual(df.iloc[0]['Opp'], 'NYK')

  def testDumpRandomSample(self):
    # We can still assert that this random sample starts with a particular player because
    dumpfile = os.path.join(self.tmpdir, 'dump.pickle')
    dump_nba_data(dumpfile, start_date='2015-10-31', end_date='2015-11-03', max_count=25, use_random=True)
    df = pandas.read_pickle(dumpfile)
    self.assertEqual(len(df), 25)
    self.assertEqual('robinth01', df.iloc[0]['bref_id'])
    self.assertEqual('MIL', df.iloc[0]['Opp'])