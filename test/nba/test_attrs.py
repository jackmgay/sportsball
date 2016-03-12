from unittest import TestCase
import tempfile, shutil
import pandas
import os
from dfs.nba.attrs import read_attrs, dumpattrs

class DumpAttrsTestCase(TestCase):
  def setUp(self):
    self.tmpdir = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.tmpdir)

  def test_read_and_dump_attrs(self):
    stadium_series = pandas.Series(data=["Lambeau", "Levis", "Qwest"], name="Stadium")
    awesomeness_series = pandas.Series(data=[100, 30, 0], name="Awesomeness")
    df = pandas.DataFrame.from_dict({"Stadium": stadium_series, "Awesomeness": awesomeness_series})
    data_pickle = os.path.join(self.tmpdir, 'data.pickle')
    attrfile = os.path.join(self.tmpdir, 'attrs.txt')
    df.to_pickle(data_pickle)
    dumpattrs(datafilename=data_pickle, attrfilename=attrfile)
    attrs = read_attrs(attrfile)
    self.assertItemsEqual(attrs, ["Stadium", "Awesomeness"])