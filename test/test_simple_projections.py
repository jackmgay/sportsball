from mock import MagicMock
import unittest
from dfs.nba.simple_projections import prepare_team_dataframes, simulate_player_stats
from dfs.extdata.bbr.gamelogs import load_gamelogs

class TestProjections(unittest.TestCase):
  def setUp(self):
    self.playerdata = load_gamelogs()
  def tearDown(self):
    pass
  def testSimPlayerStats(self):
    pass
