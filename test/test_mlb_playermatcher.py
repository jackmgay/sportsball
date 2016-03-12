from unittest import TestCase
import random
from dfs.mlb.playerid import *
from dfs.extdata.crunchtime import load_latest_mapping

random.seed(0)

class TestMatcher(TestCase):
  def setUp(self):
    mapping = load_latest_mapping()
    # Get all player names from Baseball Reference for testing
    self.player_names = filter(lambda x: len(x) > 0, mapping["bref_name"].fillna('').values)
    self.player_brefids = filter(lambda x: len(x) > 0, mapping["bref_id"].fillna('').values)


    self.mlb2name = {425877: 'Yadier Molina',
                     444432: 'Mark Trumbo',
                     474384: 'Nate Freiman',
                     516589: 'Andre Rienzo',
                     429665: 'Edwin Encarnacion'}
    self.name2pos = {
      'Mike Foltynewicz' : 'P',
      'Ross Detwiler' : 'P',
      'Buddy Boshers' : 'P',
      'Adrian Gonzalez' : '1B',
      'Chris Herrmann' : 'C',
      'Joel Peralta' : 'P',
      'Chia-Jen Lo' : 'P',
      'David Price' : 'P',
      'Ronny Cedeno' : 'SS',
      'Erisbel Arruebarrena' : 'SS'
    }

    self.duplicate_named_players = {'Jose Ramirez': [542432, 608070]}

  def tearDown(self):
    pass

  def test_name2position(self):
    for ref_name, ref_pos in self.name2pos.iteritems():
      assert name2position(ref_name) == ref_pos

  def test_name2mlbid(self):
    for ref_id, ref_name in self.mlb2name.iteritems():
      assert name2mlbid(ref_name) == ref_id

  def test_espnid2mlbid(self):
    assert espnid2mlbid(29769) == 451109
    assert espnid2mlbid(33637) == 592091
    assert espnid2mlbid(12459810384670134) is None

  def test_brefid2mlbid(self):
    assert brefid2mlbid('crowaa01') == 543070
    assert brefid2mlbid('cunniaa01') == 488811
    assert brefid2mlbid('sldglawlegawe') is None

  def test_playerispitcher(self):
    for brefid in self.player_brefids:
      result = brefid_is_pitcher(brefid)
      if type(result) != bool:
        print brefid
        print result
        assert False
