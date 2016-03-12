import tempfile
import shutil
from unittest import TestCase
from mock import MagicMock, patch
from numpy import nan, isnan
from dfs.extdata.bbr import io, webio, scraper

class IOTestCase(TestCase):
  # Tests for local input & output functions. We'll want to use the patch exemplified here when creating structs
  # so that tests don't leave files around on the filesystem.
  def setUp(self):
    self.tmpdir = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.tmpdir)

  def test_get_player_filename(self):
    # lets make sure that this mock.patch business is working correctly.
    with patch('dfs.extdata.bbr.io.datadir', self.tmpdir):
      assert io.get_player_filename('foo').startswith(self.tmpdir)

  def test_create_player_dict(self):
    # basically just check that this makes the type of dict we want
    pids_to_load = {'pid1': 'url1', 'pid2': 'url2', 'pid3': 'url3'}
    pd = io.create_player_dict(pids_to_load)
    self.assertItemsEqual(pd, pids_to_load)  # make sure keys equal
    for pid in pids_to_load:
      self.assertItemsEqual(pd[pid], {'overview_url', 'overview_url_content', 'gamelog_url_list', 'gamelog_data', 'info'})

class WebIOTestCase(TestCase):
  # Test the stuff that actually hits the basketball reference site & parses data from it
  def test_get_gamelog_url(self):
    self.assertEqual(webio.get_gamelog_url('jamesle01', 2016),
                     'http://www.basketball-reference.com/players/j/jamesle01/gamelog/2016/')
    self.assertEqual(webio.get_gamelog_url('jamesle01', 2015),
                     'http://www.basketball-reference.com/players/j/jamesle01/gamelog/2015/')

  def test_get_active_players(self):
    # We added 'letters' as a param to get_active_players so we could only scrape 1/26 as much junk in testing
    self.maxDiff = None
    active_a_players = {
      'akognjo01': 'http://www.basketball-reference.com/players/a/akognjo01.html',
      'anthoca01': 'http://www.basketball-reference.com/players/a/anthoca01.html',
      'aldrila01': 'http://www.basketball-reference.com/players/a/aldrila01.html',
      'afflaar01': 'http://www.basketball-reference.com/players/a/afflaar01.html',
      'arthuda01': 'http://www.basketball-reference.com/players/a/arthuda01.html',
      'alabiso01': 'http://www.basketball-reference.com/players/a/alabiso01.html',
      'allento01': 'http://www.basketball-reference.com/players/a/allento01.html',
      'anderal01': 'http://www.basketball-reference.com/players/a/anderal01.html',
      'anderja01': 'http://www.basketball-reference.com/players/a/anderja01.html',
      'anderju01': 'http://www.basketball-reference.com/players/a/anderju01.html',
      'anderch01': 'http://www.basketball-reference.com/players/a/anderch01.html',
      'antetgi01': 'http://www.basketball-reference.com/players/a/antetgi01.html',
      'adrieje01': 'http://www.basketball-reference.com/players/a/adrieje01.html',
      'aldrico01': 'http://www.basketball-reference.com/players/a/aldrico01.html',
      'ayongu01': 'http://www.basketball-reference.com/players/a/ayongu01.html',
      'anderky01': 'http://www.basketball-reference.com/players/a/anderky01.html',
      'adamsst01': 'http://www.basketball-reference.com/players/a/adamsst01.html',
      'armsthi01': 'http://www.basketball-reference.com/players/a/armsthi01.html',
      'adamsjo01': 'http://www.basketball-reference.com/players/a/adamsjo01.html',
      'aldemfu01': 'http://www.basketball-reference.com/players/a/aldemfu01.html',
      'anthojo01': 'http://www.basketball-reference.com/players/a/anthojo01.html',
      'allenla01': 'http://www.basketball-reference.com/players/a/allenla01.html',
      'pendeje02': 'http://www.basketball-reference.com/players/p/pendeje02.html',
      'augusdj01': 'http://www.basketball-reference.com/players/a/augusdj01.html',
      'ajincal01': 'http://www.basketball-reference.com/players/a/ajincal01.html',
      'anticpe01': 'http://www.basketball-reference.com/players/a/anticpe01.html',
      'amundlo01': 'http://www.basketball-reference.com/players/a/amundlo01.html',
      'aminual01': 'http://www.basketball-reference.com/players/a/aminual01.html',
      'acyqu01': 'http://www.basketball-reference.com/players/a/acyqu01.html',
      'asikom01': 'http://www.basketball-reference.com/players/a/asikom01.html',
      'anderry01': 'http://www.basketball-reference.com/players/a/anderry01.html',
      'arizatr01': 'http://www.basketball-reference.com/players/a/arizatr01.html'}
    # This test will fail whenever some guy whose last name starts with 'a' enters the NBA
    # (or retires). Just update the dict above.
    self.assertDictEqual(active_a_players, webio.get_active_players(letters=['a']))

  def test_dfFromGameLogURL(self):
    # Tests that dfFromGameLogURL pulls the correct dataframe for a (player, year) without errors
    df = webio.dfFromGameLogURL(webio.get_gamelog_url('jamesle01', 2015))
    assert len(df) == 89
    self.assertListEqual([u'Rk', u'G', u'Age', u'Tm',
                          u'HomeAway', u'Opp', u'WinLoss', u'GS', u'MP',
                          u'FG', u'FGA', u'FG%', u'3P', u'3PA', u'3P%',
                          u'FT', u'FTA', u'FT%', u'ORB', u'DRB', u'TRB', u'AST', u'STL',
                          u'BLK', u'TOV', u'PF', u'PTS', u'GmSc', u'+/-'],
                         list(df.columns))
    self.assertDictEqual({u'+/-': 13,
                          u'3P': 1,
                          u'3P%': 0.25,
                          u'3PA': 4,
                          u'AST': 7,
                          u'Age': u'30-031',
                          u'BLK': 2,
                          u'DRB': 3,
                          u'FG': 7,
                          u'FG%': 0.43799999999999994,
                          u'FGA': 16,
                          u'FT': 4,
                          u'FT%': 0.66700000000000004,
                          u'FTA': 6,
                          u'G': 38,
                          u'GS': 1,
                          u'GmSc': 14.6,
                          u'HomeAway': nan,
                          u'MP': u'33:04',
                          u'ORB': 0,
                          u'Opp': u'SAC',
                          u'PF': 1,
                          u'PTS': 19,
                          u'Rk': 48,
                          u'STL': 4,
                          u'TOV': 6,
                          u'TRB': 3,
                          u'Tm': u'CLE',
                          u'WinLoss': u'W (+11)'},
                         dict(df.loc['2015-01-30']))

class ScraperTestCase(TestCase):
  def test_load_overview_pages(self):
    # Test that load_overview_pages hits the overview page and extracts the gamelog_url_list for each player
    pids_to_load = webio.get_active_players(letters=['a'])
    # Trim pids_to_load down to just 3 players to actually scrape (for test speed)
    pids_to_load = dict(list(pids_to_load.iteritems())[:3])
    # Make sure that worked right (no reordering problems)
    self.assertListEqual(['akognjo01', 'anthoca01', 'aldrila01'], list(pids_to_load.keys()))
    players = io.create_player_dict(pids_to_load)
    loaded_ov = scraper.load_overview_pages(players)
    self.assertListEqual(['akognjo01', 'anthoca01', 'aldrila01'], list(players.keys()))
    for pid, stuff in loaded_ov.iteritems():
      # Make sure we loaded the overview url content for each player
      assert 'overview_url_content' in stuff
      assert 'NBA Stats | Basketball-Reference.com' in stuff['overview_url_content']
    # Make sure we loaded the right number of gamelogs for each player
    assert len(players['akognjo01']['gamelog_url_list']) == 1
    assert len(players['anthoca01']['gamelog_url_list']) == 13
    assert len(players['aldrila01']['gamelog_url_list']) == 10
    for pid, stuff in loaded_ov.iteritems():
      for url in stuff['gamelog_url_list']:
        # not gonna get too in detail here since the URLs are grabbed by a regex anyway,
        # and we will test the parsed contents next
        assert url.startswith('http://www.basketball-reference.com/players/a/')

  def test_load_player(self):
    player_dict = io.create_player_dict({'jamesle01': ''})
    player_dict['jamesle01']['gamelog_url_list'] = ['http://www.basketball-reference.com/players/j/jamesle01/gamelog/2013/',
                                                    'http://www.basketball-reference.com/players/j/jamesle01/gamelog/2015/',
                                                    'http://www.basketball-reference.com/players/j/jamesle01/gamelog/2014/']
    loaded_dict = scraper.load_player(player_dict, 'jamesle01')
    assert loaded_dict['jamesle01']['gamelog_data'] is not None
    gd = loaded_dict['jamesle01']['gamelog_data']
    assert len(gd) == 285
    # Maybe more tests go here later

  def test_update_player(self):
    player_dict = io.create_player_dict({'jamesle01': ''})
    player_dict['jamesle01']['gamelog_url_list'] = ['http://www.basketball-reference.com/players/j/jamesle01/gamelog/2013/',
                                                    'http://www.basketball-reference.com/players/j/jamesle01/gamelog/2015/',
                                                    'http://www.basketball-reference.com/players/j/jamesle01/gamelog/2014/']
    loaded_dict = scraper.load_player(player_dict, 'jamesle01')
    assert loaded_dict['jamesle01']['gamelog_data'] is not None
    gd = loaded_dict['jamesle01']['gamelog_data']
    assert len(gd) == 285
    # Okay now pretend this URL was there all along as well
    player_dict['jamesle01']['gamelog_url_list'].append('http://www.basketball-reference.com/players/j/jamesle01/gamelog/2016/')
    scraper.update_player(player_dict, 'jamesle01', 2016)
    gd = loaded_dict['jamesle01']['gamelog_data']
    assert len(gd) > 285  # but I mean, I don't know exactly what it'll be since more games are still being played this year
    import datetime       # so explicitly make sure this test is updated for the 2016-17 season
    assert datetime.datetime.today() <= datetime.datetime(year=2016, month=7, day=1)
    # Spot check a game to make sure the stats are what we expect
    test_game_dict = dict(gd.loc['2015-10-30'])
    reference_dict = {u'+/-': 7.0,
                      u'3P': 0.0,
                      u'3P%': 0.0,
                      u'3PA': 2.0,
                      u'AST': 4.0,
                      u'Age': u'30-304',
                      u'BLK': 0.0,
                      u'DFS': 41.3,
                      u'DRB': 3.0,
                      u'Date': nan,
                      u'FG': 13.0,
                      u'FG%': 0.684,
                      u'FGA': 19.0,
                      u'FT%': 0.6,
                      u'FT': 3.0,
                      u'FTA': 5.0,
                      u'G': 3.0,
                      u'GS': 1.0,
                      u'GmSc': 21.0,
                      u'HomeAway': nan,
                      u'MP': u'33:56',
                      u'ORB': 2.0,
                      u'Opp': u'MIA',
                      u'PF': 3.0,
                      u'PTS': 29.0,
                      u'Rk': 3.0,
                      u'STL': 1.0,
                      u'TOV': 4.0,
                      u'TRB': 5.0,
                      u'Tm': u'CLE',
                      u'WinLoss': u'W (+10)'}
    self.assertItemsEqual(reference_dict.keys(), test_game_dict.keys())
    for k in reference_dict:
      # fortunately almost equal works fine if the items == each other so we can just pass in strings w/o worrying
      # unfortunately nan doesn't match :( :( so we might as well case it out anyway; nevermind
      if isinstance(reference_dict[k], float):
        if isnan(reference_dict[k]):
          assert isnan(test_game_dict[k])
        else:
          self.assertAlmostEqual(reference_dict[k], test_game_dict[k], places=3)
      else:
        self.assertEqual(reference_dict[k], test_game_dict[k])

