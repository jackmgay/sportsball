
from dfs.nba.predict import get_eligible_players_df
from unittest import TestCase

class PredictDayTestCase(TestCase):
  def setUp(self):
    pass

  def test_get_eligible_players_df(self):
    players = get_eligible_players_df('2015-11-10', guess_historical=True)
    self.assertEqual(163, len(players))
    self.assertListEqual(['ibakase01',
                          'roberan03',
                          'bassbr01',
                          'nealga01',
                          'johnsja01',
                          'kaminfr01',
                          'dragigo01',
                          'brownan02',
                          'ajincal01',
                          'loveke01'],
                         list(players['bref_id'].values[:10]))
    # Make sure we have all the columns we need to get started
    self.assertItemsEqual(['bref_id', 'Tm', 'Opp', 'date', 'salary', 'pos'], list(players.columns))

