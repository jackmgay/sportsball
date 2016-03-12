from unittest import TestCase
from dfs.extdata.bsbr.scrape_bsbr import get_active_years

# should combine this with other files?

# just using testcase for assertitemsequal
class GetActiveYearsTest(TestCase):
  # test that get active years succesfully counts year-blank url as current year and doesn't dupe
  def setUp(self):
    self.url_list_1 = [u'http://www.baseball-reference.com/players/gl.cgi?id=webstal01&t=f&year=',
                       u'http://www.baseball-reference.com/players/gl.cgi?id=webstal01&t=f&year=2013',
                       u'http://www.baseball-reference.com/players/gl.cgi?id=webstal01&t=f&year=2014']

    self.url_list_2 = [ u'http://www.baseball-reference.com/players/gl.cgi?id=webstal01&t=f&year=2013',
                        u'http://www.baseball-reference.com/players/gl.cgi?id=webstal01&t=f&year=2014']

    self.url_list_3 = [u'http://www.baseball-reference.com/players/gl.cgi?id=webstal01&t=f&year=',
                       u'http://www.baseball-reference.com/players/gl.cgi?id=webstal01&t=f&year=2013',
                       u'http://www.baseball-reference.com/players/gl.cgi?id=webstal01&t=f&year=2014',
                       u'http://www.baseball-reference.com/players/gl.cgi?id=webstal01&t=f&year=2015',
                       ]

  def test_get_active_years(self):
    self.assertItemsEqual(['2013', '2014', '2015'], get_active_years(self.url_list_1))
    self.assertItemsEqual(['2013', '2014'], get_active_years(self.url_list_2))
    self.assertItemsEqual(['2013', '2014', '2015'], get_active_years(self.url_list_3))
