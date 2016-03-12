
from dfs.extdata.bsbr.scrape_bsbr import _parse_bsbr_prefix_section
import bs4


pre_html = '''
<html>
<pre>
<b><a href="/players/a/aardsda01.shtml">David Aardsma</a>                     2004-2015</b>
<a href="/players/a/aaronha01.shtml">Hank Aaron</a>+                       1954-1976
<a href="/players/a/fakeguy.shtml">Fake Aaron</a>+                       1954-2015
<a href="/players/a/aaronto01.shtml">Tommie Aaron</a>                      1962-1971
</pre>
</html>
'''

def test_parse_prefix_section():
  soup = bs4.BeautifulSoup(pre_html)
  pre_sections = soup.findAll('pre')
  players = list(_parse_bsbr_prefix_section(pre_sections[0]))
  assert len(players) == 2
  assert players[0][0] == 'aardsda01'
  assert players[0][1] == 'http://www.baseball-reference.com/players/a/aardsda01.shtml'
  assert players[1][0] == 'fakeguy'