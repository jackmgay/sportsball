from dfs.nba.playerid import name2nbaid, team_tla, get_position

def test_name2nbaid():
  # TODO: test using team lookup here as well?
  good_matches = [("Ed Davis", "davised01"),
                  ("Nick Collison", "collini01"),
                  ("Serge Ibaka", "ibakase01"),
                  ("Tony Snell", "snellto01"),
                  ("Nikola Mirotic", "mirotni01"),
                  ("Maurice Harkless", "harklma01"),
                  ("Nemanja Bjelica", "bjeline01"),
                  ("Gordon Hayward", "haywago01"),
                  ("Steven Adams", "adamsst01"),
                  ("Pau Gasol", "gasolpa01"),
                  ("Brian Roberts", "roberbr01"),
                  ("Andre Miller", "millean02"),
                  ("Elijah Millsap", "millsel01"),
                  ("Anthony Morrow", "morroan01"),
                  ("Ricky Rubio", "rubiori01"),
                  ("Hassan Whiteside", "whiteha01"),
                  ("Kevin Durant", "duranke01"),
                  ("Bryce Cotton", "cottobr01"),
                  ("Dirk Nowitzki", "nowitdi01"),
                  ("Trey Lyles", "lylestr01")]

  for name, match in good_matches:
    assert name2nbaid(name) == match

def test_team_lookup():
  assert team_tla("LA Lakers") == "LAL"
  assert team_tla("Phoenix Suns") == "PHO"
  assert team_tla("BRK") == "BRK"
  assert team_tla("CLV") == "CLE"
  assert team_tla("gsw") == "GSW"
  assert team_tla("NOH") == "NOP"
  assert team_tla("SAC") == "SAS"

def test_player_position():
  assert 'SF' == get_position('jamesle01')
  assert 'PG' == get_position('curryst01')
  assert 'C' == get_position('duncati01')
  assert 'PF' == get_position('couside01')
  assert 'SG' == get_position('hardeja01')