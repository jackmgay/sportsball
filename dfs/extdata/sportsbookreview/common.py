

def moneyline_to_implied_odds(moneyline):
  if moneyline < 0:
    return (-1.0 * moneyline) / ((-moneyline) + 100.0)
  else:
    return 100.0 / (moneyline + 100.0)