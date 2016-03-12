from fractions import gcd

def get_gcd_cost(costs, cap):
  return reduce(gcd, costs + [cap])