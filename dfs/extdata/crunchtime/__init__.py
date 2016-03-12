import os
import requests
import datetime
import pandas

from dfs import GLOBAL_ROOT

data_dir = os.path.join(GLOBAL_ROOT, 'db/mlb/crunchtime')
list_url = 'http://crunchtimebaseball.com/master.csv'
latest_version_url = 'http://crunchtimebaseball.com/baseball_map.html'

def download_latest_mapping():
  list_csv = requests.get(list_url)
  assert list_csv.ok
  with open(os.path.join(data_dir, datetime.date.today().isoformat() + '.csv'), 'w') as outf:
    outf.write(list_csv.content)

def load_latest_mapping():
  files = filter(lambda f: f.endswith('.csv'), os.listdir(data_dir))
  latest_mapping = os.path.join(data_dir, sorted(files, reverse=True)[0])
  df = pandas.DataFrame.from_csv(latest_mapping, parse_dates=True)
  return df