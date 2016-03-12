
import simplejson as json
from simplejson.scanner import JSONDecodeError
import requests
import pickle
import logging
import os

from dfs import GLOBAL_ROOT

nf_data_dir = os.path.join(GLOBAL_ROOT, 'db/{sport}/prediction_data/numberfire/')
nba_matcher = None

def available_nf_files(sport):
  dir_listing = os.listdir(os.path.join(nf_data_dir.format(sport=sport), 'player_predictions'))
  if '.DS_Store' in dir_listing:
    dir_listing.remove('.DS_Store')
  return dir_listing

def available_salary_files(sport):
  dir_listing = os.listdir(os.path.join(nf_data_dir.format(sport=sport), 'salaries'))
  if '.DS_Store' in dir_listing:
    dir_listing.remove('.DS_Store')
  return dir_listing

def get_overview_file_dir(sport):
  return os.path.join(nf_data_dir.format(sport=sport), 'overview')

def get_histplayerinfo_filename(sport, identifier):
  target_file = os.path.join(nf_data_dir.format(sport=sport), 'player_predictions', str(identifier))
  return target_file

def get_salary_filename(sport, identifier):
  target_file = os.path.join(nf_data_dir.format(sport=sport), 'salaries', str(identifier))
  return target_file

def parse_terminated_json(json_str):
  """
  Attempt to parse a truncated JSON list-of-dicts... cut off in the middle of a string value (??)
  HAHA OMG YOU CLOWN. This is because our regex didn't capture all the JSON because someone put
  a semicolon in the middle. You can't parse JSON with regexes, because JSON is context-free.
  :param json_str:
  :return str: parsed list - our best guess
  """
  try:
    parsed = json.loads(json_str)
    return parsed
  except JSONDecodeError as ex:
    if "Unterminated string" not in ex.message:
      raise
    json_str += '"'
  parsed = False
  while not parsed:
    try:
      parsed = json.loads(json_str)
      return parsed
    except JSONDecodeError as ex:
      if "Expecting ',' delimiter or '}'" in ex.message:
        json_str += '}'
      elif "Expecting ',' delimiter or ']'" in ex.message:
        json_str += ']'
      else:
        raise
      logging.debug('...reparsing JSON with %s added...', json_str[-1])
      parsed = False
  return parsed

def pickle_cache_html_helper(url, cached_page=None, cache_target=None):
  if not cached_page:
    response = requests.get(url)
    page = response.text
  else:
    with open(cached_page, 'r') as inf:
      page = pickle.load(inf)
  if cache_target:
    with open(cache_target, 'w') as outf:
      pickle.dump(page, outf)
  return page