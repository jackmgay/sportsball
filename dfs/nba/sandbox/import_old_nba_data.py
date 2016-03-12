# Load old NBA prediction data from last year - before we had better matching
import simplejson as json
import os
import pandas as pd
from dfs.nba.playerid import name2nbaid, mapping_by_id
from dfs.extdata.numberfire.io import load_nf_histplayerinfo, save_nf_histplayerinfo
from progressbar import ProgressBar

old_dict = json.load(open('db/nba/old/matching/player_name_lookup.json'))

old_to_new = {}
pbar = ProgressBar()

for id, name in pbar(list(old_dict['id2bbr'].iteritems())):
  bref_id, confidence = name2nbaid(name, get_confidence=True)
  if confidence >= 80 and isinstance(bref_id, basestring):
    #print name, bref_id, mapping_by_id.loc[bref_id, 'name']
    old_to_new[id] = bref_id

data_to_save = {}

old_pred_dir = 'db/nba/old/prediction_data/numberfire/player_predictions'
old_files = os.listdir(old_pred_dir)
for filename in old_files:
  if filename in old_to_new:
    brid = old_to_new[filename]
    print filename, brid
    old_stuff = pd.read_pickle(os.path.join(old_pred_dir, filename))
    print old_stuff.tail(3)
    try:
      new_stuff = load_nf_histplayerinfo('nba', [brid])[brid]
    except KeyError as ex:
      continue
    print new_stuff.head(3)
    print '---'
    data_to_save[brid] = old_stuff

save_nf_histplayerinfo('nba', data_to_save)
