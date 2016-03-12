
from argparse import ArgumentParser
import pandas as pd, numpy as np
from contexttimer import Timer

from .featurizers import feature_generators, distinct_nf_players_expanded

import logging
import progressbar
import warnings
import IPython

logger = logging.getLogger(__name__)
logger.addHandler(logging.FileHandler('./pipeline.log'))
logger.setLevel(logging.DEBUG)

def get_expansion_targets(expanding_live=False):
  for feature_name, (func, columns, live) in feature_generators.iteritems():
    if (expanding_live and live) or not expanding_live:
      yield feature_name, (func, columns)

def encode_names(feature_name, columns):
  return {col: (feature_name + ':' + col) for col in columns}

def expand_nba_data(infile_data, live=False):
  new_feature_dataframes = [infile_data]
  for feature_name, (func, columns) in get_expansion_targets(expanding_live=live):
    with Timer() as t:
      logger.info('Expanding %s (provides: %s)...', feature_name, ','.join(columns))
      # Apply featurizer to each row
      try:
        raw_series = [func(row) for index, row in infile_data.iterrows()]
      except Exception as ex:
        print 'Exception raised while expanding %s on' % feature_name, row
        raise ex
        IPython.embed()
      # Generate DataFrame by combining Series results of featurizer
      new_feature_data = pd.DataFrame(raw_series,
                                      index=infile_data.index)
      # Rename columns of new DataFrame to have full name of featurizer+data
      column_renaming_dict = encode_names(feature_name, columns)
      new_feature_data.rename(columns=column_renaming_dict, inplace=True)
      # and store result of this featurizer
      new_feature_dataframes.append(new_feature_data)
    logger.info('  took %d seconds', t.elapsed)
  # Stitch the expanded data together onto the input (dataframe of games played)
  expanded_data = pd.concat(new_feature_dataframes, axis=1)
  # After doing all that concatenation the index is super weird so just reset / nuke it
  expanded_data.reset_index(drop=True, inplace=True)
  return expanded_data

def discretize_data(expanded_data):
  # Transform categorical variables to indicator variables -- but only for expanded discrete columns.
  # May need to tweak how this list is generated in the future.
  categorical_cols = [c for c in expanded_data.columns if expanded_data[c].dtype.name == 'object']
  # Only discretize *expanded* columns! Stuff like player age or bref_id shouldn't be expanded
  categorical_expanded_cols = filter(lambda col: ':' in col, categorical_cols)
  expanded_discretized = pd.get_dummies(expanded_data, prefix_sep='=', columns=categorical_expanded_cols)
  return expanded_discretized

def expand_file_data(infile, outfile, live=False):
  infile_data = pd.read_pickle(infile)
  expanded = expand_nba_data(infile_data=infile_data, live=live)
  discretized = discretize_data(expanded)
  pd.to_pickle(discretized, outfile)
  return discretized

def expand_cli():
  p = ArgumentParser()
  p.add_argument("infile", default="dumped.pickle", help="Dumped player stats data.")
  p.add_argument("outfile", default="expanded.pickle", help="Expanded pickle file targets.")
  p.add_argument("--live", action='store_true', help="Use live expansion mode.")
  cfg = p.parse_args()

  outfile_data = expand_file_data(cfg.infile, cfg.outfile, live = cfg.live)

  print 'Expansion statistics:'
  print '  Expanded %d rows total.' % len(outfile_data)
  print '  Total features (attrvals) including targets:', len(outfile_data.columns)
  print '  Expanded Numberfire data for', len(distinct_nf_players_expanded), 'players.'