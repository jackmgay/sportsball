
from argparse import ArgumentParser
import pandas as pd
import numpy as np
from .attrs import read_attrs

DATE_COL = "date"

def strip_and_process_na(data, attrfile, na_strategy, include_target=True):
  attrs = read_attrs(attrfile)
  if not include_target:
    attrs = attrs[1:]
  # Keep only the attributes we care about as well as any relevant indexing columns
  relevant_columns = attrs
  if DATE_COL not in relevant_columns:
    relevant_columns.append(DATE_COL)
  # This day we can drop NA values without destroying all the data just b/c some irrelevant column is missing
  if na_strategy == 'drop':
    fixed_data = data.dropna(subset=relevant_columns)
  elif na_strategy == 'zero':
    only_updated_cols = data[relevant_columns]
    nonzero = only_updated_cols.fillna(value=0)
    fixed_data = data.copy()
    fixed_data.update(nonzero)
  else:
    raise NotImplementedError("invalid na_strategy")
  return fixed_data

def split_data(expanded_data, trainpct, split_randomly):
  train_example_count = int(len(expanded_data.index) * trainpct / 100.0)
  if split_randomly:
    train_indices = np.random.choice(expanded_data.index, size=train_example_count, replace=False)
  else:
    train_indices = expanded_data.sort(DATE_COL).index[:train_example_count]
  train_data = expanded_data.ix[train_indices]
  test_data = expanded_data.drop(train_indices)
  return train_data, test_data

def strip_and_process_to_files(expanded_file, stripped_file, attrfile, na_strategy, include_target):
  data = pd.read_pickle(expanded_file)
  stripped_data = strip_and_process_na(data=data,
                                       attrfile=attrfile,
                                       na_strategy=na_strategy,
                                       include_target=include_target)
  stripped_data.to_pickle(stripped_file)

def split_to_files(trainfile, testfile, stripped, trainpct, split_randomly):
  expanded_data = pd.read_pickle(stripped)
  train_data, test_data = split_data(expanded_data=expanded_data,
                                     trainpct=trainpct,
                                     split_randomly=split_randomly)
  train_data.to_pickle(trainfile)
  test_data.to_pickle(testfile)

def split_cli():
  p = ArgumentParser()
  p.add_argument("expanded", default="expanded.pickle", help="Expanded pickle file targets.")
  p.add_argument("stripped", default="test.pickle", help="stripped data filename")
  p.add_argument("train", default="train.pickle", help="training filename")
  p.add_argument("test", default="test.pickle", help="test filename")
  p.add_argument("attrfile", default="attrs.txt", help="attrs to care about for NA purposes")
  p.add_argument("--na-strategy", default="drop", help="what to do with NA rows (default is drop them)")
  p.add_argument("--trainpct", default=70, type=int, help="percentage of data to put into training set")
  p.add_argument("--random", action='store_true', help="split train/test sets randomly (default is by time)")
  cfg = p.parse_args()

  strip_and_process_to_files(expanded_file=pd.read_pickle(cfg.expanded),
                             stripped_file=cfg.stripped,
                             attrfile=cfg.attrfile,
                             na_strategy=cfg.na_strategy)
  split_to_files(trainfile=cfg.train,
                 testfile=cfg.test,
                 stripped=cfg.stripped,
                 trainpct=cfg.trainpct,
                 split_randomly=cfg.random)


