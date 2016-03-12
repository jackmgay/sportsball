
from argparse import ArgumentParser
import pandas as pd
import numpy as np
from .attrs import read_attrs

def strip_and_process_na(data, attrfile, na_strategy):
  attrs = read_attrs(attrfile)
  # Keep only the attributes we care about as well as any relevant indexing columns
  relevant_data = data[attrs + ["Date"]]
  # This day we can drop NA values without destroying all the data just b/c some irrelevant column is missing
  if na_strategy == 'drop':
    fixed_data = relevant_data.dropna()
  elif na_strategy == 'zero':
    fixed_data = relevant_data.fillna(value=0)
  else:
    raise NotImplementedError("invalid na_strategy")
  return fixed_data

def split_data(infile, train, test, attrfile, na_strategy, trainpct, split_randomly):
  expanded_data = strip_and_process_na(pd.read_pickle(infile), attrfile, na_strategy)
  train_example_count = int(len(expanded_data.index) * trainpct / 100.0)
  if split_randomly:
    train_indices = np.random.choice(expanded_data.index, size=train_example_count)
  else:
    train_indices = expanded_data.sort("Date").index[:train_example_count]
  train_data = expanded_data.ix[train_indices]
  test_data = expanded_data.drop(train_indices)

  pd.to_pickle(train_data, train)
  pd.to_pickle(test_data, test)

def split_cli():
  p = ArgumentParser()
  p.add_argument("expanded", default="expanded.pickle", help="Expanded pickle file targets.")
  p.add_argument("train", default="train.pickle", help="training filename")
  p.add_argument("test", default="test.pickle", help="test filename")
  p.add_argument("attrfile", default="attrs.txt", help="attrs to care about for NA purposes")
  p.add_argument("--na-strategy", default="drop", help="what to do with NA rows (default is drop them)")
  p.add_argument("--trainpct", default=70, type=int, help="percentage of data to put into training set")
  p.add_argument("--random", action='store_true', help="split train/test sets randomly (default is by time)")
  cfg = p.parse_args()

  split_data(infile=cfg.expanded, train=cfg.train, test=cfg.test,
             attrfile=cfg.attrfile,
             na_strategy=cfg.na_strategy, trainpct=cfg.trainpct, split_randomly=cfg.random)