
from argparse import ArgumentParser
import pandas as pd

def read_attrs(attrfile):
  attrs = [line.strip() for line in open(attrfile, 'r').readlines()]
  return attrs

def dumpattrs(datafile, attrfile):
  data = pd.read_pickle(datafile)
  root_attr_names = data.columns
  with open(attrfile, 'w') as outf:
    for attr in root_attr_names:
      outf.write(attr)
      outf.write('\n')

def dump_attrs_cli():
  p = ArgumentParser()
  p.add_argument("data", default="train.pickle", help="file to get attr names from")
  p.add_argument("attrs", default="attrs.txt", help="place to store default list of attrs")
  cfg = p.parse_args()

  dumpattrs(datafile=cfg.data, attrfile=cfg.attrs)