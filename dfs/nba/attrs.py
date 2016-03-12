from argparse import ArgumentParser
import pandas as pd

def read_attrs(attrfilename):
  attrs = [line.strip() for line in open(attrfilename, 'r').readlines()]
  return attrs

def dumpattrs(datafilename, attrfilename):
  data = pd.read_pickle(datafilename)
  root_attr_names = data.columns
  with open(attrfilename, 'w') as outf:
    for attr in root_attr_names:
      outf.write(attr)
      outf.write('\n')

def dump_attrs_cli():
  p = ArgumentParser()
  p.add_argument("data", default="train.pickle", help="file to get attr names from")
  p.add_argument("attrs", default="attrs.txt", help="place to store default list of attrs")
  cfg = p.parse_args()

  dumpattrs(datafilename=cfg.data, attrfilename=cfg.attrs)