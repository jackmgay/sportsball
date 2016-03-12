import pandas
import os

def combine_dataframe_into_pickle_file(dataframe, outfile, overwrite=False):
  """
  Save the provided pandas dataframe as a pickle to the provided file path. If a file is already present at that
  location, unpickle it, combine the dataframes, and save the result as a pickle (overwriting the file but keeping the
  data). Uses combine_first, prioritizing new data but keeping data from before.
  Obviously this will blow up catastrophically if there is a file at outfile which is not a DataFrame, and the data
  will get super gross if it *is* a DataFrame but the indices do not match.
  :param pandas.DataFrame dataframe: input dataframe
  :param str outfile: output file
  :return None:
  """
  if os.path.exists(outfile) and not overwrite:
    target_df = pandas.read_pickle(outfile)
    merged_df = dataframe.combine_first(target_df)
    merged_df.to_pickle(outfile)
  else:
    dataframe.to_pickle(outfile)

def combine_dataframe_into_csv(dataframe, outfile, date_cols=None, ignore_index=False):
  """
  Save the provided pandas dataframe as a csv to the provided file path. If a file is already present at that
  location, load it, combine the dataframes, and save the result as a csv (overwriting the file but keeping the
  data). Uses combine_first, prioritizing new data but keeping data from before.
  Obviously this will blow up if there is a file at outfile which is not a DataFrame, and the data
  will get super gross if it *is* a DataFrame but the indices do not match.

  This is superior to combine_dataframe_into_pickle_file in that it is more robust for long-term data serialization,
  and it makes the resulting data human-readable!
  It is going to be notably slower though, and it will lose information about indexing if you aren't careful.
  So, tradeoffs.

  :param pandas.DataFrame dataframe: input dataframe
  :param str outfile: output file
  :param list[str] date_cols: columns that should be parsed as dates
  :param bool ignore_index: does the index contain meaningful data? Important for determining overwrites
  :return None:
  """
  if os.path.exists(outfile):
    target_df = pandas.read_csv(outfile, parse_dates=date_cols, index_col=0)
    if ignore_index:
      merged_df = pandas.concat([target_df, dataframe], ignore_index=True)
    else:
      merged_df = dataframe.combine_first(target_df)
    merged_df.to_csv(outfile)
  else:
    dataframe.to_csv(outfile)