"""
Sometimes everything gets messed up and you need to haul trades out of your BSBR data
"""
import IPython
from dfs.extdata.bsbr.scrape_bsbr import load_full_gamelogs, save_dataframes

gamelogs = load_full_gamelogs()
for player, gl_dict in gamelogs.iteritems():
  for dict_type in ['batting', 'pitching']:
    df = gl_dict.get(dict_type)
    if df is not None:
      reduced_df = df[~df['Tm'].str.startswith("Player went")]
      if len(reduced_df) < len(df):
        print 'Dropped %d rows from %s [%s]' % (len(df) - len(reduced_df), player, dict_type)
        gl_dict[dict_type] = reduced_df
save_dataframes(gamelogs, overwrite=True)
