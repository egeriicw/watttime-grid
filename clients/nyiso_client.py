import os
import sys
import requests
import zipfile
import StringIO
import calendar
import pandas as pd

# path from ipynb to data
data_dir = 'data/isos/nyiso'

def download_historical_interchange(yr, mo):
    """download data and put in directory structure"""    
    # set up
    zipfname = '%s%s01ExternalLimitsFlows_csv.zip' % (yr, mo)
    url = os.path.join('http://mis.nyiso.com/public/csv/ExternalLimitsFlows/',
                       zipfname)
            
    # get
    response = requests.get(url)
    print response.url

    # unzip into target directory
    zipf = zipfile.ZipFile(StringIO.StringIO(response.content))
    zipf.extractall(path=data_dir)
    zipf.close()
    
def load_interchange(yrs, mos):
    # set up storage
    pieces = []
    
    # read all csvs
    for yr in yrs:
        for mo in mos:
            for dy in ['%02d' % i for i in range(1, calendar.monthrange(int(yr), int(mo))[1]+1)]:
                try:
                    csvf = os.path.join(data_dir, '%s%s%sExternalLimitsFlows.csv' % (yr, mo, dy))
                    mini_df = pd.read_csv(csvf, parse_dates=True)
                except IOError:
                    print '...skipping %s-%s-%s' % (yr, mo, dy)
                    continue

                try:
                    pivoted = mini_df.pivot(index='Timestamp', columns='Interface Name', values='Flow (MWH)')
                    pieces.append(pivoted)
                except pd.core.reshape.ReshapeError:
                    '...problem pivoting %s-%s-%s' % (yr, mo, dy)

    # collect pieces
    df = pd.concat(pieces)
    df.index = pd.to_datetime(df.index)
    return df
    
def net_hrly_interchange(df):
    # set up col labels
    col_names = {
        'NET_NY_PJM': ['SCH - PJ - NY', 'SCH - PJM_NEPTUNE', 'SCH - PJM_VFT'],
        'NET_NY_ONT': ['SCH - OH - NY'],
        'NET_NY_ISNE': ['SCH - NE - NY', 'SCH - NPX_CSC', 'SCH - NPX_1385'],
        'NET_NY_HQ': ['SCH - HQ - NY', 'SCH - HQ_CEDARS', 'SCH - HQ_IMPORT_EXPORT'],
    }
    
    # sum over cols
    net_dict = {}
    for new_col, old_cols in col_names.iteritems():
        for col in old_cols:
            try:
                net_dict[new_col] += df[col].fillna(0)
            except KeyError:
                net_dict[new_col] = df[col].fillna(0)
    net_df = pd.DataFrame(net_dict)
                
    # aggregate from 5-min MWh to hourly MWh
    hrly_df = net_df.resample('H', how='sum') * 5.0/60.0
    return hrly_df
    
if __name__ == "__main__":
    yrs = sys.argv[1].split(',')
    mos = ['%02d' % i for i in range(1,13)]
    
    # download all interchange data
  #  for yr in yrs:
  #      for mo in mos:
  #          download_historical_interchange(yr, mo)
    
    # aggregate over interchange
    raw_df = load_interchange(yrs, mos)
    hrly_df = net_hrly_interchange(raw_df)
    
    # write
    outf = os.path.join(data_dir, 'interchange_hourly.csv')
    print outf
    hrly_df.to_csv(outf, index_label='Date')