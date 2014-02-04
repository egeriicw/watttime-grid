from pandas.io import sql
import pandas as pd
import sqlite3
from epa_client import *
import sys
import time
import csv

regions = {
    'all': ['al', 'az', 'ar', 'ca', 'co', 'ct', 'dc',
            'de', 'fl', 'ga', 'id', 'il', 'in', 'ia',
            'ks', 'ky', 'la', 'me', 'md', 'ma', 'mi',
            'mn', 'mi', 'mo', 'mt', 'ne', 'nv', 'nh',
            'nj', 'nm', 'ny', 'nc', 'nd', 'oh', 'ok',
            'or', 'pa', 'ri', 'sc', 'sd', 'tn', 'tx',
            'ut', 'vt', 'va', 'wa', 'wv', 'wi', 'wy'],
    'some': ['ny', 'ma'],
    }    

def agg_epa(yr, islice1=0, islice2=50):
    
    facility_df = pd.read_csv(os.path.join('data/original', 'facilities_balancingauthority.csv'))
    
    for st in regions['all'][islice1:islice2]:
        load(yr, st)
        epa_df = data[yr][st]
        
        facilities_in_st_df = facility_df[facility_df['State']==st.upper()]
       # fac2ba_map = dict(set(zip(ba2fac_df[" Facility ID (ORISPL)"], ba2fac_df["BA_ABBREV"])))
        
        for ba in facilities_in_st_df["BA_ABBREV"].unique():
            print '...%s' % ba
            facilities_in_ba_df = facilities_in_st_df[facilities_in_st_df["BA_ABBREV"]==ba]
            facilities_in_ba = list(facilities_in_ba_df[" Facility ID (ORISPL)"].unique())
            
            df = epa_df[epa_df["ORISPL_CODE"].isin(facilities_in_ba)]
            hrly_agg = df.groupby(['OP_DATE', 'OP_HOUR']).aggregate(np.sum)[['GLOAD (MW)', 'CO2_MASS (tons)']]
            hrly_agg['BA_ABBREV'] = pd.Series(ba, index=hrly_agg.index)
            hrly_agg['STATE'] = pd.Series(st.upper(), index=hrly_agg.index)
            hrly_agg.to_csv('data/aggregated/%s_%s_%s_aggregate_raw.csv' % (yr, ba, st.upper()),
                            quoting=csv.QUOTE_NONNUMERIC)
                            
        data[yr].pop(st)            
            
if __name__ == "__main__":
    yr = sys.argv[1]
   # islice1 = int(sys.argv[2])
   # islice2 = islice1 + 7#sys.argv[2]
    agg_epa(yr)