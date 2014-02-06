import pandas as pd
from epa_client import agg_by_ba
import os.path
import sys
import csv
from multiprocessing import Pool
from functools import partial

regions = {
    'all': ['al', 'az', 'ar', 'ca', 'co', 'ct', 'dc',
            'de', 'fl', 'ga', 'id', 'il', 'in', 'ia',
            'ks', 'ky', 'la', 'me', 'md', 'ma', 'mi',
            'mn', 'mi', 'mo', 'mt', 'ne', 'nv', 'nh',
            'nj', 'nm', 'ny', 'nc', 'nd', 'oh', 'ok',
            'or', 'pa', 'ri', 'sc', 'sd', 'tn', 'tx',
            'ut', 'vt', 'va', 'wa', 'wv', 'wi', 'wy'],
    }    
    
            
if __name__ == "__main__":
    # get cmdline args
    yr = sys.argv[1]
    
    # set up other data
    facility_df = pd.read_csv(os.path.join('data/original', 'facilities_balancingauthority.csv'))
    
    # set up pool
    nproc = 4
    p = Pool(nproc)
    
    # get list of dicts like df[st][ba]
    agg_data = p.map(partial(agg_by_ba, yr=yr, facility_df=facility_df),
                     regions['all'])
    
        
    # loop over all BAs with any facilities
    for ba in facility_df["BA_ABBREV"].unique():
        print ba
        
        # set up storage
        summed = None
        
        # collect sum over all states
        for d in agg_data:
            for this_st, st_data in d.iteritems():
                if ba in st_data.keys():
                    if summed is None:
                        summed = st_data[ba]
                    else:
                        summed += st_data[ba]
                else:
                    continue       
                
        # write
        if summed is not None:
            summed['BA_ABBREV'] = pd.Series(ba, index=summed.index)
            summed.to_csv('data/aggregated_epa/%s_%s_aggregate_raw.csv' % (yr, ba),
                            quoting=csv.QUOTE_NONNUMERIC)
 