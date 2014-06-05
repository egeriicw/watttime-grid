import pandas as pd
from clients import epa_form923_client
import os.path
import sys
import csv
import time
from multiprocessing import Pool
from functools import partial


def process_ba(ba, start):
    print ba, 'start after', round((time.clock() - start)/60.0, 1), 'min'

    # get df with full dataset for BA
    ba_df = epa_form923_client.agg_by_ba(yr=yr, ba=ba, water_df=water_df)

    if ba_df:
        # write to file
        outfname = os.path.join(data_dir, 'water_data/hourly_by_coolingsystem_and_ba',
                               '%s_%s_hourly_by_coolingsystem_and_ba.csv' % (yr, ba))
        ba_df.to_csv(outfname, index_label='RowID', quoting=csv.QUOTE_NONNUMERIC)

    return True

if __name__ == "__main__":
    # get cmdline args
    yr = sys.argv[1]
    try:
        data_dir = sys.argv[2]
    except IndexError:
        data_dir = '../data'

    # set up other data
    facility_df = pd.read_csv(os.path.join(data_dir, 'facilities&balancingauthority.csv'))
    water_df = epa_form923_client.expand_water(yr, facility_df)
    ba_names = facility_df["BA_ABBREV"].unique()
    print 'going to process %d BAs: %s' % (len(ba_names), ba_names)

    # set up pool
    nproc = 4
    p = Pool(nproc)

    start = time.clock()

    # loop over all BAs with any facilities
    p.map(partial(process_ba, start=start),
          ba_names)
