##########
# imports
##########

# this was run using pandas v0.11.0, scipy v0.12.0, numpy v1.8.0
import pandas as pd
import os
import epa_client
import numpy as np

######################
# read and parse data
######################

# path from ipynb to data
data_dir = '../data/water_data'
parsed_xls = None
    
def parse_water(yr):
    """read year from xls and return df"""
    # xlsfname = os.path.join(data_dir, 'EIA923_Schedule8_PartsA-F_EnvData_%s_FinalRelease.xlsx' % yr)
    # sheetname = '8D Cooling Operations'
    # cols = 'A:D,F:K,M:P'
    # df = pd.read_excel(xlsfname, sheetname,
    #                             skiprows=4, header=4, parse_cols=cols, na_values=['.'])
    csvfname = os.path.join(data_dir, 'EIA923_raw_%s.csv' % yr)
    df = pd.read_csv(csvfname, na_values=['.'])
    return df

def expand_water(yr, facility_df):
    """Join water and facility data"""
    # load water
    water_df = parse_water(yr)

    # merge with facilities
    selected_facility_df = facility_df[[' Facility ID (ORISPL)', ' Year', 'State', 'BA_ABBREV']]
    merged_df = pd.merge(water_df, selected_facility_df, how='inner',
                            left_on=['Plant ID', 'Year'],
                            right_on=[' Facility ID (ORISPL)', ' Year'])

    # drop unneeded columns and rows
    for col in ['Plant ID', ' Year']:
        del merged_df[col]
    cleaned_df = merged_df.drop_duplicates()
    cleaned_df.rename(columns={' Facility ID (ORISPL)': 'ORISPL_CODE'}, inplace=True)

    # return
    return cleaned_df    

def agg_by_ba(yr, ba, water_df):
    # get ORISPL codes for all facilities in this BA
    facilities_in_ba_df = water_df[water_df["BA_ABBREV"]==ba]
    facilities_in_ba = list(facilities_in_ba_df["ORISPL_CODE"].unique())

    # loop over states
    pieces = []
    for st in facilities_in_ba_df["State"].unique():
        # load data
        data = epa_client.load(yr, st)

        # collect power+carbon data for all facilities with those ORISPLs
        by_unit_df = data[yr][st][data[yr][st]["ORISPL_CODE"].isin(facilities_in_ba)]
        by_unit_df = by_unit_df.fillna(0)

        # expand date stamp into year month day
        by_unit_df['Year'] = by_unit_df.apply(lambda row: int(row['OP_DATE'].split('-')[2]), axis=1)
        by_unit_df['Month'] = by_unit_df.apply(lambda row: int(row['OP_DATE'].split('-')[1]), axis=1)
        by_unit_df['Day'] = by_unit_df.apply(lambda row: int(row['OP_DATE'].split('-')[0]), axis=1)

        # aggregate units into plants
        by_plant_df = by_unit_df.groupby(['Year', 'Month', 'Day', 'OP_HOUR', 'ORISPL_CODE']).aggregate(np.sum)[['GLOAD (MW)', 'CO2_MASS (tons)']]
        piece = by_plant_df.reset_index()

        # store
        pieces.append(piece)

    # collect pieces
    try:
        carbon_df = pd.concat(pieces)
    except Exception:
        return None

    # merge with water
    merged_df = pd.merge(carbon_df, water_df, how='inner')
    return merged_df