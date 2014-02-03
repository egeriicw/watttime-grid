##########
# imports
##########

# this was run using pandas v0.11.0, scipy v0.12.0, numpy v1.8.0
import pandas as pd
from scipy import stats
import numpy as np
import os

#############################
# initialize or clear data
#############################

data = {}

######################
# read and parse data
######################

# path from ipynb to data
data_dir = 'data/epa'

def download(yr, st, mo):
    """download data from EPA and put in directory structure"""
    # import
    import ftplib
    import zipfile
    
    # set up
    zipfname = '%s%s%s.zip'% (yr, st, mo)
    ftpfpath = 'dmdnload/emissions/hourly/monthly'
    ftpfname = os.path.join([ftpfpath, yr, zipfname])
    
    # open ftp client
    client = ftplib.FTP('ftp.epa.gov')
    client.login()
    
    # if file is on the ftp server...
    if ftpfname in client.nlst(ftpfpath):
        # download
        with open(zipfname, "wb") as f:
            print '... downloading %s' % mo
            client.retrbinary('RETR %s' % ftpfname , f.write)
            
        # unzip into target directory
        zipf = zipfile.ZipFile(zipfname)
        zipf.extractall(path=data_dir)
        zipf.close()
        
        # clean up
        os.remove(zipfname)
    
    # if not, log
    else:
        print '... skipping %s' % mo
        
    # always close the client
    client.close()
    
def load(yr, st):
    """read year and state from csv into data[yr][st]"""
    # add year if it's not there
    if not yr in data.keys():
        data[yr] = {}
        
    # check if we've already read it
    if not st in data[yr].keys():
        print "loading %s %s" % (yr, st)

        # storage
        pieces = []
        
        for mo in ['%02.f' % i for i in range(1,13)]:
            # set up path
            csvfname = os.path.join(data_dir, '%s%s%s.csv' % (yr, st, mo))
            
            # download file if it doesn't exist
            if not os.access(csvfname, os.F_OK):
                download(yr, st, mo)
                
            # csv -> dataframe
            try:
                pieces.append(pd.read_csv(csvfname))
            except:
                print '... skipping %s' % mo
            
        # combine into one big dataframe
        df = pd.concat(pieces)
        df['OP_DATE'] = pd.to_datetime(df['OP_DATE'])
        data[yr][st] = df
    
def get_unit_group(yr, st):
    """group by facility name and unit id"""
    # load and group
    try:
        this_data = data[yr][st].groupby(['FACILITY_NAME','UNITID'])
    except KeyError:
        load(yr, st)
        this_data = data[yr][st].groupby(['FACILITY_NAME','UNITID'])
        
    return this_data

def get_hr_group(yr, st):
    """group by date and hour"""
    # load and group
    try:
        this_data = data[yr][st].groupby(['OP_DATE','OP_HOUR'])
    except KeyError:
        load(yr, st)
        this_data = data[yr][st].groupby(['OP_DATE','OP_HOUR'])
        
    return this_data

def get_emissions(df):
    """lb of CO2 emissions"""
    try:
        s = df['CO2_MASS (tons)']
    except KeyError:
        s = df['CO2_MASS']
    return s * 2000.0

def get_generation(df):
    """MW of generation"""
    try:
        s = df['GLOAD (MW)']
    except KeyError:
        s = df['GLOAD']
    return s

def get_runtime(df):
    """fraction of time it was running"""
    return df['OP_TIME']

def total_gen_by_hour(yr, region):
    """total MW fossil generation over all units in an hour"""
    gen = None
    for st in region:
        this_gen = get_generation(get_hr_group(yr,st).sum()).fillna(0)
        if gen is None:
            gen = this_gen
        else:
            gen += this_gen
    return gen.fillna(0)

def total_em_by_hour(yr, region):
    """total lb CO2 over all units in an hour"""
    em = None
    for st in region:
        this_em = get_emissions(get_hr_group(yr,st).sum()).fillna(0)
        if em is None:
            em = this_em
        else:
            em += this_em
    return em.fillna(0)

def peak_hour_each_day(s):
    """ Return a set of (dy, hr) keys for the peak hour of each day.
        s should be a Series with keys [dy, hr].
    """   
    return set([(dy, np.argmax(group)) for dy, group in s.groupby(level=['OP_DATE'])])

def vals_in_ile(s, n_ile_min, n_ile_max=None):
    """Return a set of keys for rows with values in the nth percentile (or in the range if max is given)."""
    # default is 1 percentile bin
    if n_ile_max is None:
        n_ile_max = n_ile_min
        
    # minimum and maximum values
    val_min = np.percentile(s, n_ile_min)
    val_max = np.percentile(s, n_ile_max+1)
        
    # keys in range
    return set(s[(s >= val_min) & (s < val_max)].keys())
    
def days_with_peak_in_ile(s, n_ile_min, np_ile_max=None):
    """ Return a grouped Series of days whose peak is in the percentile range of hours.
        s should be a Series with keys [dy, hr].
    """
    # intersect sets of keys
    keys = vals_in_ile(s, n_ile_min, np_ile_max) & peak_hour_each_day(s)
    day_keys = [k[0] for k in keys]
    
    # select
    return s.select(lambda k: k[0] in day_keys)

def error_between_groups(g1, g2):
    """Error function is simple RMS for now."""
    return np.sqrt(np.mean((g1.values - g2.values)**2))
    
