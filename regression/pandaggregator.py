#This is step 1
#Aggregates data from multiple csv files into one numpy array for convenient processing later.
#Similar to aggregator.py, but uses pandas and is more convenient for different data

import pickle
import pandas as p
import os
import csv
import numpy as np
from datetime import datetime

epa_path = "/home/human/Desktop/Good/Better/data/2011_EPA"
peak_path= "/home/human/Desktop/Good/Better/data/2011_PEAKS"
"""
#This code is what originally loaded/pickled the data. It was slow.
#uncomment to run
peaks = []
authorities = []
for peak_file in sorted(os.listdir(peak_path)):
    this = p.DataFrame.from_csv("{0}/{1}".format(peak_path, peak_file))
    peaks.append(this)

for epa_file in sorted(os.listdir(epa_path)):
    this = p.DataFrame.from_csv("{0}/{1}".format(epa_path, epa_file))
    authorities.append(this)

peaks = np.array(peaks)
authorities = np.array(authorities)

with open("/home/human/Desktop/Good/Better/peaks.pickle", "wb") as pickler:
    pickle.dump(peaks, pickler)

with open("/home/human/Desktop/Good/Better/authorities.pickle", "wb") as pickler:
    pickle.dump(authorities, pickler)

auths = {}
for authority in authorities:
    gloads = []
    index = 0
    for day in range(0, 365):
        total_mw = 0
        for hour in range(0,24):
            if index+hour < 8760:
                total_mw += authority["GLOAD (MW)"][index+hour]
            index += 1
        gloads.append(total_mw)
    auths[authority["BA_ABBREV"][0]] = gloads

with open("/home/human/Desktop/Good/Better/auths.pickle", "wb") as pickler:
    pickle.dump(auths, pickler)
#with open("/home/human/Desktop/Good/Better/authorities.pickle","rb") as pickler:
#    authorities = pickle.load(pickler)
#end = datetime.now()
#Don't need this anymore, but you might to process new authorities data. 

#dpeaks = {}
#index = 0
#for authority in sorted(auths.keys()):
#    dpeaks[authority] = [value for value in peaks[index]["gloadmw"]]
#    index += 1

with open("/home/human/Desktop/Good/Better/peaks.pickle","rb") as pickler:
    peaks = pickle.load(pickler)

with open("/home/human/Desktop/Good/Better/auths.pickle", "rb") as pickler:
    auths = pickle.load(pickler)

with open("/home/human/Desktop/Good/Better/dpeaks.pickle", "rb") as pickler:
    dpeaks = pickle.load(pickler)
"""
with open("/home/human/Desktop/Good/Better/authorities.pickle", "rb") as pickler:
    authorities = pickle.load(pickler)
    #will now compute the change in mw per authority per hour    
reg_auths = {}

for authority in authorities:
    reg_auths[authority["BA_ABBREV"][0]] = [[authority["GLOAD (MW)"][hour]- authority["GLOAD (MW)"][hour -1], authority["CO2_MASS (tons)"][hour] - authority["CO2_MASS (tons)"][hour-1]] for hour in range(0, len(authority))]

with open("/home/human/Desktop/Good/Better/reg_auths.pickle", "wb") as pickler:
    pickle.dump(reg_auths, pickler)

print("Success!")
