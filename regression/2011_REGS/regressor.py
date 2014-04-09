import pickle
import csv
import os 
import numpy as np
from scipy import stats
from operator import itemgetter
from datetime import datetime

with open("/home/human/Desktop/Good/Better/auths.pickle", "rb") as pickler:
    total_mw = pickle.load(pickler)

with open("/home/human/Desktop/Good/Better/dpeaks.pickle", "rb") as pickler:
    dpeaks = pickle.load(pickler)

with open("/home/human/Desktop/Good/Better/reg_auths.pickle", "rb") as pickler:
    authorities = pickle.load(pickler)

def generate_difference(var_dict):
#computes difference between every day's variable, for every authority
    return_dict = {}
    for authority in sorted(var_dict.keys()):
        differences = []
        for day in var_dict[authority]:
            today = [] #new row for each day
            for ensuing in var_dict[authority]:
                today.append(abs(day - ensuing))
            differences.append(today) 
        return_dict[authority] = differences
    return return_dict

def expected_difference(var_dict):
#computes expected difference between every day's variable
    return_dict = {}
    for authority in sorted(var_dict.keys()):
        total = 0
        count = 0
        for day in var_dict[authority]: #compute the average of variable
            total += day
            count += 1
        avg = total/count               #average
        expected_diffs = []
        for day in var_dict[authority]:
            eds = []   #new row for each day
            expectation = abs( day - ((avg - (day/365))*(365/364))) #rolling average
            for ensuing in var_dict[authority]:
                eds.append(expectation)
            expected_diffs.append(eds)
        return_dict[authority] = expected_diffs
    return return_dict

def difference_proportions(expected_var_diffs, observed_var_diffs):
    return_dict = {}
    for authority in sorted(expected_var_diffs.keys()):
        proportions = []
        for index in range(0, 365):
            props = []
            for day in range(0, 365):
                props.append(observed_var_diffs[authority][index][day]/expected_var_diffs[authority][index][day])
            proportions.append(props)
        return_dict[authority] = proportions
    return return_dict

def aggregate(prop1, prop2):
    return_dict = {}
    for authority in sorted(prop1.keys()):
        props = []
        for index in range(0, 365):
            orps = []
            for day in range(0, 365):
                orps.append((day, (prop1[authority][index][day] + prop2[authority][index][day])/2))
            props.append(sorted(orps, key=itemgetter(1))) #sorts the list based on proportion
        return_dict[authority] = props
    return return_dict


start = datetime.now()

print("Computing differences between variables...")
mw_diffs = generate_difference(total_mw)
print("...")
peak_diffs = generate_difference(dpeaks)
print("done.")

print("Computing expected differences between variables...")
mw_Xdiffs = expected_difference(total_mw)
print("...")
peak_Xdiffs = expected_difference(dpeaks)
print("done.")

print("Computing difference proportions...")
mw_diff_proportions = difference_proportions(mw_Xdiffs, mw_diffs) 
print("...")
peak_diff_proportions = difference_proportions(peak_Xdiffs, peak_diffs)
print("done.")

print("Finishing finding similarity between days in 2 varaibles...")
aggregate_props = aggregate(mw_diff_proportions, peak_diff_proportions)
print("done.")

print("Cooking up a warm batch of regressions...")

regressions = {}
for authority in authorities.keys():
    authority_result = []
    for day in aggregate_props[authority]:
        most_similar = [day[i][0] for i in range(0,40)]
        d_CO2 = []
        d_MW = []

        for hour in range(0,24): 
            d_CO2.append([authorities[authority][hour + 24*day_i][1] for day_i in most_similar])
            d_MW.append([authorities[authority][hour + 24*day_i][0] for day_i in most_similar])

        hour_result = []
        for hour in range(0, len(d_CO2)):
            output = stats.linregress(d_MW[hour], d_CO2[hour])
            hour_result.append((output[0], output[-1])) #only keeps the first and last 
        authority_result.append(hour_result)

    print(authority + ", yum!")
    regressions[authority] = authority_result


end = datetime.now()

time = end-start

print("In total, the process took:",time)

print("Making csv files")
for authority in regressions.keys():
    print("...")
    writer = csv.writer(open( "{0}_REGS.csv".format(authority), 'w'), delimiter = ',') 
    writer.writerow(["DAY"] + ["HOUR_{0}".format(hour) for hour in range(0,24)])
    for day, row in enumerate(regressions[authority]):
        writer.writerow([day] + list(row))
        

print("Success!")
