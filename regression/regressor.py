#This is step 2.
#Takes preprocessed data from aggregator, pandaggregator 
#Computes similarity between days and runs regression on the 40 days in the similarity groupings
#Performs regression with scipy
"""
Determines similarity in multiple variables according to the 'difference proportion' process described in Similarity.pdf.

This is summarized:

    1. Compute the expected difference between the value of every variable x in every unit of time.
       E[MY_x_VALUE - RANDOM_x_VALUE]
       Reduces to: MY_x_VALUE - MEANOFALLOTHER_x_VALUES
       Generates n^2 size data structure for each variable x.

    2. Find the actual difference between the value of every variable x in every unit of time.
       MY_x_VALUE - NEXT_x_VALUE
       Generates n^2 size data stucture for each variable x.

    3. Find the proportion of expected difference and actual difference for every variable x
    , for every pair of time units. 
       val1-val2 / E[val1 - val2]
        Combines previous structures into one n^2 size data structure for each variable x.
        
    4. The time units most similar to eachother are those for which the sum of these proportions for       each variable is minimized.
       Once the structure is created, we can read off the m most similar days with ease, and then a        different amount after that without recomputing,
       since ordering the sum of difference proportions created a ranked listing of how similar each unit of time is to every other unit of time. 

    5. Performs regression and outputs to csv file

   
In this example, 4 varaibles are used to determine similarity, and 2 are used in the regressions. 


"""
       

import pickle
import math
import csv
import os 
import numpy as np
from scipy import stats
from operator import itemgetter

with open("/home/human/Desktop/Good/Better/reg_auths.pickle", "rb") as pickler:
    authorities = pickle.load(pickler) #import change in mw and co2 data, hourly
    c_auth = {}
    c_auth["CISO"] = authorities["CISO"] #in this example, we only look at CISO
                                         #The process can be done for every BA

with open("/home/human/Desktop/Good/Better/auths.pickle", "rb") as pickler:
    total_mw = pickle.load(pickler) #import total mw data, daily
    c_totalmw = {}
    c_totalmw["CISO"] = total_mw["CISO"]#Only looking at CISO

with open("/home/human/Desktop/Good/Better/dpeaks.pickle", "rb") as pickler:
    dpeaks = pickle.load(pickler)   #import peak mw data, daily
    c_peakmw = {}
    c_peakmw["CISO"] = dpeaks["CISO"]#Just CISO

with open("/home/human/Desktop/Good/Better/avgs.pickle", "rb") as pickler:
    avg_prices = pickle.load(pickler) #import average price data, daily
    c_avpr = {}
    c_avpr["CISO"] = avg_prices #CISO ONLY

with open("/home/human/Desktop/Good/Better/maxes.pickle", "rb") as pickler:
    max_prices = pickle.load(pickler) #import max price data, daily
    c_peakpr = {}
    c_peakpr["CISO"] = max_prices #ONLY CISO


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
            if math.isnan(day): #FIXES PESKY PROBLEM WITH NAN AT THE END OF LISTS
                day = var_dict[authority][0]
            total += day
            count += 1
        avg = total/count               #average
        expected_diffs = []
        for day in var_dict[authority]:
            eds = []   #new row for each day
            expectation = abs(      day - (      (avg - (day/365) * (365/364))       )     )  #rolling average
            for ensuing in var_dict[authority]:
                eds.append(expectation)
            expected_diffs.append(eds)
        return_dict[authority] = expected_diffs
    return return_dict

def difference_proportions(expected_var_diffs, observed_var_diffs):
#generates difference proportions from expected differences
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

def aggregateBAD(prop1, prop2):
#aggregates multiple difference proportion dicts into a single dictionary
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

def aggregate(*args):
#aggregates multiple difference proportion dictionaries into a single dictionary
    return_dict = {}
    for authority in sorted(args[0].keys()):
        props = []
        for index in range(0, 365):
            orps = []
            for day in range(0,365):
                orps.append((day, (sum([prop[authority][index][day] for prop in args])/len(args))))
            props.append(sorted(orps, key=itemgetter(1))) #sorts the list based on diff proportion
        return_dict[authority] = props
    return return_dict
               



start = datetime.now()  #for timing

print("Computing differences between variables...")
pr_diffs = generate_difference(c_avpr)
print("...")
ppr_diffs =generate_difference(c_peakpr)
print("...")
mw_diffs = generate_difference(c_totalmw)
print("...")
pmw_diffs = generate_difference(c_peakmw)
 
print("done.")

print("Computing expected differences between variables...")

pr_Xdiffs = expected_difference(c_avpr)
print("...")
ppr_Xdiffs =expected_difference(c_peakpr)
print("...")
mw_Xdiffs = expected_difference(c_totalmw)
print("...")
pmw_Xdiffs= expected_difference(c_peakmw)
print("done.")

print("Computing difference proportions...")
pr_diff_props = difference_proportions(pr_Xdiffs, pr_diffs)
print("...")
ppr_diff_props= difference_proportions(ppr_Xdiffs,ppr_diffs)
print("...")
mw_diff_props = difference_proportions(mw_Xdiffs, mw_diffs) 
print("...")
pmw_diff_props= difference_proportions(pmw_Xdiffs,pmw_diffs)
print("done.")

print("Finishing finding similarity between days in 2 varaibles...")
aggregate_props = aggregate(mw_diff_props, pr_diff_props, pmw_diff_props, ppr_diff_props)
print("done.")

print("Cooking up a warm batch of regressions...")

regressions = {}
for authority in c_auth.keys():
    authority_result = []
    for day in aggregate_props[authority]:
        most_similar = [day[i][0] for i in range(0,40)]
        d_CO2 = []
        d_MW = []

        for hour in range(0,24): 
            d_CO2.append([c_auth[authority][hour + 24*day_i][1] for day_i in most_similar])
            d_MW.append([c_auth[authority][hour + 24*day_i][0] for day_i in most_similar])

        hour_result = []
        for hour in range(0, len(d_CO2)):
            output = stats.linregress(d_MW[hour], d_CO2[hour])
            hour_result.append((output[0], output[-1])) #only keeps the first and last output from linregress
        authority_result.append(hour_result)

    print(authority + ", yum!")
    regressions[authority] = authority_result


end = datetime.now()

time = end-start

print("In total, the process took:",time) #for timing

print("Writing csv files")
for authority in regressions.keys():
    print("...")
    writer = csv.writer(open( "{0}_REGS.csv".format(authority), 'w'), delimiter = ',') 
    writer.writerow(["DAY"] + ["HOUR_{0}".format(hour) for hour in range(0,24)])
    for day, row in enumerate(regressions[authority]):
        writer.writerow([day] + list(row))

print("Success!")
