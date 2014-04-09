import re
import pickle 
import os
import numpy as np
import csv
from scipy import stats
np.set_printoptions(suppress=True)

with open("/home/human/WattTime/data/data_array.pickle", "rb") as pickler:
    data_array = pickle.load(pickler) #loads the prepackaged array
#functions for preservation of abstraction for getting into from 1-Dimmensional arrays (rows)

def get_date(array):  #day of the year
    return array[0]

def get_hours(array): #total op_time in that day (among all plants)
    return array[1]

def get_MW(array):    #total MW of electricity produced in that day (among all plants)
    return array[2]

def get_CO2(array):   #total tons of CO2 produced in that day (among all plants)
    return array[3]

# end of functions for getting info from 1-Dimmensional arrays    

def avg_var(array, get_var):
# returns the average of var over all values of var in 2D_arrar, where get_var is used to get var from a row in the array.
    total = 0
    count = 0
    for row in array:
        total += float(get_var(row)) 
        count += 1
    return total/count
"""
#This is a test that demonstrates marrp. Uncomment to run
marrp(data_array, lambda x: get_date(x) ) #technique for dalyed computation
"""
"""
    #This is a test that demonstrates avg_var. Uncomment to run
print(avg_var( data_array, lambda x: get_MW(x) ))
"""

def difference(array, get_var):
#finds the difference between every day in one variable, appending it to the end of every row
    range_length = len(array) #to prevent unnecessary re-computing
    days = []
    for day in range(0, range_length):
        differences = []
        for ensuing in range(day+1, range_length):
            difference_between = abs(float(get_var(array[day])) - float(get_var(array[ensuing])))
            differences.append(difference_between)
        days.append(differences)
    return days #'triangular' list

def expected_difference(array, get_var, average_var):
#finds the expected difference between every day in one variable. This is kept separate from 'difference()' for readability.
    range_length = len(array)
    range_length_pop = range_length -1
    days = []
    for day in range(0, range_length):
        differences = []
        for ensuing in range(day+1, range_length):
            expectation = abs( float(get_var(array[day])) - (average_var-(float(get_var(array[day])) /range_length))*(range_length/range_length_pop)) #efficient implementation of 'rolling average'
            differences.append(expectation)
        days.append(differences)
    return days #'triangular' list

def square(triangular):
#"squares" and returns a "triangular" list into a full list, extracting appropriate values
    range_length = len(triangular)
    index = 0 
    for row in triangular:
        for element in reversed(range(0, index)):
            row.insert(0,triangular[element][index-1])
        index += 1
    return triangular 

def difference_proportion(expected_var_diffs, real_var_diffs):
#takes expected difference and actual difference and returns the difference proprtion for every day, between every other day
    x = 0 #'coordinates' of the grid
    y = 0
    proportions = []
    for row in range(0, len(expected_var_diffs)):
        results = []
        for value in range(0, len(expected_var_diffs[row])):
            results.append(real_var_diffs[row][value]/expected_var_diffs[row][value])
        proportions.append(results)
    return proportions

def sum_proportions(difference_proportion1, difference_proportion2):
    sums = []
    sim_range= len(difference_proportion1)
    for day in range(0, sim_range):
        proportions =[]
        for proportion in range(0, len(difference_proportion1[day])):
            proportions.append([(difference_proportion1[day][proportion] + difference_proportion2[day][proportion])/2, difference_proportion1[day][proportion], difference_proportion2[day][proportion], proportion])
        sums.append(proportions)
    sums = np.array(sums)
    return sums

def x_most_similar( x, day, adp):
#returns the x most similar days to a day
    a = adp[day]
    return a[a[:,0].argsort()][:x]

def get_similarity(difference_proportion1, difference_proportion2):
#returns an array containing how similar the days are based on the sum of the difference proportions
# in the format: [sum of differences, difference1, difference2, day]
    results = []
    sim_range = len(difference_proportion1)
    for day in range(0, sim_range):
        results.append([(difference_proportion1[day] + difference_proportion2[day]), difference_proportion1[day], difference_proportion2[day], day])
    results = np.array(results)
    return results

            
average_MW = avg_var(data_array, lambda x: get_MW(x))                                                       #compute average MW over all days

average_CO2 = avg_var(data_array,lambda x: get_CO2(x))                                                      #compute average CO2 over all days

Expected_MW_differences = square(expected_difference(data_array, lambda x: get_MW(x), average_MW))          #calculate expected difference in MW between each day and every other day
Expected_CO2_differences = square(expected_difference(data_array, lambda x: get_CO2(x), average_CO2))       #calculate expected difference in CO2 between each day and every other day

real_MW_differences = square(difference(data_array, lambda x: get_MW(x)))                                   #compute actual difference in MW between each day and every other day

real_CO2_differences = square(difference(data_array, lambda x: get_CO2(x)))                                 #compute actual difference in CO2 between each day and every other day 

MW_difference_proportions = np.array(difference_proportion(Expected_MW_differences, real_MW_differences))   #divide the above values and put in array representing how different each day is in MW

CO2_difference_proportions = np.array(difference_proportion(Expected_CO2_differences, real_CO2_differences))#divide the above values and put in array representing how different each day is in C02

aggregate_difference_proportions = sum_proportions(MW_difference_proportions, CO2_difference_proportions)   #how different each day is to every other day in the 2 variables

#print(x_most_similar(11, 6, aggregate_difference_proportions))                                              #prints the 10 most similar days to January 7, 2012  in terms of C02 tons  and MW/hr  



regresses = {}
for day in range(0, len(aggregate_difference_proportions)):
    regresses[day] = x_most_similar(40, day, aggregate_difference_proportions)

peaks = []


mws = []
for day in range(0, len(data_array)):
    mws.append(data_array[day][2])

co2s = []
for day in range(0, len(data_array)):
    co2s.append(data_array[day][3])

results = []
for iday in regresses:
    simday_arr = regresses[iday]
    mw_vals = [float(mws[int(simday[3])]) for simday in simday_arr]
    cos_vals=[float(co2s[int(simday[3])]) for simday in simday_arr]
    slope, intercept, r_val, p_val, ster = stats.linregress(np.array(mw_vals), np.array(cos_vals))
    results.append([slope, intercept, r_val, p_val, ster])
"""    
#for csv dumping
writer = csv.writer(open('regression_results.csv', 'w'), delimiter=',')
writer.writerow(["day", "slope: CO2 vs. MW", "intercept", "r_val", "p_val", "error"])
for i,row in enumerate(results):
    writer.writerow([i] + row)
   """ 
