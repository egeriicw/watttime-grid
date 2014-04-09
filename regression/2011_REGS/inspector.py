#This file used to analyze data that exists locally. Data is downloaded seperately. 
from numpy import array
import csv
import os
import pickle

path = "/home/human/WattTime/data/CA-2012" 
path2= "/home/human/WattTime/data/CA-2012-Price"
plants = {}
pices = {}

#functions for preservation of abstraction
def get_name(row):
    try: return float(row[1])
    except: return 0

def get_ORISPL_code(row):
    try: return float(row[2])
    except: return 0

def get_unit_id(row):
    try: return float(row[3])
    except: return 0

def get_date(row):
    try: return row[4]
    except: return 0

def get_hour(row):
    try: return float(row[5])
    except: return 0

def get_op_time(row):
    try: return float(row[6])
    except: return 0 

def get_GLOAD(row):
    try: return float(row[7])
    except: return 0

def get_SLOAD(row):
    return row[8]

def get_SO2_MASS(row):
    return row[9]

def get_SO2_MASS_MEASURE(row):
    return row[10]

def get_SO2_RATE(row):
    return row[11]

def get_SO2_RATE_MEASURE(row):
    return row[12]

def get_CO2_MASS(row):
    try: return row[17]
    except: return 0

#for price data begin
def get_LMP_TYPE(row):
    try: return row[6]
    except: return 0

def get_OPR_DT(row):
    try: return row[0]
    except: return 0

def get_HE01(row):
    try: return row[11]
    except: return 0

def get_AVPR(row):
#returns the average price in a day
    total_price = 0
    for i in range(11,35):
        try: total_price += float(row[i])
        except: total_price += 0
    return total_price/24
#for price data end

def get_first(row):
    return row[0] #gets first item in row
# end of abstraction functions for getting info from csv list object

"""
#This is for opening new .csv files. This should now be done by opener.py
#here for reference only.

for filename in os.listdir(path):
    with open("{0}/{1}".format(path,filename)) as csv_file:
        data_object = csv.reader(csv_file)
        for row in data_object:
            if "{0} ID:{d1}".format(get_name(row),get_unit_id(row)) not in plants:
                plants["{0} ID:{1}".format(get_name(row),get_unit_id(row))] = [row]
            else:                      
                if get_unit_id(row) == get_unit_id(get_first(plants["{0} ID:{1}".format(get_name(row),get_unit_id(row))])):
                    plants["{0} ID:{1}".format(get_name(row),get_unit_id(row))].append(row)
                else:
                    plants["{0} ID:{1}".format(get_name(row),get_unit_id(row))] = [row]
"""
with open("/home/human/WattTime/data/plants.pickle","rb") as pickler:
    plants= pickle.load(pickler) #gets info for plants dictionary from file

"""
#This is for generating an "aggregate plant", that measures total amount over all plants
#This should now be done by opener.py. Here for reference only. 

def tryfloat(string):
    try: return float(string)
    except: return 0

aggregate_plant ={}
worklist = []
for plant in plants:
    for row in plants[plant]:
        if get_date(row) not in aggregate_plant:
            totalhour = get_op_time(row)
            totalmw = get_GLOAD(row)
            totalCO2 = get_CO2_MASS(row)
            aggregate_plant[get_date(row)] = [tryfloat(totalhour), tryfloat(totalmw), tryfloat(totalCO2)]
        else:
            hour = get_op_time(row)
            mw = get_GLOAD(row)
            CO2 = get_CO2_MASS(row)
            aggregate_plant[get_date(row)][0] += tryfloat(hour)
            aggregate_plant[get_date(row)][1] += tryfloat(mw)
            aggregate_plant[get_date(row)][2] += tryfloat(CO2)
"""
with open("/home/human/WattTime/data/aggregate_plant.pickle","rb") as pickler:
    aggregate_plant = pickle.load(pickler) #gets info for aggregate_plant dictionary from file

"""
#this is a test that displays the contents of the aggregate_plant dictionary
#uncomment to run
for value in sorted(aggregate_plant):
   print(value, aggregate_plant[value])
"""   
#this converts the aggregate plant dictionary into a data list.
data_list = []
for date in sorted(aggregate_plant):
    data_list.append([date] + aggregate_plant[date])
data_list.pop()
data_list.pop()
#thisconverts the data_list into a 2-dimmensional numpy array 
data_array = array([row for row in data_list])    

with open("/home/human/WattTime/data/data_array.pickle","wb") as pickler:
    pickle.dump(data_array, pickler) #dumps the array for later use by engine


"""
#this is a test that displays the contents of the data_list list
#uncomment to run
for i in data_list:
    print(i)
"""

#these 3 dictionaries exist to make working with their respective variables more convenient
"""
aggregate_HOUR = {} 
for i in aggregate_plant:
    aggregate_HOUR[aggregate_plant[i][0]] = i
    
aggregate_MW = {}
for i in aggregate_plant:
    aggregate_MW[aggregate_plant[i][1]] = i

aggregate_CO2 = {}
for i in aggregate_plant:
    aggregate_CO2[aggregate_plant[i][2]] = i
"""
"""
#this is a test that displays the contents of the dictionaries in increasing order
#uncomment to run
for dic in [aggregate_HOUR,aggregate_MW,aggregate_CO2]:
    for value in sorted(dic):
        print(value, dic[value])
"""

"""
 #This is for opening new .csv files for price. This should now be done by opener.py
 #Here for reference only. 

for filename in os.listdir(path2):
    with open("{0}/{1}".format(path2,filename)) as csv_file:
        price_object = csv.reader(csv_file)
        for row in price_object:
            if get_LMP_TYPE(row) == "LMP":
                prices[get_OPR_DT(row)] = row
"""

with open("/home/human/WattTime/data/prices.pickle","rb") as pickler:
    prices = pickle.load(pickler) #gets info for prices dictionary from file

av_daily_prices = {}
for date in prices:
    av_daily_prices[get_AVPR(prices[date])] = date
"""
#This is a test that prints the average daily prices in increasing order.
#uncomment to run
for price in sorted(av_daily_prices):
    print(price, av_daily_prices[price])
"""
av_daily_prices_bydate = {}
for date in prices:
    av_daily_prices_bydate[date] = get_AVPR(prices[date])
        
"""
#Another formatting test
for plant in sorted(plants):
    for row in plants[plant]:
        print("Plant: {0}; Date: {1} Hour: {2}; Operating Percent: {3}".format(get_name(row), get_date(row), get_hour(row), get_op_time(row)))

print("Success!")

"""

def count_rows(plants, key):
    count = 0   
    for row in plants[key]:
        count +=1

def mean(dictionary):
    total = 0
    count = 0
    for value in dictionary.values():
        count +=1
        try: total += value
        except: total += 0
    return total/count

def avpr_when_on(plants, prices):
    outlist = []
    for plant in sorted(plants):
        for row in plants[plant]:
            outlist.append("{0}: {1}".format(get_date(row), get_hour(row)))

def operating_time_average(plants, period=24):
 #returns average operating time over all plants in given period
    returndict = {}
    for plant in plants:
        count = 0
        ontime = 0
        for row in plants[plant]:
            count += 1
            try: ontime += float(get_op_time(row))
            except: ontime += 0
        returndict[plant] = (ontime * period)/count
    return mean(returndict)


def average_X(plants, plant, get_X,  period=24):
    total = 0 
    count = 0
    for row in plants[plant]:
        if float(get_X(row)) >= 0:
            count += 1
            total += float(get_X(row))
    if count == 0: 
        return 0
    return (total * period)/count

def average_X_dict(plants, get_X, period=24):
    AV_DICT = {}
    for plant in plants:
        AV_DICT[plant] = average_X(plants, plant, get_X, period) 
    return AV_DICT

def op_time_av_plant(plant, period=24):
    ontime = 0
    for row in plant:
        try: ontime += float(get_op_time(row))
        except: ontime += 0
    return (ontime * period)/8784

def CO2_per_MW(plant, period=1):
    CO2 = 0
    MW = 0
    for row in plant:
        try: CO2 += float(get_CO2_MASS(row))
        except: CO2 += 0  
    for row in plant:
        try: MW += float(get_GLOAD(row))
        except: MW += 0
    if MW == 0:
        return 0
    return (period * (CO2/MW))
"""
#FOR GRAPHING WITH MATPLOTLIB/PYLAB

CO2_DICT = {}
for plant in sorted(plants):
    CO2_DICT[CO2_per_MW(plants[plant])] = plant
for plant in sorted(CO2_DICT):
    print(plant, CO2_DICT[plant])
xvals =[]
yvals =[]
for plant in sorted(CO2_DICT):
    xvals.append(CO2_DICT[plant])
    yvals.append(plant)
import matplotlib.pyplot as plt
import pylab

fig = plt.figure()
graph = fig.add_subplot(111)
fig.subplots_adjust(top=0.85)
graph.set_ylabel("CO2 per MW/hr")
graph.set_xlabel("Plants")
fig.suptitle("Average CO2 per MW", fontsize=25, fontweight="bold")
x = range(len(xvals))
pylab.plot(x, yvals, "g")
pylab.show()
"""
    
def at_least(a, b):
    if a >= b:
        return True
    else:
        return False

def at_most(a, b):
    if a <= b:
        return True
    else:
        return False

def on_percent(plants, percent, operator, period=24): 
#returns a list of plants that were operating (on average) at least X percent of a period
    returnlist = []
    for plant in plants:
        if operator(op_time_av_plant(plants[plant], period), percent*period):
            returnlist.append(plant)
    return returnlist
"""
#This is a test that prints all plants on at least half of an average day
#Uncomment to run
for plant in on_percent(plants, .5 , at_least, 24):
    print (plant)
"""
def similar(plants, operator, threshold, comparer, value, period=24):
    #returns a dictionary of plants greater than or less than(depending on operator) a certain threshold ratio of the output of a value function over a period, compared by some comparer function
    returndict= {}
    finaldict = {}
    for plant in plants:
        returndict[plant] = comparer(plants[plant], period)
    for item in returndict:
        if operator(returndict[item]/value, threshold * value):
            finaldict[item] = returndict[item]   
    return finaldict

""""
#This is a test which displays an example use of "similar" function from above.
#Uncomment to run

sortdict =  similar(plants, at_least, 1 , op_time_av_plant, operating_time_average(plants))
reverse = {}
for plant in sorted(sortdict):
    reverse[sortdict[plant]] = plant
for i in reverse:
    print(i, reverse[i])
"""
def similar_days(dict_list, date, radius):
    """ takes in a list of pairs of dictionaries of format:
       [({average of var1: corresponding-date }, {corresponding-date: average of var1}), ({average of var2: corresponding-date... }...)...]
    as well as a date, and a radius. Returns the most similar days to the given day by minimizing
    the distance between the values recorded in that day and the average values in
    the {radius} amount of days requested. A radius of 5 would return the 10 most
    similar days: 5 days with values greater than the day, and 5 days with lower values
    """
    return("failure")

def similar_days(dict_list, date, amount):
    my_difference ="nope"  
    print("Success!")

