#This is step 1
#Aggregates data from multiple csv files into one numpy array for convenient processing later 
import csv
import os
import pickle
import numpy

path = "/home/human/Desktop/Good/Better/data/aggregated_epa/" #where new emissions/use data is
plants = {}
prices = {}
"""
plants_raw = []
for filename in os.listdir(data_path):
    plants_raw.append(numpy.loadtxt(open("{0}/{1}".format(data_path, filename), "rb"), delimiter=",", dtype="str"))

plants_raw = numpy.array(plants_raw)

print("done!")
"""
def get_first(row):
    return row[0]

def get_name(row):
    try: return row[1]
    except: return 0

def get_unit_id(row):
    try: return float(row[3])
    except: return 0

def get_date(row):
    try: return row[4]
    except: return 0

def get_op_time(row):
    try: return float(row[6])
    except: return 0

def get_GLOAD(row):
    try: return float(row[7])
    except: return 0

def get_CO2_MASS(row):
    try: return float(row[17])
    except: return 0

def get_LMP_TYPE(row):
    try: return row[6]
    except: return 0

def get_OPR_DT(row):
    try: return row[0]
    except: return 0

formatted_name = lambda row:"{0} ID:{1}".format(get_date(row),get_unit_id(row)) 

for filename in os.listdir(path):
    with open("{0}/{1}".format(path,filename)) as csv_file:
        data_object = csv.reader(csv_file)
        for row in data_object:
            if formatted_name(row) not in plants:
                plants[formatted_name(row)] = [row] 
            else:
                if get_unit_id(row) == get_unit_id(get_first(plants[formatted_name(row)])):
                    plants[formatted_name(row)].append(row)
                else:
                    plants[formatted_name(row)] = [row]

with open("/home/human/WattTime/data/.pickle","wb") as pickler:
    pickle.dump(plants, pickler) #dumps the dictionary to file for later use by inspector


aggregate_plant = {}
for plant in plants:
    for row in plants[plant]:
        if get_date(row) not in aggregate_plant:
            totalhour = get_op_time(row)
            totalmw = get_GLOAD(row)
            totalCO2 = get_CO2_MASS(row)
            aggregate_plant[get_date(row)] = [totalhour, totalmw, totalCO2]
        else:
            hour = get_op_time(row)
            mw = get_GLOAD(row)
            CO2 = get_CO2_MASS(row)
            aggregate_plant[get_date(row)][0] += hour
            aggregate_plant[get_date(row)][1] += mw
            aggregate_plant[get_date(row)][2] += CO2
        
with open("/home/human/WattTime/data/aggregate_plant.pickle","wb") as pickler:
    pickle.dump(aggregate_plant, pickler) #dumps the dictionary to file for later use by inspector

for filename in os.listdir(path2):
    with open("{0}/{1}".format(path2,filename)) as csv_file:
        price_object = csv.reader(csv_file)
        for row in price_object:
            if get_LMP_TYPE(row) == "LMP":
                prices[get_OPR_DT(row)] = row

with open("/home/human/WattTime/data/prices.pickle","wb") as pickler:
    pickle.dump(prices, pickler) #dumps the dictionary to file for later use by inspector
