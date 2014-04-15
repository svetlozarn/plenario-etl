# Generate a schema for a city of Chicago dataset.

import urllib2
import json
import sys
import os
import pandas as pd
import math
import csv

metadataURL = "https://data.cityofchicago.org/api/views/"
datasetDir = "/project/evtimov/wopr/data/"
mapDir = "../etl_maps/"
datasetList = "datasets.csv"

#conversion dict for socrata data types to psql data types
typeConversion = {
    "date" : "timestamp",
    "calendar_timestamp" : "date", #"calendar_date" : "date"
    "location" : "point",
    "text" : "text",
    "phone" : "text",
    "html" : "text",
    "url" : "text",
    "email" : "text",
    "percent" : "text",
    "checkbox" : "boolean",
    "number" : "float8",
    "money" : "money"}

def adjust_types(datasetName, dataTypes, names):
     
    #locate dataset csv file
    datasetFiles = [f for f in os.listdir(datasetDir) if datasetName in f]
    datasetFile = sorted(datasetFiles, key=lambda f: f[-14:-4])[0]
    
    #read part of csv file into dataframe
    df = pd.read_csv(datasetDir + datasetFile, dtype=object, nrows=10000)
    
    #adjust length of varchar based on longest value, adjust numeric types
    for i in range(len(dataTypes)):
        if dataTypes[i] == "text" and names[i] in df.columns:
            length = len(max(map(lambda x: str(x), df[names[i]]), key=len))
            dataTypes[i] = "varchar(" + str(int(2*length)) + ")"
        elif dataTypes[i] == "float8" and names[i] in df.columns:
            if not any("." in x for x in map(lambda x: str(x), df[names[i]])):
                dataTypes[i] = "integer"


def write_map(metadataID, datasetName, key, obsDate, obsTs):

    #load json file from city of Chicago
    js = urllib2.urlopen(metadataURL + metadataID)
    js = json.load(js)

    #get relevant fields from json file
    names = [x['name'] for x in js['columns']]
    fieldNames = [x['fieldName'] for x in js['columns']]
    dataTypes = [x['renderTypeName'] for x in js['columns']]

    #replace socrata data types with psql data types
    for i, j in typeConversion.iteritems():
        dataTypes = [t.replace(i, j) for t in dataTypes]
    
    #location field in chicago-environmental-complaints contains text address
    if datasetName == "chicago-environmental-complaints":
        dataTypes = [t.replace("point", "text") for t in dataTypes]

    adjust_types(datasetName, dataTypes, names)

    fp = open(mapDir + datasetName + ".map", 'w')

    #write the map
    fp.write("--dataset_name\n" + datasetName + "\n")
    fp.write("--key\n" + key + "\n")
    fp.write("--obs_date\n" + obsDate + "\n")
    fp.write("--obs_ts\n" + obsTs + "\n")
    fp.write("--columns\n" + "\n".join([fieldNames[i] + " " + dataTypes[i] for i in range(len(dataTypes))]))


with open(datasetList, 'rb') as f:
    reader = csv.reader(f)
    datasets = [row for row in reader]

for (metadataID, datasetName, key, obsDate, obsTs) in datasets:
    write_map(metadataID, datasetName, key, obsDate, obsTs)
