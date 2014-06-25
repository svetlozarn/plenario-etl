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

# conversion dict for socrata data types to postgre data types
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


# set text fields to varchar(n) where n = 2*length of longest value; set integer fields to integer
def adjust_types(datasetName, datasetFile, dataTypes, names):
    tmp = pd.read_csv(datasetDir + datasetFile, dtype=object, chunksize=1000000)
    maxLen = [-1]*len(dataTypes)
    isInt = [True]*len(dataTypes)
    for chunk in tmp:
        df = chunk
        for i in range(len(dataTypes)):
            if dataTypes[i] == "text" and names[i] in df.columns:
                length = len(max(map(lambda x: str(x), df[names[i]]), key=len))
                if length > maxLen[i]: maxLen[i] = length
            if dataTypes[i] == "float8" and names[i] in df.columns:
                if any("." in x for x in map(lambda x: str(x), df[names[i]])): isInt[i] = False
    for i in range(len(dataTypes)):
        if maxLen[i] >= 0: dataTypes[i] = "varchar(%d)" % (2*maxLen[i])
        if dataTypes[i] == "float8" and isInt[i] == True: dataTypes[i] = "integer"
    return dataTypes


# make sure columns names are safe
def adjust_columns(fieldNames, key, obsDate, obsTs):
    # longest possible column name in postgre
    maxLen = 63;
    # if column name matches a postgre data type, append a "1"
    fieldNames = [x+"1" if x in typeConversion.viewvalues() else x[:maxLen] for x in fieldNames]
    key = key+"1" if key in typeConversion.viewvalues() else key[:maxLen]
    obsDate = obsDate+"1" if obsDate in typeConversion.viewvalues() else obsDate[:maxLen]
    obsTs = obsTs+"1" if obsTs in typeConversion.viewvalues() else obsTs[:maxLen]
    return fieldNames, key, obsDate, obsTs


def create_map(metadataID, datasetName, datasetFile, key, obsDate, obsTs):

    # load json file from city of Chicago
    js = urllib2.urlopen(metadataURL + metadataID)
    js = json.load(js)

    # get relevant fields from json file
    names = [x['name'] for x in js['columns']]
    fieldNames = [x['fieldName'] for x in js['columns']]
    dataTypes = [x['renderTypeName'] for x in js['columns']]

    # replace socrata data types with postgre data types
    for i, j in typeConversion.iteritems():
        dataTypes = [t.replace(i, j) for t in dataTypes]
    
    # location field in chicago-environmental-complaints contains text address
    if datasetName == "chicago-environmental-complaints":
        dataTypes = [t.replace("point", "text") for t in dataTypes]

    dataTypes = adjust_types(datasetName, datasetFile, dataTypes, names)
    fieldNames, key, obsDate, obsTs = adjust_columns(fieldNames, key, obsDate, obsTs)
    return key, obsDate, obsTs, fieldNames, dataTypes


def write_map(datasetName, key, obsDate, obsTs, fieldNames, dataTypes):
    fp = open(mapDir + datasetName + ".map", 'w')
    fp.write("--dataset_name\n" + datasetName + "\n")
    fp.write("--key\n" + key + "\n")
    fp.write("--obs_date\n" + obsDate + "\n")
    fp.write("--obs_ts\n" + obsTs + "\n")
    fp.write("--columns\n" + "\n".join([fieldNames[i] + " " + dataTypes[i] for i in range(len(dataTypes))]))


with open(datasetList, 'rb') as f:
    reader = csv.reader(f)
    datasets = [row for row in reader]

for (metadataID, datasetName, key, obsDate, obsTs) in datasets:
    # assumes data file naming convention is: datasetname_yyyy-mm-dd.csv
    datasetFiles = sorted([f for f in os.listdir(datasetDir) if datasetName in f], key=lambda f: f[-14:-4])
    datasetFile = datasetFiles[0]
    key, obsDate, obsTs, fieldNames, dataTypes = create_map(metadataID, datasetName, datasetFile, key, obsDate, obsTs)
    write_map(datasetName, key, obsDate, obsTs, fieldNames, dataTypes)
