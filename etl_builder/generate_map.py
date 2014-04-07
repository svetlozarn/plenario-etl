# Generate a schema for a city of Chicago dataset.

import urllib2
import json
import sys
import os
import pandas as pd
import math

testDatasets = [
    ("fypr-ksnz", "chicago-environmental-complaints", "complaint_id", "complaint_date", "NULL"),
    ("hec5-y4x5", "chicago-311-graffiti_removal", "service_request_number", "creation_date", "NULL")]

metadataURL = "https://data.cityofchicago.org/api/views/"
datasetDir = "/project/evtimov/wopr/data/"

#city of Chicago dataset IDs, csv file base names, key, obs date, obs ts
datasets = [
    #("mq3i-nnqe", "chicago-bus-stop-boardings", "", "", "NULL"),
    #("ygr5-vcbg", "chicago-towed-vehicles", "", "", "NULL"),
    #("77hq-huss", "chicago-congestion-estimates", "", "", "NULL"),
    #("n4j6-wkkf", "chicago-congestion-estimates-segments", "", "", "NULL"),
    ("ijzp-q8t2", "chicago-crimes-all", "id", "orig_date", "NULL"),
    ("r5kz-chrr", "chicago-business-licenses", "license_id", "payment_date", "NULL"),
    ("fypr-ksnz", "chicago-environmental-complaints", "complaint_id", "complaint_date", "NULL"),
    ("hec5-y4x5", "chicago-311-graffiti_removal", "service_request_number", "creation_date", "NULL"),
    ("uxic-zsuj", "chicago-311-tree-trims", "service_request_number", "creation_date", "NULL"),
    ("9ksk-na4q", "chicago_311_garbage_carts", "service_request_number", "creation_date", "NULL"),
    ("97t6-zrhs", "chicago_311_rodent_baiting", "service_request_number", "creation_date", "NULL"),
    ("7as2-ds3y", "chicago_311_potholes_reported", "service_request_number", "creation_date", "NULL"),
    ("me59-5fac", "chicago_311_sanitation_code_complaints", "service_request_number", "creation_date", "NULL"),
    ("t28b-ys7j", "chicago_311_alley_lights_out", "service_request_number", "creation_date", "NULL"),
    ("3aav-uy2v", "chicago_311_street_lights_one_out", "service_request_number", "creation_date", "NULL"),
    ("zuxi-7xem", "chicago_311_street_lights_all_out", "service_request_number", "creation_date", "NULL")]

#conversion dict for socrata data types to psql data types
typeConversion = {
    "date" : "timestamp",
    "calendar_timestamp" : "date", #"calendar_date" : "date"
    "location" : "point",
    "text" : "varchar(12)",
    "phone" : "varchar(12)",
    "html" : "varchar(12)",
    "url" : "varchar(2083)",
    "email" : "varchar(254)",
    "percent" : "varchar(12)",
    "checkbox" : "boolean",
    "number" : "float8",
    "money" : "money"}


def write_map(metadataID, datasetName, key, obsDate, obsTs):

    #load json file from city of Chicago
    js = urllib2.urlopen(metadataURL + metadataID)
    js = json.load(js)

    #get relevant fields from json file
    names = [x['name'] for x in js['columns']]
    fieldNames = [x['fieldName'] for x in js['columns']]
    dataTypes = [x['renderTypeName'] for x in js['columns']]

    numFields = len(fieldNames)

    #replace socrata data types with psql data types
    for i, j in typeConversion.iteritems():
        dataTypes = [t.replace(i, j) for t in dataTypes]
    
    #location field in chicago-environmental-complaints contains text address
    if datasetName == "chicago-environmental-complaints":
        dataTypes = [t.replace("point", "varchar(12)") for t in dataTypes]

    #locate dataset csv file
    datasetFiles = [ f for f in os.listdir(datasetDir) if datasetName in f ]
    datasetFile = sorted(datasetFiles, key=lambda f: f[-14:-4])[0]

    #read entire csv file into dataframe
    tmp = pd.read_csv(datasetDir + datasetFile, dtype=object, iterator=True, chunksize=1000)
    df = pd.concat([chunk for chunk in tmp], ignore_index=True)

    #adjust length of varchar based on longest value, adjust numeric types
    for i in range(numFields):
        if dataTypes[i] == "varchar(12)" and names[i] in df.columns:
            length = len(max(map(lambda x: str(x), df[names[i]]), key=len))
            dataTypes[i] = "varchar(" + str(int(1.2*length)) + ")"
        elif dataTypes[i] == "float8" and names[i] in df.columns:
            if not any("." in x for x in map(lambda x: str(x), df[names[i]])):
                dataTypes[i] = "integer"

    mapPath = "../etl_maps/"

    fp = open(mapPath + datasetName + ".map", 'w')

    #write the map
    fp.write("--dataset_name\n" + datasetName + "\n")
    fp.write("--key\n" + key + "\n")
    fp.write("--obs_date\n" + obsDate + "\n")
    fp.write("--obs_ts\n" + obsTs + "\n")
    fp.write("--columns\n" + "\n".join([fieldNames[i] + " " + dataTypes[i] for i in range(numFields)]))


for (metadataID, datasetName, key, obsDate, obsTs) in testDatasets:
#for (metadataID, datasetName, key, obsDate, obsTs) in datasets:
    write_map(metadataID, datasetName, key, obsDate, obsTs)
