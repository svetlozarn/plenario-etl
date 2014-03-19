# Generate a schema for a city of Chicago dataset.
# Requires two arguments:
#   1) ID of dataset on data.cityofchicago.org (e.g. 'hec5-y4x5')
#   2) Dataset name (e.g. 'chicago-311-graffiti_removal')
# Note: fields used for key and obs_date must be entered manually after map is produced
# **Still being tested, not yet working for large datasets (e.g. chicago-crimes-all)**

import urllib2, json, sys, os
import pandas as pd
import math


metadataID = sys.argv[1]
datasetName = sys.argv[2]

metadataURL = "https://data.cityofchicago.org/api/views/"
datasetPath = "/project/evtimov/wopr/data/"

js = urllib2.urlopen(metadataURL + metadataID)
js = json.load(js)

def extract_fields(js, field):
    return [ x[field] for x in js['columns'] ]

names = extract_fields(js, 'name')
fieldNames = extract_fields(js, 'fieldName')
dataTypes = extract_fields(js, 'renderTypeName')

numFields = len(fieldNames)


typeConversion = {"calendar_date" : "date", "location" : "point", "text" : "varchar(10)", "number" : "float8"}

for i, j in typeConversion.iteritems():
    dataTypes = [ t.replace(i, j) for t in dataTypes ]

datasetFiles = [ f for f in os.listdir(datasetPath) if datasetName in f ]
datasetFile = sorted(datasetFiles, key=lambda f: f[-14:-4])[0]

tmp = pd.read_csv(datasetPath + datasetFile, dtype=object, iterator=True, chunksize=1000)
df = pd.concat([ chunk for chunk in tmp ], ignore_index=True)

for i in range(numFields):
    if dataTypes[i] == "varchar(10)" and names[i] in df.columns:
        length = len(max(map(lambda x: str(x), df[names[i]]), key=len))
        dataTypes[i] = "varchar(" + str(int(1.2*length)) + ")"
    elif dataTypes[i] == "float8" and names[i] in df.columns:
        if not any("." in x for x in map(lambda x: str(x), df[names[i]])):
            dataTypes[i] = "integer"


mapPath = "../etl_maps/"
mapFields = [ "--columns", "--key", "--obs_date", "--obs_ts", "--dataset_name" ]

fp = open(mapPath + datasetName + ".map", 'w')

fp.write(mapFields[0] + "\n")
for i in range(numFields):
    fp.write(fieldNames[i] + " " + dataTypes[i] + "\n")
fp.write(mapFields[1] + "\n\n")
fp.write(mapFields[2] + "\n\n")
fp.write(mapFields[3] + "\nNULL\n")
fp.write(mapFields[4] + "\n" + datasetName)
