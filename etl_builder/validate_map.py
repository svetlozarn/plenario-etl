import urllib2
import json
import sys
import os
import pandas as pd
import math
import csv
import re
import generate_map as gm


def read_map(datasetName):
    fp = open(gm.mapDir + datasetName + ".map", 'r')
    lines = fp.read().splitlines()
    datasetName = lines[1]
    key = lines[3]
    obsDate = lines[5]
    obsTs = lines[7]
    fieldNames = [x.split()[0] for x in lines[9:len(lines)]]
    dataTypes = [x.split()[1] for x in lines[9:len(lines)]]
    return key, obsDate, obsTs, fieldNames, dataTypes


def check_exists(datasetName, numCols):
    r = re.compile('\d\d\d\d-\d\d-\d\d')
    date = sorted([f for f in os.listdir(gm.datasetDir) if r.match(f[-14:-4]) is not None], key=lambda f: f[-14:-4])[-134][-14:-4]
    datasetFiles = sorted([f for f in os.listdir(gm.datasetDir) if datasetName in f and date in f], key=lambda f: f[-14:-4])
    print("Validating %s data from %s" % (datasetName, date))
    if len(datasetFiles) == 0:
        return 1, "ERROR : file does not exist\n"
    else:
        tmp = pd.read_csv(gm.datasetDir + datasetFiles[0], nrows=1)
        if len(tmp.columns) != numCols and os.stat(gm.datasetDir + datasetFiles[0]).st_size < 250:
            return 2, "ERROR : invalid file\n"
    return 0, datasetFiles[0]


def check_columns(cols1, cols2, types1, types2):
    if cols1 == cols2:
        if types1 == types2:
            return 0, ""
        else:
            return 1, "ERROR : data types changed\n"
    if len(cols1) == len(cols2):
        if sorted(cols1) == sorted(cols2):
            return 2, "ERROR : column order changed\n"
        else:
            return 3, "ERROR : column names changed\n"
    elif len(cols1) < len(cols2):
        return 4, "ERROR : new column added\n"
    return 5, "ERROR : column removed\n"


with open(gm.datasetList, 'rb') as f:
    reader = csv.reader(f)
    datasets = [row for row in reader]

fp = open("validate.log", 'w')
for (metadataID, datasetName, key, obsDate, obsTs) in datasets:
    fp.write(datasetName + "\n")
    key1, obsDate1, obsTs1, fieldNames1, dataTypes1 = read_map(datasetName)
    [error, message] = check_exists(datasetName, len(dataTypes1))
    if error != 0:
        fp.write(message)
        continue
    datasetFile = message
    key2, obsDate2, obsTs2, fieldNames2, dataTypes2 = gm.create_map(metadataID, datasetName, datasetFile, key, obsDate, obsTs)
    [error, message] = check_columns(fieldNames1, fieldNames2, dataTypes1, dataTypes2)
    if error != 0:
        fp. write(message)
