# Generate metadata csv for city of Chicago datasets.

import urllib2
import json
import sys
import os
import datetime
import csv

metadataURL = "https://data.cityofchicago.org/api/views/"
dataURL = "https://data.cityofchicago.org/d/"
datasetList = "datasets.csv"
datasetDir = "/project/evtimov/wopr/data/"
metadataDir = "../meta/"
metadataFiles = ["meta_master.csv", "meta_attributes.csv", "meta_misc.csv"]

def write_meta(metadataID, datasetName):
    
    datasetTag = datasetName.replace("-", "_")

    #load json file from city of Chicago
    js = urllib2.urlopen(metadataURL + metadataID)
    js = json.load(js)
    
    #meta_master: extract general metadata from json
    try: humanName = js['name']
    except: humanName = ""

    try: description = js['description']
    except: description = ""

    try: sourceURL = dataURL + js['id']
    except: sourceURL = dataURL

    try: updateFreq = js['metadata']['custom_fields']['Metadata']['Frequency']
    except: updateFreq = ""
    
    obsFrom = obsTo = bbox = ""
    
    row = [datasetTag, humanName, description, sourceURL, obsFrom, obsTo, bbox, updateFreq]
    row = [x.encode('utf8').strip() for x in row]
    
    fp = open(metadataDir + metadataFiles[0], 'a')
    w = csv.writer(fp)
    w.writerow(row)
    fp.close()
    
    #meta_attributes: extract attribute name and description metadata from json
    fp = open(metadataDir + metadataFiles[1], 'a')
    w = csv.writer(fp)
    
    for x in js['columns']:
        if 'description' in x:
            row = [datasetTag, x['name'], x['description']]
            row = [x.encode('utf8').strip() for x in row]
            w.writerow(row)

    fp.close()

    #meta_misc: extract miscellaneous metadata from json
    fp = open(metadataDir + metadataFiles[2], 'a')
    w = csv.writer(fp)

    try:
        row = [datasetTag, "Data Owner", js['metadata']['custom_fields']['Metadata']['Data Owner']]
        row = [x.encode('utf8').strip() for x in row]
        w.writerow(row)
    except: pass

    try:
        row = [datasetTag, "Time Period", js['metadata']['custom_fields']['Metadata']['Time Period']]
        row = [x.encode('utf8').strip() for x in row]
        w.writerow(row)
    except: pass

    try:
        row = [datasetTag, "attribution", js['attribution']]
        row = [x.encode('utf8').strip() for x in row]
        w.writerow(row)
    except: pass

    try:
        row = [datasetTag, "category", js['category']]
        row = [x.encode('utf8').strip() for x in row]
        w.writerow(row)
    except: pass

    fp.close()



headers = [["dataset_name","human_name","description","source_url","obs_from","obs_to","bbox","update_freq"],
           ["dataset_name","attribute_name","attribute_description"],
           ["dataset_name","meta_attribute_name","meta_attribute_value"]]

#create metadata csvs and write headers
for i in range(3):
    fp = open(metadataDir + metadataFiles[i], 'wb')
    w = csv.writer(fp)
    w.writerow(headers[i])
    fp.close()

with open(datasetList, 'rb') as f:
    reader = csv.reader(f)
    datasets = [row for row in reader]

for (metadataID, datasetName, key, obsDate, obsTs) in datasets:
    write_meta(metadataID, datasetName)
