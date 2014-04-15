# Generate init_<dataset_name>.sql

import sys
import os
import datetime

datasetDir = "/project/evtimov/wopr/data/"
sqlDir = "../sql/init/"


def write_init(mapFile):
    fp = open(mapFile, 'r')
    lines = fp.read().splitlines()

    datasetName = lines[1]
    key = lines[3]
    obsDate = lines[5]
    obsTs = lines[7]
    attributes = [x.split()[0].lower() for x in lines[9:len(lines)]]
    types = [x.split()[1].upper() for x in lines[9:len(lines)]]

    srcFiles = [f for f in os.listdir(datasetDir) if datasetName in f]
    srcFile = sorted(srcFiles, key=lambda f: f[-14:-4])[-1]

    startDate = srcFile[-14:-4]
    datasetTag = datasetName.replace("-", "_")

    create = [attributes[i] + " " + types[i] for i in range(0, len(attributes))]
    insert = [a for a in attributes]
    
    createLoc = ""
    insertLoc = ""
    parseLoc = ""
    
    if datasetTag == "chicago_environmental_complaints":
        createLoc = """latitude FLOAT8,
longitude FLOAT8,
location POINT,
"""
        insertLoc = """latitude,
longitude,
location,
"""
        parseLoc = """FLOAT8((regexp_matches(address, '\((.*),.*\)'))[1]) AS latitude,
FLOAT8((regexp_matches(address, '\(.*,(.*)\)'))[1]) AS longitude,
POINT((regexp_matches(address, '\((.*)\)'))[1]) AS location,
"""
    
    sqlInit = """
DROP TABLE IF EXISTS SRC_{datasettag};
        
CREATE TABLE IF NOT EXISTS SRC_{datasettag}(
{create},
dup_ver SERIAL,
PRIMARY KEY({srckey}, dup_ver));
        
\copy SRC_{datasettag}({copy}) FROM '{path}' WITH DELIMITER ',' CSV HEADER;
        
DROP TABLE IF EXISTS DAT_{datasettag};
        
CREATE TABLE IF NOT EXISTS DAT_{datasettag}(
{datasettag}_row_id SERIAL,
start_date DATE,
end_date DATE DEFAULT NULL,
current_flag BOOLEAN DEFAULT true,
dup_ver INTEGER,
{createloc}{create},
UNIQUE({srckey}, dup_ver),
PRIMARY KEY({datasettag}_row_id));
        
INSERT INTO DAT_{datasettag}(
start_date,
dup_ver,
{insertloc}{insert})
SELECT
'{startdate}' AS start_date,
dup_ver,
{parseloc}{insert}
FROM SRC_{datasettag};
        
INSERT INTO DAT_Master(
start_date,
end_date,
current_flag,
location,
latitude,
longitude,
obs_date,
obs_ts,
dataset_name,
dataset_row_id,
location_geom)
SELECT
start_date,
end_date,
current_flag,
location,
latitude,
longitude,
{obsdate} AS obs_date,
{obsts} AS obs_ts,
'{datasettag}' AS dataset_name,
{datasettag}_row_id AS dataset_row_id,
ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
FROM
DAT_{datasettag};
""".format(datasettag=datasetTag,
           create=",\n".join(create),
           copy=", ".join(insert),
           srckey=key,
           path=datasetDir+srcFile,
           startdate=startDate,
           insert=",\n".join(insert),
           obsdate=obsDate,
           obsts=obsTs,
           createloc=createLoc,
           insertloc=insertLoc,
           parseloc=parseLoc)

    fp = open(sqlDir + "init_" + datasetName + ".sql", 'w')
    fp.write(sqlInit)


mapDir = "../etl_maps/"
mapFiles = [f for f in os.listdir(mapDir) if ".map" in f]

for mapFile in mapFiles:
    write_init(mapDir + mapFile)
