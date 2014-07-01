# Generate update_<dataset_name>.sql

import sys
import os
import datetime

datasetDir = "/project/evtimov/wopr/data/"
sqlDir = "../sql/update/"

def write_update(mapFile):
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
    createKey = [attributes[i] + " " + types[i] for i in range(0, len(attributes)) if attributes[i] == key]
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
    
    sqlUpdate = """
\\timing ON

--
-- load raw data 
--
DROP TABLE IF EXISTS SRC_{datasettag};
 
CREATE TABLE IF NOT EXISTS SRC_{datasettag}(
{create},
line_num SERIAL,
PRIMARY KEY(line_num));
        
\copy SRC_{datasettag}({copy}) FROM '{path}' WITH DELIMITER ',' CSV HEADER;


--
-- create DUP table with calculated dup_ver for each srcKey
--
DROP TABLE IF EXISTS DUP_{datasettag};

CREATE TABLE IF NOT EXISTS DUP_{datasettag}(         
{createkey},
line_num INTEGER,
dup_ver INTEGER,
PRIMARY KEY({srckey}, dup_ver));

INSERT INTO DUP_{datasettag}
SELECT {srckey},
line_num,
RANK () OVER (PARTITION BY {srckey} ORDER BY line_num DESC) AS dup_ver
FROM SRC_{datasettag};

--
-- find new records in raw data and store their srcKey + dupVer in NEW_<dataset_name>
-- 
DROP TABLE IF EXISTS NEW_{datasettag};

CREATE TABLE IF NOT EXISTS NEW_{datasettag}(
  {createkey},
  line_num INTEGER,  
  dup_ver INTEGER,   
  PRIMARY KEY({srckey}, dup_ver)
);

INSERT INTO NEW_{datasettag}
SELECT {srckey}, 
line_num,
dup_ver
FROM SRC_{datasettag}
JOIN DUP_{datasettag} USING (line_num, {srckey})
LEFT OUTER JOIN DAT_{datasettag} USING ({srckey}, dup_ver)
WHERE {datasettag}_row_id IS NULL;

--
-- insert new records in DAT_<dataset_name>
--
INSERT INTO DAT_{datasettag}(
start_date,
dup_ver,
{insertloc}{insert})
SELECT
'{startdate}' AS start_date,
dup_ver,
{parseloc}{insert}
FROM SRC_{datasettag}
JOIN NEW_{datasettag} USING (line_num,{srckey});

--
-- insert new records in DAT_master
--
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
DAT_{datasettag}
JOIN NEW_{datasettag} USING ({srckey},dup_ver);
""".format(datasettag=datasetTag,
           create=",\n".join(create),
           createkey=", ".join(createKey),
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

    fp = open(sqlDir + "update_" + datasetName + ".sql", 'w')
    fp.write(sqlUpdate)


mapDir = "../etl_maps/"
mapFiles = [f for f in os.listdir(mapDir) if ".map" in f]

for mapFile in mapFiles:
    print mapFile
    write_update(mapDir + mapFile)
