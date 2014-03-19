# Generate init_<dataset_name>.sql
# Requires one argument:
#	1) Path to dataset map
# Note: dataset csv file should have already undergone preprocessing (e.g. deduplication)

import sys, os, datetime

schemaFile = sys.argv[1]
attributes = []
types = []
key = obsDate = obsTs = loc = lat = lon = datasetName = ""

fp = open(schemaFile, 'r')
lines = fp.read().splitlines()
for i in range(0, len(lines)):
    if lines[i] == "--columns":
        i += 1
        while lines[i][0] != "-":
            attributes.append(lines[i].split()[0].upper())
            types.append(lines[i].split()[1].upper())
            i += 1
    if lines[i] == "--key":
        key = lines[i+1].upper()
        i += 1
    if lines[i] == "--obs_date":
        obsDate = lines[i+1].upper()
        i += 1
    if lines[i] == "--obs_ts":
        obsTs = lines[i+1].upper()
        i += 1
    if lines[i] == "--dataset_name":
        datasetName = lines[i+1]
        i += 1


SRC_DIR = "/home/alund/wopr/processed_data/dedup/" #"/project/evtimov/wopr/data/"

srcFiles = [ f for f in os.listdir(SRC_DIR) if datasetName in f ]
srcFile = sorted(srcFiles, key=lambda f: f[-14:-4])[0]

startDate = srcFile[-14:-4]
datasetTag = datasetName.replace("-", "_")

CREATE = [ attributes[i] + " " + types[i] for i in range(0, len(attributes)) ]
INSERT = [ a for a in attributes ]

SQL_INIT = """
DROP TABLE IF EXISTS SRC_{datasettag};
    
CREATE TABLE IF NOT EXISTS SRC_{datasettag}(
{create},
PRIMARY KEY({srckey}));
    
\copy SRC_{datasettag} FROM '{path}' WITH DELIMITER ',' CSV HEADER;
    
DROP TABLE IF EXISTS DAT_{datasettag};
    
CREATE TABLE IF NOT EXISTS DAT_{datasettag}(
{datasettag}_row_id SERIAL,
start_date DATE,
end_date DATE DEFAULT NULL,
current_flag BOOLEAN DEFAULT true,
{create},
PRIMARY KEY({datasettag}_row_id),
UNIQUE({srckey}, start_date));
    
INSERT INTO DAT_{datasettag}(
start_date,
{insert})
SELECT
'{startdate}' AS start_date,
{insert}
FROM SRC_{datasettag};
    
INSERT INTO DAT_Master(
start_date,
end_date,
current_flag,
Location,
LATITUDE,
LONGITUDE,
obs_date,
obs_ts,
dataset_name,
dataset_row_id)
SELECT
start_date,
end_date,
current_flag,
LOCATION,
LATITUDE,
LONGITUDE,
{obsdate} AS obs_date,
{obsts} AS obs_ts,
'{datasettag}' AS dataset_name,
{datasettag}_row_id AS dataset_row_id
FROM
DAT_{datasettag};
""".format(datasettag=datasetTag,
           create=",\n".join(CREATE),
           copy=", ".join(CREATE),
           srckey=key,
           path=SRC_DIR+srcFile,
           startdate=startDate,
           insert=",\n".join(INSERT),
           obsdate=obsDate,
           obsts=obsTs)


SQL_DIR = "../sql/init/"

fp = open(SQL_DIR + "init_" + datasetName + ".sql", 'w')
fp.write("\\timing\n")
fp.write(SQL_INIT)
