\timing

DROP TABLE IF EXISTS SRC_chicago_environmental_complaints;
        
CREATE TABLE IF NOT EXISTS SRC_chicago_environmental_complaints(
complaint_id VARCHAR(14),
complaint_type VARCHAR(49),
address VARCHAR(103),
street_number INTEGER,
street_number_to INTEGER,
direction VARCHAR(10),
street_name VARCHAR(26),
street_type VARCHAR(13),
inspector VARCHAR(10),
complaint_date DATE,
complaint_detail VARCHAR(3756),
inspection_log VARCHAR(4242),
data_source VARCHAR(34),
modified_date DATE,
dup_ver SERIAL,
PRIMARY KEY(complaint_id, dup_ver));
        
\copy SRC_chicago_environmental_complaints(complaint_id, complaint_type, address, street_number, street_number_to, direction, street_name, street_type, inspector, complaint_date, complaint_detail, inspection_log, data_source, modified_date) FROM '/project/evtimov/wopr/data/chicago-environmental-complaints_2014-04-06.csv' WITH DELIMITER ',' CSV HEADER;
        
DROP TABLE IF EXISTS DAT_chicago_environmental_complaints;
        
CREATE TABLE IF NOT EXISTS DAT_chicago_environmental_complaints(
chicago_environmental_complaints_row_id SERIAL,
start_date DATE,
end_date DATE DEFAULT NULL,
current_flag BOOLEAN DEFAULT true,
dup_ver INTEGER,
latitude FLOAT8,
longitude FLOAT8,
location POINT,
complaint_id VARCHAR(14),
complaint_type VARCHAR(49),
address VARCHAR(103),
street_number INTEGER,
street_number_to INTEGER,
direction VARCHAR(10),
street_name VARCHAR(26),
street_type VARCHAR(13),
inspector VARCHAR(10),
complaint_date DATE,
complaint_detail VARCHAR(3756),
inspection_log VARCHAR(4242),
data_source VARCHAR(34),
modified_date DATE,
UNIQUE(complaint_id, dup_ver),
PRIMARY KEY(chicago_environmental_complaints_row_id));
        
INSERT INTO DAT_chicago_environmental_complaints(
start_date,
dup_ver,
latitude,
longitude,
location,
complaint_id,
complaint_type,
address,
street_number,
street_number_to,
direction,
street_name,
street_type,
inspector,
complaint_date,
complaint_detail,
inspection_log,
data_source,
modified_date)
SELECT
'2014-04-06' AS start_date,
dup_ver,
FLOAT8((regexp_matches(address, '\((.*),.*\)'))[1]) AS latitude,
FLOAT8((regexp_matches(address, '\(.*,(.*)\)'))[1]) AS longitude,
POINT((regexp_matches(address, '\((.*)\)'))[1]) AS location,
complaint_id,
complaint_type,
address,
street_number,
street_number_to,
direction,
street_name,
street_type,
inspector,
complaint_date,
complaint_detail,
inspection_log,
data_source,
modified_date
FROM SRC_chicago_environmental_complaints;
        
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
complaint_date AS obs_date,
NULL AS obs_ts,
'chicago_environmental_complaints' AS dataset_name,
chicago_environmental_complaints_row_id AS dataset_row_id,
ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
FROM
DAT_chicago_environmental_complaints;
