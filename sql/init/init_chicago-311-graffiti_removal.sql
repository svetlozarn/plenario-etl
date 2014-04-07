\timing

DROP TABLE IF EXISTS SRC_chicago_311_graffiti_removal;
        
CREATE TABLE IF NOT EXISTS SRC_chicago_311_graffiti_removal(
creation_date DATE,
status VARCHAR(18),
completion_date DATE,
service_request_number VARCHAR(26),
type_of_service_request VARCHAR(27),
what_type_of_surface_is_the_graffiti_on_ VARCHAR(48),
where_is_the_graffiti_located_ VARCHAR(36),
street_address VARCHAR(56),
zip_code INTEGER,
x_coordinate FLOAT8,
y_coordinate FLOAT8,
ward INTEGER,
police_district INTEGER,
community_area INTEGER,
ssa VARCHAR(12),
latitude FLOAT8,
longitude FLOAT8,
location POINT,
dup_ver SERIAL,
PRIMARY KEY(service_request_number, dup_ver));
        
\copy SRC_chicago_311_graffiti_removal(creation_date, status, completion_date, service_request_number, type_of_service_request, what_type_of_surface_is_the_graffiti_on_, where_is_the_graffiti_located_, street_address, zip_code, x_coordinate, y_coordinate, ward, police_district, community_area, ssa, latitude, longitude, location) FROM '/project/evtimov/wopr/data/chicago-311-graffiti_removal_2014-04-06.csv' WITH DELIMITER ',' CSV HEADER;
        
DROP TABLE IF EXISTS DAT_chicago_311_graffiti_removal;
        
CREATE TABLE IF NOT EXISTS DAT_chicago_311_graffiti_removal(
chicago_311_graffiti_removal_row_id SERIAL,
start_date DATE,
end_date DATE DEFAULT NULL,
current_flag BOOLEAN DEFAULT true,
dup_ver INTEGER,
creation_date DATE,
status VARCHAR(18),
completion_date DATE,
service_request_number VARCHAR(26),
type_of_service_request VARCHAR(27),
what_type_of_surface_is_the_graffiti_on_ VARCHAR(48),
where_is_the_graffiti_located_ VARCHAR(36),
street_address VARCHAR(56),
zip_code INTEGER,
x_coordinate FLOAT8,
y_coordinate FLOAT8,
ward INTEGER,
police_district INTEGER,
community_area INTEGER,
ssa VARCHAR(12),
latitude FLOAT8,
longitude FLOAT8,
location POINT,
UNIQUE(service_request_number, dup_ver),
PRIMARY KEY(chicago_311_graffiti_removal_row_id));
        
INSERT INTO DAT_chicago_311_graffiti_removal(
start_date,
dup_ver,
creation_date,
status,
completion_date,
service_request_number,
type_of_service_request,
what_type_of_surface_is_the_graffiti_on_,
where_is_the_graffiti_located_,
street_address,
zip_code,
x_coordinate,
y_coordinate,
ward,
police_district,
community_area,
ssa,
latitude,
longitude,
location)
SELECT
'2014-04-06' AS start_date,
dup_ver,
creation_date,
status,
completion_date,
service_request_number,
type_of_service_request,
what_type_of_surface_is_the_graffiti_on_,
where_is_the_graffiti_located_,
street_address,
zip_code,
x_coordinate,
y_coordinate,
ward,
police_district,
community_area,
ssa,
latitude,
longitude,
location
FROM SRC_chicago_311_graffiti_removal;
        
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
creation_date AS obs_date,
NULL AS obs_ts,
'chicago_311_graffiti_removal' AS dataset_name,
chicago_311_graffiti_removal_row_id AS dataset_row_id,
ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
FROM
DAT_chicago_311_graffiti_removal;
