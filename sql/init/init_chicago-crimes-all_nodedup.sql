DROP TABLE IF EXISTS SRC_chicago_crimes_all;

CREATE TABLE IF NOT EXISTS SRC_chicago_crimes_all(
ID INTEGER,
Case_Number VARCHAR(10),
Orig_Date TIMESTAMP,
Block VARCHAR(50),
IUCR VARCHAR(10),
Primary_Type VARCHAR(100),
Description VARCHAR(100),
Location_Description VARCHAR(50),
Arrest BOOLEAN,
Domestic BOOLEAN,
Beat VARCHAR(10),
District VARCHAR(5),
Ward INTEGER,
Community_Area VARCHAR(10),
FBI_Code VARCHAR(10),
X_Coordinate INTEGER,
Y_Coordinate INTEGER,
Year INTEGER,
Updated_On TIMESTAMP,
Latitude FLOAT8,
Longitude FLOAT8,
Location POINT);


\copy SRC_chicago_crimes_all FROM '/project/evtimov/wopr/data/chicago-crimes-all_2014-03-23.csv' WITH (FORMAT CSV, HEADER true, DELIMITER ',')

--
-- create the DAT table for chicago_crimes_all and  
--  populate it with the first source file
-- 

DROP TABLE IF EXISTS DAT_chicago_crimes_all;

CREATE TABLE IF NOT EXISTS DAT_chicago_crimes_all(
chicago_crimes_all_row_id SERIAL,
start_date DATE,
end_date DATE DEFAULT NULL,
current_flag BOOLEAN DEFAULT true,
ID INTEGER,
Case_Number VARCHAR(10),
Orig_Date TIMESTAMP,
Block VARCHAR(50),
IUCR VARCHAR(10),
Primary_Type VARCHAR(100),
Description VARCHAR(100),
Location_Description VARCHAR(50),
Arrest BOOLEAN,
Domestic BOOLEAN,
Beat VARCHAR(10),
District VARCHAR(5),
Ward INTEGER,
Community_Area VARCHAR(10),
FBI_Code VARCHAR(10),
X_Coordinate INTEGER,
Y_Coordinate INTEGER,
Year INTEGER,
Updated_On TIMESTAMP,
Latitude FLOAT8,
Longitude FLOAT8,
Location POINT,
PRIMARY KEY(chicago_crimes_all_row_id));

INSERT INTO DAT_chicago_crimes_all(
start_date,
ID,
Case_Number,
Orig_Date,
Block,
IUCR,
Primary_Type,
Description,
Location_Description,
Arrest,
Domestic,
Beat,
District,
Ward,
Community_Area,
FBI_Code,
X_Coordinate,
Y_Coordinate,
Year,
Updated_On,
Latitude,
Longitude,
Location)
SELECT
'2014-03-23' AS start_date,
ID,
Case_Number,
Orig_Date,
Block,
IUCR,
Primary_Type,
Description,
Location_Description,
Arrest,
Domestic,
Beat,
District,
Ward,
Community_Area,
FBI_Code,
X_Coordinate,
Y_Coordinate,
Year,
Updated_On,
Latitude,
Longitude,
Location
FROM SRC_chicago_crimes_all;

--
-- insert new chicago-crimes-all tuples 
--  in DAT_master
--


INSERT INTO DAT_Master(
  start_date,
  end_date,
  current_flag,
  Location,
  Latitude,
  Longitude,
  obs_date,
  obs_ts,
  dataset_name,
  dataset_row_id,
  location_geom)
SELECT
  start_date,
  end_date,
  current_flag,
  Location,
  Latitude,
  Longitude,
  Orig_Date AS obs_date,
  NULL AS obs_ts,
  'chicago_crimes_all' AS dataset_name,
  chicago_crimes_all_row_id AS dataset_row_id,
  ST_SetSRID(ST_MakePoint(Longitude, Latitude), 4326)
FROM
  DAT_chicago_crimes_all;






