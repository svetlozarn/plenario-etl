# read the first line of data file and generate schema

DATA_DIR = "/project/evtimov/wopr/data/"
DEDUP_DIR = "/project/evtimov/wopr/data/"
INIT_DIR = "/project/evtimov/wopr/sql/init-test/"
LAST_DATE="2014-03-29"

DATASET_NAMES = [
#"chicago_crimes_all",
#"chicago_business_licenses",
#"chicago_311_graffiti_removal",
#"chicago_environmental_complaints",
    ("chicago_311_garbage_carts","service_request_number", "creation_date"),
    ("chicago_311_rodent_baiting","service_request_number", "creation_date"),
    ("chicago_311_potholes_reported","service_request_number", "creation_date"),
    ("chicago_311_sanitation_code_complaints","service_request_number", "creation_date"),
    ("chicago_311_alley_lights_out","service_request_number", "creation_date"),
    ("chicago_311_street_lights_one_out","service_request_number", "creation_date"),
    ("chicago_311_street_lights_all_out","service_request_number", "creation_date"),
    ("chicago_building_permits","id", "issue_date"),
    ("chicago_building_violations","id", "violation_date" ),
    ("chicago_311_vacant_and_abandoned_building","service_request_number", "date_service_request_was_received"),
    ("chicago_food_inspections","inspection_id","inspection_date")
]

VAR_TYPES = {
'creation_date' : 'DATE',
'status' : 'VARCHAR(20)',
'completion_date' : 'DATE',
'service_request_number' : 'VARCHAR(50)',
'type_of_service_request' : 'VARCHAR(100)',
'number_of_black_carts_delivered' : 'INTEGER',
'current_activity' : 'VARCHAR(100)',
'most_recent_action' : 'VARCHAR(100)',
'street_address' : 'VARCHAR(100)',
'zip_code' : 'CHAR(10)',
'x_coordinate' : 'FLOAT8',
'y_coordinate' : 'FLOAT8',
'ward' : 'INTEGER',
'police_district' : 'INTEGER',
'community_area' : 'INTEGER',
'ssa' : 'VARCHAR(10)',
'latitude' : 'FLOAT8',
'longitude' : 'FLOAT8',
'location' : 'POINT',
'number_of_potholes_filled_on_block' : 'FLOAT',
'zip' : 'CHAR(10)',
'number_of_premises_baited' : 'INTEGER',
'number_of_premises_with_garbage' : 'INTEGER',
'number_of_premises_with_rats' : 'FLOAT',
'id' : 'INTEGER',
'permitnum' : 'VARCHAR(20)',
'permit_type' : 'VARCHAR(100)',
'issue_date' : 'DATE',
'estimated_cost' : 'VARCHAR(20)',
'amount_waived' : 'VARCHAR(20)',
'amount_paid' : 'VARCHAR(20)',
'total_fee' : 'VARCHAR(20)',
'street_number' : 'VARCHAR(10)',
'street_direction' : 'VARCHAR(5)',
'street_name' : 'VARCHAR(50)',
'suffix' : 'VARCHAR(10)',
'work_description' : 'TEXT',
'pin1' : 'VARCHAR(20)',
'pin2' : 'VARCHAR(20)',
'pin3' : 'VARCHAR(20)',
'pin4' : 'VARCHAR(20)',
'pin5' : 'VARCHAR(20)',
'pin6' : 'VARCHAR(20)',
'pin7' : 'VARCHAR(20)',
'pin8' : 'VARCHAR(20)',
'pin9' : 'VARCHAR(20)',
'pin10' : 'VARCHAR(20)',
'contractor_1_type' : 'VARCHAR(50)',
'contractor_1_name' : 'VARCHAR(100)',
'contractor_1_address' : 'VARCHAR(100)',
'contractor_1_city' : 'VARCHAR(40)',
'contractor_1_state' : 'VARCHAR(20)',
'contractor_1_zipcode' : 'VARCHAR(20)',
'contractor_1_phone' : 'VARCHAR(30)',
'contractor_2_type' : 'VARCHAR(50)',
'contractor_2_name' : 'VARCHAR(100)',
'contractor_2_address' : 'VARCHAR(100)',
'contractor_2_city' : 'VARCHAR(40)',
'contractor_2_state' : 'VARCHAR(20)',
'contractor_2_zipcode' : 'VARCHAR(20)',
'contractor_2_phone' : 'VARCHAR(30)',
'contractor_3_type' : 'VARCHAR(50)',
'contractor_3_name' : 'VARCHAR(100)',
'contractor_3_address' : 'VARCHAR(100)',
'contractor_3_city' : 'VARCHAR(40)',
'contractor_3_state' : 'VARCHAR(20)',
'contractor_3_zipcode' : 'VARCHAR(20)',
'contractor_3_phone' : 'VARCHAR(30)',
'contractor_4_type' : 'VARCHAR(50)',
'contractor_4_name' : 'VARCHAR(100)',
'contractor_4_address' : 'VARCHAR(100)',
'contractor_4_city' : 'VARCHAR(40)',
'contractor_4_state' : 'VARCHAR(20)',
'contractor_4_zipcode' : 'VARCHAR(20)',
'contractor_4_phone' : 'VARCHAR(30)',
'contractor_5_type' : 'VARCHAR(50)',
'contractor_5_name' : 'VARCHAR(100)',
'contractor_5_address' : 'VARCHAR(100)',
'contractor_5_city' : 'VARCHAR(40)',
'contractor_5_state' : 'VARCHAR(20)',
'contractor_5_zipcode' : 'VARCHAR(20)',
'contractor_5_phone' : 'VARCHAR(30)',
'contractor_6_type' : 'VARCHAR(50)',
'contractor_6_name' : 'VARCHAR(100)',
'contractor_6_address' : 'VARCHAR(100)',
'contractor_6_city' : 'VARCHAR(40)',
'contractor_6_state' : 'VARCHAR(20)',
'contractor_6_zipcode' : 'VARCHAR(20)',
'contractor_6_phone' : 'VARCHAR(30)',
'contractor_7_type' : 'VARCHAR(50)',
'contractor_7_name' : 'VARCHAR(100)',
'contractor_7_address' : 'VARCHAR(100)',
'contractor_7_city' : 'VARCHAR(40)',
'contractor_7_state' : 'VARCHAR(20)',
'contractor_7_zipcode' : 'VARCHAR(20)',
'contractor_7_phone' : 'VARCHAR(30)',
'contractor_8_type' : 'VARCHAR(50)',
'contractor_8_name' : 'VARCHAR(100)',
'contractor_8_address' : 'VARCHAR(100)',
'contractor_8_city' : 'VARCHAR(40)',
'contractor_8_state' : 'VARCHAR(20)',
'contractor_8_zipcode' : 'VARCHAR(20)',
'contractor_8_phone' : 'VARCHAR(30)',
'contractor_9_type' : 'VARCHAR(50)',
'contractor_9_name' : 'VARCHAR(100)',
'contractor_9_address' : 'VARCHAR(100)',
'contractor_9_city' : 'VARCHAR(40)',
'contractor_9_state' : 'VARCHAR(20)',
'contractor_9_zipcode' : 'VARCHAR(20)',
'contractor_9_phone' : 'VARCHAR(30)',
'contractor_10_type' : 'VARCHAR(50)',
'contractor_10_name' : 'VARCHAR(100)',
'contractor_10_address' : 'VARCHAR(100)',
'contractor_10_city' : 'VARCHAR(40)',
'contractor_10_state' : 'VARCHAR(20)',
'contractor_10_zipcode' : 'VARCHAR(20)',
'contractor_10_phone' : 'VARCHAR(30)',
'contractor_11_type' : 'VARCHAR(50)',
'contractor_11_name' : 'VARCHAR(100)',
'contractor_11_address' : 'VARCHAR(100)',
'contractor_11_city' : 'VARCHAR(40)',
'contractor_11_state' : 'VARCHAR(20)',
'contractor_11_zipcode' : 'VARCHAR(20)',
'contractor_11_phone' : 'VARCHAR(30)',
'contractor_12_type' : 'VARCHAR(50)',
'contractor_12_name' : 'VARCHAR(100)',
'contractor_12_address' : 'VARCHAR(100)',
'contractor_12_city' : 'VARCHAR(40)',
'contractor_12_state' : 'VARCHAR(20)',
'contractor_12_zipcode' : 'VARCHAR(20)',
'contractor_12_phone' : 'VARCHAR(30)',
'contractor_13_type' : 'VARCHAR(50)',
'contractor_13_name' : 'VARCHAR(100)',
'contractor_13_address' : 'VARCHAR(100)',
'contractor_13_city' : 'VARCHAR(40)',
'contractor_13_state' : 'VARCHAR(20)',
'contractor_13_zipcode' : 'VARCHAR(20)',
'contractor_13_phone' : 'VARCHAR(30)',
'contractor_14_type' : 'VARCHAR(50)',
'contractor_14_name' : 'VARCHAR(100)',
'contractor_14_address' : 'VARCHAR(100)',
'contractor_14_city' : 'VARCHAR(40)',
'contractor_14_state' : 'VARCHAR(20)',
'contractor_14_zipcode' : 'VARCHAR(20)',
'contractor_14_phone' : 'VARCHAR(30)',
'contractor_15_type' : 'VARCHAR(50)',
'contractor_15_name' : 'VARCHAR(100)',
'contractor_15_address' : 'VARCHAR(100)',
'contractor_15_city' : 'VARCHAR(40)',
'contractor_15_state' : 'VARCHAR(20)',
'contractor_15_zipcode' : 'VARCHAR(20)',
'contractor_15_phone' : 'VARCHAR(30)',
'violation_last_modified_date' : 'DATE',
'violation_date' : 'DATE',
'violation_code' : 'VARCHAR(10)',
'violation_status' : 'VARCHAR(20)',
'violation_status_date' : 'DATE',
'violation_description' : 'VARCHAR(100)',
'violation_location' : 'TEXT',
'violation_inspector_comments' : 'TEXT',
'violation_ordinance' : 'TEXT',
'inspector_id' : 'VARCHAR(10)',
'inspection_number' : 'INTEGER',
'inspection_status' : 'VARCHAR(10)',
'inspection_waived' : 'VARCHAR(10)',
'inspection_category' : 'VARCHAR(20)',
'department_bureau' : 'VARCHAR(30)',
'address' : 'VARCHAR(100)',
'property_group' : 'INTEGER',
'inspection_id' : 'INTEGER',
'dba_name' : 'VARCHAR(100)',
'aka_name' : 'VARCHAR(100)',
'license_num' : 'INTEGER',
'facility_type' : 'VARCHAR(50)',
'risk' : 'VARCHAR(20)',
'city' : 'VARCHAR(40)',
'state' : 'VARCHAR(20)',
'inspection_date' : 'DATE',
'inspection_type' : 'VARCHAR(50)',
'results' : 'VARCHAR(20)',
'violations' : 'TEXT',
'what_is_the_nature_of_this_code_violation' : 'VARCHAR(100)',
'service_request_type' : 'VARCHAR(100)',
'date_service_request_was_received' : 'DATE',
'location_of_building_on_the_lot_if_garage_change_type_code_to_bgd' : 'TEXT',
'is_the_building_dangerous_or_hazardous' : 'VARCHAR(50)',
'is_building_open_or_boarded' : 'VARCHAR(100)',
'if_the_building_is_open_where_is_the_entry_point' : 'TEXT',
'is_the_building_currently_vacant_or_occupied' : 'VARCHAR(50)',
'is_the_building_vacant_due_to_fire' : 'VARCHAR(10)',
'any_people_using_property_homeless_childen_gangs' : 'VARCHAR(10)',
'address_street_number' : 'VARCHAR(20)',
'address_street_direction' : 'VARCHAR(5)',
'address_street_name' : 'VARCHAR(50)',
'address_street_suffix' : 'VARCHAR(10)'
}

import re
PATTERN = re.compile(r'''((?:[^,"']|"[^"]*"|'[^']*')+)''')

def process_dataset(datasetName, bizKey, obsName):
    fp = open("%s%s_%s.csv" % (DATA_DIR, datasetName, LAST_DATE))

    line = fp.readline()
    fp.close()

    line = line.strip().lower()
    elems = PATTERN.split(line)[1::2]

    schema = []

    print "++++++++++++++ %s ++++++++++++++" % datasetName
    for e in elems:
        e = e.strip().replace(' ', '_')
        e = e.strip().replace('#', 'num')
        e = e.strip().replace('?', '')
        e = e.strip().replace(',', '')
        e = e.strip().replace('"', '')
        e = e.strip().replace('(', '')
        e = e.strip().replace(')', '')
        e = e.strip().replace('.', '')
        if VAR_TYPES.has_key(e):
            if e == 'location_of_building_on_the_lot_if_garage_change_type_code_to_bgd':
                schema.append((e[:-7],VAR_TYPES[e]))
            else:
                schema.append((e,VAR_TYPES[e]))
        else:
            print e
    
    gp = open("%sinit_%s.sql" % (INIT_DIR, datasetName), 'w')

    # create SRC
    gp.write("DROP TABLE IF EXISTS SRC_%s;\n\n" % datasetName)
    gp.write("CREATE TABLE IF NOT EXISTS SRC_%s(\n" % datasetName)

    for v in schema:
        gp.write("%s %s,\n" % (v[0],v[1]))
    gp.write("dup_ver SERIAL,\n")
    gp.write("PRIMARY KEY(%s,dup_ver));\n\n" % bizKey)

    attrList = [v[0] for v in schema]
    attrListStr = ','.join(attrList)

    # load SRC

    gp.write("\copy SRC_%s (%s) FROM '%s%s_%s.csv' WITH (FORMAT CSV, HEADER true, DELIMITER ',')\n\n" % (datasetName, attrListStr, DEDUP_DIR, datasetName, LAST_DATE))
 
    # create DAT
    gp.write("DROP TABLE IF EXISTS DAT_%s;\n\n" % datasetName)
    gp.write("CREATE TABLE IF NOT EXISTS DAT_%s(\n" % datasetName)
    gp.write("%s_row_id SERIAL,\n" % datasetName)
    gp.write("start_date DATE,\n")
    gp.write("end_date DATE DEFAULT NULL,\n")
    gp.write("current_flag BOOLEAN DEFAULT true,\n")
    gp.write("dup_ver INTEGER,\n")
    for v in schema:
        gp.write("%s %s,\n" % (v[0],v[1]))
    gp.write("UNIQUE(%s,dup_ver),\n" % bizKey)
    gp.write("PRIMARY KEY(%s_row_id));\n\n" % datasetName)

    # insert DAT
    gp.write("INSERT INTO DAT_%s(\n" % datasetName)
    gp.write("start_date,\n")
    gp.write("%s\n" % attrListStr)
    gp.write(")\n")
    gp.write("SELECT '2014-03-29' AS start_date,\n")
    gp.write("%s\n" % attrListStr)
    gp.write("FROM SRC_%s;\n\n" % datasetName)

    # insert DAT_master

    gp.write("INSERT INTO DAT_Master(\n")
    gp.write("start_date,\n")
    gp.write("end_date,\n")
    gp.write("current_flag,\n")
    gp.write("location,\n")
    gp.write("latitude,\n")
    gp.write("longitude,\n")
    gp.write("obs_date,\n")
    gp.write("obs_ts,\n")
    gp.write("dataset_name,\n")
    gp.write("dataset_row_id,\n")
    gp.write("location_geom)\n")
    gp.write("SELECT\n")
    gp.write("start_date,\n")
    gp.write("end_date,\n")
    gp.write("current_flag,\n")
    gp.write("location,\n")
    gp.write("latitude,\n")
    gp.write("longitude,\n")
    gp.write("%s AS obs_date,\n" % obsName)
    gp.write("NULL AS obs_ts,\n")
    gp.write("'%s' AS dataset_name,\n" % datasetName)
    gp.write("%s_row_id AS dataset_row_id,\n" % datasetName)
    gp.write("ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)\n")
    gp.write("FROM\n")
    gp.write("DAT_%s;\n\n" % datasetName)




for (datasetName,bizKey,obsName) in DATASET_NAMES:
    process_dataset(datasetName,bizKey,obsName)
