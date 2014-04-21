DROP TABLE IF EXISTS DAT_Meta3;

CREATE TABLE DAT_Meta3(
    dataset_name VARCHAR(100),
    attribute_name VARCHAR(200),
    attribute_value TEXT,
    PRIMARY KEY(dataset_name, attribute_name),
);

\copy DAT_Meta3(dataset_name, attribute_name, attribute_value) FROM '/project/evtimov/wopr/data/meta3.csv' WITH DELIMITER ',' CSV HEADER;
