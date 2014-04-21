DROP TABLE IF EXISTS DAT_Meta2;

CREATE TABLE DAT_Meta2(
    dataset_name VARCHAR(100),
    attribute_name VARCHAR(200),
    attribute_description TEXT,
    PRIMARY KEY(dataset_name, attribute_name),
);

\copy DAT_Meta2(dataset_name, attribute_name, attribute_description) FROM '/project/evtimov/wopr/data/meta2.csv' WITH DELIMITER ',' CSV HEADER;
