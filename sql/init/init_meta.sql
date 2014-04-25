DROP TABLE IF EXISTS meta_attributes;

DROP TABLE IF EXISTS meta_misc;

DROP TABLE IF EXISTS meta_master;


CREATE TABLE meta_master(
    dataset_name VARCHAR(100),
    human_name VARCHAR(200),
    description TEXT,
    source_url VARCHAR(100),
    obs_from DATE,
    obs_to DATE,
    bbox GEOMETRY(POLYGON,4326),
    update_freq VARCHAR(100),
    PRIMARY KEY(dataset_name)
);

\copy meta_master(dataset_name, human_name, description, source_url, obs_from, obs_to, bbox, update_freq) FROM '../../meta/meta_master.csv' WITH DELIMITER ',' CSV HEADER;


CREATE TABLE meta_attributes(
    dataset_name VARCHAR(100),
    attribute_name VARCHAR(200),
    attribute_description TEXT,
    PRIMARY KEY(dataset_name, attribute_name),
    FOREIGN KEY(dataset_name) REFERENCES meta_master(dataset_name)
);

\copy meta_attributes(dataset_name, attribute_name, attribute_description) FROM '../../meta/meta_attributes.csv' WITH DELIMITER ',' CSV HEADER;


CREATE TABLE meta_misc(
    dataset_name VARCHAR(100),
    meta_attribute_name VARCHAR(200),
    meta_attribute_value TEXT,
    PRIMARY KEY(dataset_name, meta_attribute_name),
    FOREIGN KEY(dataset_name) REFERENCES meta_master(dataset_name)
);

\copy meta_misc(dataset_name, meta_attribute_name, meta_attribute_value) FROM '../../meta/meta_misc.csv' WITH DELIMITER ',' CSV HEADER;
