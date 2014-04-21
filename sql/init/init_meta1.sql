DROP TABLE IF EXISTS DAT_Meta1;

CREATE TABLE DAT_Meta1(
    dataset_name VARCHAR(100),
    human_name VARCHAR(200),
    description TEXT,
    obs_from DATE,
    obs_to DATE,
    bbox GEOMETRY(POLYGON,4326),
    source_url VARCHAR(100),
    update_freq VARCHAR(100),
    PRIMARY KEY(dataset_name),
);

\copy DAT_Meta1(dataset_name, human_name, description, obs_from, obs_to, bbox, source_url, update_freq) FROM '/project/evtimov/wopr/data/meta1.csv' WITH DELIMITER ',' CSV HEADER;
