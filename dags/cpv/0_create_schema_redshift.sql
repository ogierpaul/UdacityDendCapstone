CREATE TABLE IF NOT EXISTS staging.cpv_attributes (
    codecpv VARCHAR(8) PRIMARY KEY,
    description VARCHAR
);

CREATE TABLE IF NOT EXISTS datalake.cpv_attributes (LIKE staging.cpv_attributes);
