CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.cpv_attributes AS
    SELECT * FROM datalake.cpv_attributes;

REFRESH MATERIALIZED VIEW dwh.cpv_attributes;