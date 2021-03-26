TRUNCATE dwh.cpv_attributes;

INSERT INTO dwh.cpv_attributes
SELECT * FROM datalake.cpv_attributes;