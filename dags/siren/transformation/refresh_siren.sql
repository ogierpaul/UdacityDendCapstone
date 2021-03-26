TRUNCATE dwh.siren_attributes;

INSERT INTO dwh.siren_attributes
SELECT * FROM datalake.siren_standardized
INNER JOIN (SELECT siren FROM dwh.decp_siren_used) b USING(siren);