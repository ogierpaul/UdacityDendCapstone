TRUNCATE dwh.infogreffe_attributes;

INSERT INTO dwh.infogreffe_attributes
SELECT * FROM datalake.infogreffe_standardized
INNER JOIN (SELECT siren FROM dwh.decp_siren_used) b USING(siren);