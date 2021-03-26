TRUNCATE dwh.decp_titulaires;

INSERT INTO dwh.decp_titulaires
SELECT * FROM datalake.decp_titulaires_standardized;


TRUNCATE dwh.decp_marches;

INSERT INTO dwh.decp_marches
SELECT * FROM datalake.decp_marches;

TRUNCATE dwh.decp_siren_used;

INSERT INTO dwh.decp_siren_used
SELECT DISTINCT siren FROM dwh.decp_titulaires
WHERE siren IS NOT NULL;