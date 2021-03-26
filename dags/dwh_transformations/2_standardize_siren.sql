CREATE OR REPLACE VIEW datalake.siren_standardized AS
SELECT
siren,
COALESCE(nomUniteLegale, '') || COALESCE(nomUsageUniteLegale, '') || COALESCE(denominationUniteLegale, '') || COALESCE(denominationUsuelle1UniteLegale, '') as name,
economieSocialeSolidaireUniteLegale,
trancheEffectifsUniteLegale,
categorieentreprise,
'FR' || LPAD( CAST(MOD(12 + 3 * MOD(CAST(siren AS INTEGER), 97), 97) AS VARCHAR), 2, '0')  || siren as euvat
FROM
datalake.siren_attributes;


CREATE MATERIALIZED VIEW  IF NOT EXISTS dwh.siren_standardized AS
SELECT * FROM datalake.siren_standardized
INNER JOIN (SELECT siren FROM dwh.decp_distinct_siren) b USING(siren);

REFRESH MATERIALIZED VIEW dwh.siren_standardized;