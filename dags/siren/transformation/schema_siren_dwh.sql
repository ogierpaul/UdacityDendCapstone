CREATE OR REPLACE VIEW datalake.siren_standardized AS
SELECT siren,
       COALESCE(nomUniteLegale, '') || COALESCE(nomUsageUniteLegale, '') || COALESCE(denominationUniteLegale, '') ||
       COALESCE(denominationUsuelle1UniteLegale, '') as name,
       economieSocialeSolidaireUniteLegale,
       trancheEffectifsUniteLegale,
       categorieentreprise
-- 'FR' || LPAD( CAST(MOD(12 + 3 * MOD(CAST(siren AS INTEGER), 97), 97) AS VARCHAR), 2, '0')  || siren as euvat
FROM datalake.siren_attributes;


CREATE TABLE IF NOT EXISTS dwh.siren_attributes
(
    siren                               VARCHAR,
    name                                VARCHAR,
    economieSocialeSolidaireUniteLegale VARCHAR,
    trancheEffectifsUniteLegale         VARCHAR,
    categorieentreprise                 VARCHAR
);