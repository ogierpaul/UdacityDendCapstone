CREATE OR REPLACE VIEW datalake.siren_standardized AS
SELECT siren,
       'FR' || LPAD( CAST(MOD(12 + 3 * MOD(CAST(siren AS INTEGER), 97), 97) AS VARCHAR), 2, '0')  || siren as euvat,
       COALESCE(nomUniteLegale, '') || COALESCE(nomUsageUniteLegale, '') || COALESCE(denominationUniteLegale, '') || COALESCE(denominationUsuelle1UniteLegale, '') as sirenname,
       economieSocialeSolidaireUniteLegale,
       trancheEffectifsUniteLegale,
       categorieentreprise
FROM datalake.siren_attributes;


CREATE TABLE IF NOT EXISTS dwh.siren_attributes
(
    siren                               VARCHAR,
    euvat                               VARCHAR,
    sirenname                           VARCHAR,
    economieSocialeSolidaireUniteLegale VARCHAR,
    trancheEffectifsUniteLegale         VARCHAR,
    categorieentreprise                 VARCHAR
);