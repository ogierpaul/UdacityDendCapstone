CREATE OR REPLACE VIEW datalake.infogreffe_unique_millesime AS
SELECT
       infogreffe_uid,
       siren,
       millesime,
       date_de_cloture,
       duree,
       tranche_ca_millesime,
       ca,
       resultat,
       effectif
FROM (
         SELECT *,
                row_number() over (PARTITION BY (siren_millesime) ORDER BY date_de_cloture DESC) AS row_n
         FROM (SELECT *, siren || millesime::VARCHAR AS siren_millesime
               FROM datalake.infogreffe_attributes
               WHERE duree = 12) b
     ) ranked
WHERE ranked.row_n = 1
;


CREATE OR REPLACE VIEW datalake.infogreffe_standardized AS
SELECT
infogreffe_uid,
siren,
'FR' || LPAD( CAST(MOD(12 + 3 * MOD(CAST(siren AS INTEGER), 97), 97) AS VARCHAR), 2, '0')  || siren as euvat,
millesime,
date_de_cloture,
duree,
tranche_ca_millesime,
ca,
resultat,
effectif
FROM
datalake.infogreffe_unique_millesime;

CREATE TABLE IF NOT EXISTS dwh.infogreffe_attributes (
    infogreffe_uid VARCHAR PRIMARY KEY ,
    siren VARCHAR,
    euvat VARCHAR,
    millesime INTEGER,
    date_de_cloture DATE,
    duree INTEGER,
    tranche_ca_millesime VARCHAR,
    ca FLOAT,
    resultat FLOAT,
    effectif FLOAT
);