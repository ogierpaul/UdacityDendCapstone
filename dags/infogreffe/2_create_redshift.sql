CREATE TABLE IF NOT EXISTS staging.infogreffe_attributes
(
    siren                      VARCHAR,
    millesime_1                VARCHAR,
    date_de_cloture_exercice_1 VARCHAR,
    duree_1                    VARCHAR,
    tranche_ca_millesime_1     VARCHAR,
    ca_1                       VARCHAR,
    resultat_1                 VARCHAR,
    effectif_1                 VARCHAR,
    millesime_2                VARCHAR,
    date_de_cloture_exercice_2 VARCHAR,
    duree_2                    VARCHAR,
    tranche_ca_millesime_2     VARCHAR,
    ca_2                       VARCHAR,
    resultat_2                 VARCHAR,
    effectif_2                 VARCHAR
);

CREATE TABLE IF NOT EXISTS datalake.infogreffe_attributes (
    infogreffe_uid VARCHAR,
    siren VARCHAR,
    millesime INTEGER,
    date_de_cloture date,
    duree INTEGER,
    tranche_ca_millesime VARCHAR,
    ca FLOAT,
    resultat FLOAT,
    effectif FLOAT
);


CREATE OR REPLACE VIEW staging.infogreffe_normalized AS
SELECT
    md5(siren || millesime || date_de_cloture || duree) as infogreffe_uid,
    siren,
    millesime::INTEGER,
    date_de_cloture::DATE,
    duree::INTEGER,
    tranche_ca_millesime,
    ca::FLOAT,
    resultat::FLOAT,
    effectif::FLOAT
FROM (
SELECT
       siren,
       millesime_1 as millesime,
       date_de_cloture_exercice_1 as date_de_cloture,
       duree_1 as duree,
       tranche_ca_millesime_1 as tranche_ca_millesime,
       ca_1 as ca,
       resultat_1 as resultat,
       effectif_1 as effectif
FROM staging.infogreffe_attributes
WHERE LENGTH(millesime_1) =4
UNION
(SELECT
       siren,
       millesime_2 as millesime,
       date_de_cloture_exercice_2 as date_de_cloture,
       duree_2 as duree,
       tranche_ca_millesime_2 as tranche_ca_millesime,
       ca_2 as ca,
       resultat_2 as resultat,
       effectif_2 as effectif
FROM staging.infogreffe_attributes
WHERE LENGTH(millesime_2) =4 )
    ) b;

CREATE OR REPLACE VIEW staging.infogreffe_ranked AS
SELECT
       infogreffe_uid,
       siren,
       millesime,
       date_de_cloture,
       duree,
       tranche_ca_millesime,
       ca,
       resultat,
       effectif,
       ROW_NUMBER()  OVER (PARTITION BY infogreffe_uid ORDER BY duree DESC, date_de_cloture DESC) AS row_n
FROM staging.infogreffe_normalized;
