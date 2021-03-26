siren,date_de_cloture_exercice_1,duree_1,ca_1,resultat_1,effectif_1,date_de_cloture_exercice_2,duree_2,ca_2,resultat_2,effectif_2


CREATE TABLE IF NOT EXISTS staging.infogreffe_attributes
(
    siren                      VARCHAR PRIMARY KEY,
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

-- CREATE TABLE IF NOT EXISTS staging.infogreffe
-- (
--     siren                      VARCHAR(9) PRIMARY KEY,
--     nic                        VARCHAR(6),
--     denomination               VARCHAR(256),
--     forme_juridique            VARCHAR(32),
--     code_ape                   VARCHAR(32),
--     libelle_ape                VARCHAR(256),
--     adresse                    VARCHAR(256),
--     code_postal                VARCHAR(8),
--     ville                      VARCHAR(256),
--     millesime               VARCHAR(64),
--     date_de_cloture_exercice VARCHAR(32),
--     duree                    VARCHAR(16),
--     tranche_ca_millesime     VARCHAR(64),
--     ca                       VARCHAR(64),
--     resultat                 VARCHAR(64),
--     effectif                 VARCHAR(64)
-- );

CREATE TABLE IF NOT EXISTS datalake.infogreffe_attributes
(
    LIKE staging.infogreffe_attributes
);