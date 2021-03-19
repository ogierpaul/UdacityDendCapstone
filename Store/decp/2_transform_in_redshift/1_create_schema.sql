CREATE SCHEMA IF NOT EXISTS staging;

DROP TABLE IF EXISTS staging.decp_marches;

CREATE TABLE IF NOT EXISTS staging.decp_marches
(
    "decp_uid"               VARCHAR PRIMARY KEY,
    "source"                 varchar,
    "decp_id"                VARCHAR,
    "type"                   varchar,
    "nature"                 varchar,
    "procedure"              varchar,
    "objet"                  VARCHAR,
    "codecpv"                varchar,
    "dureemois"              INTEGER,
    "datenotification"       VARCHAR,
    "datepublicationdonnees" VARCHAR,
    "montant"                DOUBLE PRECISION,
    "formeprix"              varchar,
    "acheteur_id" VARCHAR,
    "acheteur_name" VARCHAR
);

DROP TABLE IF EXISTS datalake.decp_marches;

CREATE TABLE IF NOT EXISTS datalake.decp_marches( LIKE staging.decp_marches);

DROP TABLE IF EXISTS staging.decp_titulaires;

CREATE TABLE IF NOT EXISTS staging.decp_titulaires (
    "decp_uid" VARCHAR,
    "titulaire_id" VARCHAR,
    "titulaire_name" VARCHAR,
    "titulaire_typeidentifiant" VARCHAR
);

CREATE TABLE IF NOT EXISTS datalake.decp_titulaires (LIKE staging.decp_titulaires);

DROP TABLE IF EXISTS staging.decp_titulaires_formatted;

CREATE TABLE IF NOT EXISTS staging.decp_titulaires_formatted (
    "decp_uid" VARCHAR(128),
    "eu_vat" VARCHAR(64),
    "siren" VARCHAR(9),
    "siret" VARCHAR(14),
    "titulaire_name" VARCHAR(256)
);


DROP TABLE IF EXISTS datalake.decp_awarded;

CREATE TABLE IF NOT EXISTS datalake.decp_awarded(
    "decp_uid" VARCHAR,
    "eu_vat" VARCHAR,
    "siren" VARCHAR,
    "siret" VARCHAR,
    "titulaire_name" VARCHAR,
    "countrycode" VARCHAR,
    PRIMARY KEY (decp_uid, eu_vat)
);
