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


CREATE TABLE IF NOT EXISTS datalake.decp_marches( LIKE staging.decp_marches);

CREATE TABLE IF NOT EXISTS staging.decp_titulaires (
    "decp_uid" VARCHAR,
    "titulaire_id" VARCHAR,
    "titulaire_name" VARCHAR,
    "titulaire_typeidentifiant" VARCHAR
);

CREATE TABLE IF NOT EXISTS datalake.decp_titulaires (
    decp_bridge_uid VARCHAR PRIMARY KEY ,
    decp_uid VARCHAR,
    titulaire_id VARCHAR,
    titulaire_name VARCHAR,
    titulaire_typeidentifiant VARCHAR
);