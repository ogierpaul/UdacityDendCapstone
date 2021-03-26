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


CREATE OR REPLACE VIEW staging.decp_titulaires_ranked AS
SELECT decp_bridge_uid,
        decp_uid,
        titulaire_id,
        titulaire_name,
        titulaire_typeidentifiant,
        ROW_NUMBER()  OVER (PARTITION BY decp_bridge_uid ORDER BY length(titulaire_name) DESC) AS row_n
 FROM (
     SELECT
    md5(coalesce(decp_uid, '') || coalesce(titulaire_typeidentifiant, '') || coalesce(titulaire_id, '') ) as decp_bridge_uid,
    decp_uid,
    titulaire_id,
    titulaire_name,
    titulaire_typeidentifiant
FROM  staging.decp_titulaires
     ) b;

CREATE OR REPLACE VIEW staging.decp_marches_ranked AS
SELECT
       "decp_uid",
       "source",
       "decp_id",
       "type",
       "nature",
       "procedure",
       "objet",
       "codecpv",
       "dureemois",
       "datenotification"::DATE,
       "datepublicationdonnees"::DATE,
       "montant"::DOUBLE PRECISION,
       "formeprix",
       "acheteur_id" ,
       "acheteur_name",
        ROW_NUMBER() OVER (PARTITION BY decp_uid ORDER BY datepublicationdonnees DESC) AS row_n
FROM staging.decp_marches;