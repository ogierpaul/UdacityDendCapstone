-- This table is the same as the row json file, used to land COPY
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
    "acheteur_id"            VARCHAR,
    "acheteur_name"          VARCHAR
);

-- this table deduplicates the rows, convert to date the columns. Will be used for Upsert
CREATE OR REPLACE VIEW staging.decp_marches_unique AS
SELECT "decp_uid",
       "source",
       "decp_id",
       "type",
       "nature",
       "procedure",
       "objet",
       "codecpv",
       "dureemois",
       "datenotification",
       "datepublicationdonnees",
       "montant",
       "formeprix",
       "acheteur_id",
       "acheteur_name"
FROM (
         SELECT "decp_uid",
                "source",
                "decp_id",
                "type",
                "nature",
                "procedure",
                "objet",
                "codecpv",
                "dureemois",
                to_date(datenotification, 'YYYY-MM-DD')                                        as datenotification,
                to_date(datepublicationdonnees, 'YYYY-MM-DD')                                  as datepublicationdonnees,
                "montant"::DOUBLE PRECISION,
                "formeprix",
                "acheteur_id",
                "acheteur_name",
                ROW_NUMBER() OVER (PARTITION BY decp_uid ORDER BY datepublicationdonnees DESC) AS row_n
         FROM staging.decp_marches) h
where h.row_n = 1
;

-- this is the target table of the upsert
CREATE TABLE IF NOT EXISTS datalake.decp_marches
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
    "datenotification"       DATE,
    "datepublicationdonnees" DATE,
    "montant"                DOUBLE PRECISION,
    "formeprix"              varchar,
    "acheteur_id"            VARCHAR,
    "acheteur_name"          VARCHAR
);


-- this Table has the same structure as the JSON file. is the landing table of the COPY statement
CREATE TABLE IF NOT EXISTS staging.decp_titulaires
(
    "decp_uid"                  VARCHAR,
    "titulaire_id"              VARCHAR,
    "titulaire_name"            VARCHAR,
    "titulaire_typeidentifiant" VARCHAR
);

-- this table deduplicates the rows, using a hashed id. Will be used for Upsert
CREATE OR REPLACE VIEW staging.decp_titulaires_unique AS
SELECT decp_bridge_uid,
       decp_uid,
       titulaire_id,
       titulaire_name,
       titulaire_typeidentifiant
FROM (
         SELECT decp_bridge_uid,
                decp_uid,
                titulaire_id,
                titulaire_name,
                titulaire_typeidentifiant,
                ROW_NUMBER() OVER (PARTITION BY decp_bridge_uid ORDER BY length(titulaire_name) DESC) AS row_n
         FROM (
                  SELECT md5(coalesce(decp_uid, '') || coalesce(titulaire_typeidentifiant, '') ||
                             coalesce(titulaire_id, '')) as decp_bridge_uid,
                         decp_uid,
                         titulaire_id,
                         titulaire_name,
                         titulaire_typeidentifiant
                  FROM staging.decp_titulaires
              ) b) h
WHERE h.row_n = 1;

-- this is the target table of the upsert
CREATE TABLE IF NOT EXISTS datalake.decp_titulaires
(
    decp_bridge_uid           VARCHAR PRIMARY KEY,
    decp_uid                  VARCHAR,
    titulaire_id              VARCHAR,
    titulaire_name            VARCHAR,
    titulaire_typeidentifiant VARCHAR
);




