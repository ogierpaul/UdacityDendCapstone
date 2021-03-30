CREATE OR REPLACE VIEW datalake.decp_titulaires_trimmed_ids
AS
SELECT
    decp_bridge_uid,
    decp_uid,
     UPPER(REPLACE(REPLACE(REPLACE(TRIM(titulaire_id), ' ', ''), '.', ''), '-' ,'')) as titulaire_id,
   TRIM(titulaire_name) as titulaire_name,
     CASE
         WHEN UPPER(titulaire_typeidentifiant) = 'SIRET' THEN 'SIRET'
        WHEN UPPER(LEFT(titulaire_typeidentifiant, 3)) = 'TVA' THEN 'TVA'
    END as titulaire_typeidentifiant
FROM datalake.decp_titulaires
WHERE
      (NOT (titulaire_id IS NULL OR titulaire_id = '999999999' OR titulaire_id = ''))
    AND
      (titulaire_typeidentifiant = 'SIRET' OR Left(titulaire_typeidentifiant, 3) = 'TVA');

CREATE OR REPLACE VIEW datalake.decp_titulaires_valid_ids
AS
SELECT
decp_bridge_uid,
decp_uid,
titulaire_id,
   titulaire_name,
   CASE
       WHEN titulaire_typeidentifiant = 'TVA' THEN 'TVA'
        WHEN (titulaire_typeidentifiant = 'SIRET' AND LENGTH(titulaire_id) =14) THEN 'SIRET'
        WHEN (titulaire_typeidentifiant = 'SIRET' AND LENGTH(titulaire_id) =9) THEN 'SIREN'
    END as titulaire_typeidentifiant,
    CASE
       WHEN titulaire_typeidentifiant = 'TVA' AND LEFT(titulaire_id, 2) = 'FR' THEN SUBSTRING(titulaire_id, 5, 9)
        WHEN (titulaire_typeidentifiant = 'SIRET' AND LENGTH(titulaire_id) =14) THEN LEFT(titulaire_id, 9)
        WHEN (titulaire_typeidentifiant = 'SIRET' AND LENGTH(titulaire_id) =9) THEN titulaire_id
        ELSE NULL
    END as siren
FROM (
     SELECT *
     FROM datalake.decp_titulaires_trimmed_ids
     WHERE
         (
            titulaire_typeidentifiant = 'SIRET'
           AND (LENGTH (titulaire_id) = 14 OR LENGTH (titulaire_id) = 9)
           AND (titulaire_id ~ '^[0-9]*$')
       )
        OR
         (
         titulaire_typeidentifiant = 'TVA'
        AND titulaire_id ~ '^[A-z][A-z]'
        )
 ) b;

CREATE OR REPLACE VIEW datalake.decp_titulaires_standardized
AS
SELECT
    decp_bridge_uid,
       decp_uid,
       CASE
           WHEN titulaire_typeidentifiant = 'TVA' THEN titulaire_id
           WHEN siren IS NOT NULL THEN 'FR' || LPAD(
                            CAST(MOD(12 + 3 * MOD(CAST(siren AS INTEGER), 97), 97) AS VARCHAR), 2, '0')  || siren
        END as eu_vat,
       siren,
       CASE WHEN titulaire_typeidentifiant = 'SIRET' THEN titulaire_id ELSE NULL END as siret,
       titulaire_name,
       CASE
            WHEN titulaire_typeidentifiant = 'TVA' THEN LEFT(titulaire_id, 2)
            WHEN siren IS NOT NULL THEN 'FR'
           ELSE NULL
        END as titulaire_iso_country
FROM datalake.decp_titulaires_valid_ids;


CREATE TABLE IF NOT EXISTS dwh.decp_titulaires(
    decp_bridge_uid VARCHAR PRIMARY KEY ,
    decp_uid VARCHAR,
    euvat VARCHAR,
    siren varchar,
    siret VARCHAR,
    titulaire_name VARCHAR,
    titulaire_iso_country VARCHAR
);

CREATE OR REPLACE VIEW datalake.decp_marches_prepared AS
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
       "acheteur_name",
       CASE WHEN montant >= 99000000 THEN 1 ELSE 0 END as "suspicious_amount"
FROM datalake.decp_marches;

CREATE TABLE IF NOT EXISTS dwh.decp_marches
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
    "acheteur_name"          VARCHAR,
    "suspicious_amount"     INTEGER
);

CREATE TABLE IF NOT EXISTS dwh.decp_siren_used (
    siren VARCHAR PRIMARY KEY
);