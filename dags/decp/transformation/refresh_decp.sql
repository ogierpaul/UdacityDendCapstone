TRUNCATE dwh.decp_titulaires;

INSERT INTO dwh.decp_titulaires
SELECT * FROM datalake.decp_titulaires_standardized;


TRUNCATE dwh.decp_marches;

INSERT INTO dwh.decp_marches
SELECT * FROM datalake.decp_marches_prepared;

TRUNCATE dwh.decp_siren_used;

INSERT INTO dwh.decp_siren_used
SELECT DISTINCT siren FROM dwh.decp_titulaires
WHERE siren IS NOT NULL;

TRUNCATE dwh.time;

INSERT INTO dwh.time
SELECT
    "date",
    month("date") as "month",
    year("date") as "year",
    year("date")::TEXT || '-' ||month("date")::TEXT as "year_month"
FROM (
    SELECT DISTINCT datenotification as "date"
    FROM dwh.decp_marches
    WHERE dwh.decp_marches.datenotification IS NOT NULL
    UNION
    (
        SELECT DISTINCT datepublicationdonnees as "date"
        FROM dwh.decp_marches
        WHERE dwh.decp_marches.datepublicationdonnees IS NOT NULL
    )
    ) c
