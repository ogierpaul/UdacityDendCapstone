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
    date_part('day', "date")::INTEGER as "day",
    date_part('month', "date")::INTEGER as "month",
    date_part('year', "date")::INTEGER as "year",
    date_part('year', "date")::TEXT || '-' ||lpad(date_part('month', "date")::TEXT, 2, '0') as "year_month"
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
    ) c;
