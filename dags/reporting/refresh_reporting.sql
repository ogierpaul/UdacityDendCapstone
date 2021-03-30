TRUNCATE dwh.fact_per_suppliername;

INSERT INTO dwh.fact_per_suppliername
SELECT sirenname, SUM(total_amount) as total_amount, SUM(n_contract) as n_contract
FROM (
SELECT siren, sum(montant) as total_amount, COUNT(*) as n_contract
FROM (
    SELECT decp_uid, montant
    FROM dwh.decp_marches
    WHERE montant < 99000000
    ) b
INNER JOIN (
    SELECT decp_uid, siren
    FROM dwh.decp_titulaires
    WHERE siren is not null
) c USING (decp_uid)
GROUP BY siren) d
LEFT JOIN (SELECT siren, sirenname FROM dwh.siren_attributes) e USING(siren)
WHERe sirenname IS NOT NULL
GROUP BY sirenname
ORDER BY total_amount DESC;


TRUNCATE dwh.fact_per_ess;

INSERT INTO dwh.fact_per_ess
SELECT ess, SUM(total_amount) as total_amount, SUM(n_contract) as n_contract
FROM
     (
SELECT siren, sum(montant) as total_amount, COUNT(*) as n_contract
FROM (
    SELECT decp_uid, montant
    FROM dwh.decp_marches
    WHERE montant < 99000000
    ) b
INNER JOIN (
    SELECT decp_uid, siren
    FROM dwh.decp_titulaires
    WHERE siren is not null
) c USING (decp_uid)
GROUP BY siren) d
LEFT JOIN (SELECT siren, economieSocialeSolidaireUniteLegale as ess FROM dwh.siren_attributes) e USING(siren)
GROUP BY ess
ORDER BY total_amount DESC
;

TRUNCATE dwh.contracts_year_month;

INSERT INTO dwh.contracts_year_month
SELECT year, year_month, COUNT(*) as n_contracts
FROM (SELECT datenotification FROM dwh.decp_marches where datenotification IS NOT NULL) a
INNER JOIN (SELECT date, year, year_month FROM dwh.time) b on b.date = a.datenotification
WHERE year>=2018 and year <=2021
GROUP BY year, year_month
ORDER BY year_month ASC;
