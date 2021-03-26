SELECT
       infogreffe_uid,
       siren,
       millesime,
       date_de_cloture,
       duree,
       tranche_ca_millesime,
       ca,
       resultat,
       effectif
FROM
staging.infogreffe_ranked
WHERE staging.infogreffe_ranked.row_n = 1;