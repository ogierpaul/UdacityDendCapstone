SELECT decp_bridge_uid,
       decp_uid,
       titulaire_id,
       titulaire_name,
       titulaire_typeidentifiant
FROM
(SELECT decp_bridge_uid,
        decp_uid,
        titulaire_id,
        titulaire_name,
        titulaire_typeidentifiant,
        ROW_NUMBER()  OVER (PARTITION BY decp_bridge_uid ORDER BY decp_uid) AS row_n
 FROM (SELECT md5(coalesce(decp_uid, '') || coalesce(titulaire_typeidentifiant, '') || coalesce(titulaire_id, '') ) as decp_bridge_uid, decp_uid, titulaire_id, titulaire_name, titulaire_typeidentifiant
FROM  staging.decp_titulaires) h
) AS ranked
WHERE ranked.row_n = 1;