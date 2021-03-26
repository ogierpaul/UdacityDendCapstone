SELECT decp_bridge_uid,
       decp_uid,
       titulaire_id,
       titulaire_name,
       titulaire_typeidentifiant
FROM
staging.decp_titulaires_ranked
WHERE staging.decp_titulaires_ranked.row_n = 1;