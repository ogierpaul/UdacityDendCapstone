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
       "datenotification",
       "datepublicationdonnees",
       "montant",
       "formeprix",
       "acheteur_id" ,
       "acheteur_name"
FROM staging.decp_marches_ranked
WHERE staging.decp_marches_ranked.row_n = 1;