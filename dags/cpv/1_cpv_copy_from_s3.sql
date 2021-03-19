TRUNCATE staging.cpv_attributes;

COPY staging.cpv_attributes
FROM 's3'
IAM_ROLE AS {arn}
REGION {region}
COMPUPDATE OFF
TIMEFORMAT as 'epochmillisecs'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
FORMAT AS JSON 'auto';