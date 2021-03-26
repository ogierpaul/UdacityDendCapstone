CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.decp_marches
AS
    SELECT * FROM datalake.decp_marches;