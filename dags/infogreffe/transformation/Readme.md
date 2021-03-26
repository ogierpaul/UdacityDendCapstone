# Refresh Infogreffe in the Data Warehouse
## Purpose
- Maintain up-to-date, quality Infogreffe financial statements in the Data Warehouse Layer

## Transformations applied
- Select only the rows for a full financial year (`duree` = 12)
- Deduplicate the data based on latest closure date (`date_cloture`)
- Select only the rows that are present in the DECP data