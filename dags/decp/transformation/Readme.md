# Refresh DECP codes in the Data Warehouse
## Purpose
- Maintain up-to-date, quality DECP tables in the Data Warehouse Layer

## Transformations applied
### Decp Marches
- None

### Decp Titulaires
- Removing incorrect `titulairesid` rows
- Creating `euvat`, `siren`, `siret` columns using the `titulaireid` and `titulairetypeidentifiant` columns
    - ensure interoperability with the model
- Extracting `titulaire_iso_country` column

### Decp Siren Used
- Select distinct non-null siren in a table
    - this table will be used to select only those rows in `siren` and `infogreffe` tables

### Time
- Select DISTINCT `datepublicationdonnees` and `datenotification`
- Union the two above as a single date
- extract `month`, `year`, and `year_month` column


## Reference
- [Documentation](https://github.com/ogierpaul/UdacityDendCapstone/blob/master/docs/decp/Readme.md)
 