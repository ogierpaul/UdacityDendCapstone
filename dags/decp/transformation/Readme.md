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


## Reference
- [Cpv Documentation](https://github.com/ogierpaul/UdacityDendCapstone/blob/master/docs/decp/Readme.md)
 