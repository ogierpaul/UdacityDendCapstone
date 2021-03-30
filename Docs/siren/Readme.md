# SIREN Base de données Stock (French commerce registry)
## Origin
- Classification and legal information on french firms & legal entities
- [URL](https://www.data.gouv.fr/fr/datasets/r/204d7dd9-8002-43b2-8cd1-6f6eaa47e4b0)
- [Description](https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/)
- [Official Website](https://www.data.gouv.fr/en/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/#_)
- Published once a month
- 2.6 Gb in Size
- 77 millions lines

## Data Description
At the Data Warehouse Level:

|column|description|
|---|---|
|`siren`|Unique french legal entity identifier, primary key|
|`euvat`|Unique european legal entity identifier for VAT (Value-added Tax) purpose. Calculated from `siren`|
|`name`|Concatenation of different possible name fields `nomUniteLegale`, `nomUsageUniteLegale`, `denominationUniteLegale`, `denominationUsuelle1UniteLegale`|
|`economieSocialeSolidaireUniteLegale`| If the legal entity has a special non-profit status|
|`trancheEffectifsUniteLegale`| Tranch of number of employees|
|`categorieentreprise`| |

## Transformations applied
- Calculation of `euvat`
- Calculation of `name`
- In the data warehouse layer, only select rows where the `siren` is found in `decp_siren_used` 