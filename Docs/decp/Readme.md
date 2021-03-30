# Données Essentielles de la Commande Publique (DECP)
## Description
- Contracts signed, description and their beneficiaries
- [Official Website](https://www.data.gouv.fr/fr/datasets/5cd57bf68b4c4179299eb0e9/)
- 300 Mb
- Updated daily

## decp_marches and decp_titulaires
- The raw data can be normalized in two tables
    - one containing the contract attributes: *decp_marches*
    - the other one detailing the supplier awarded *decp_titulaires*
- All `siren` attributes found in the contracts are stored in `decp_siren_used`.
    - At the data warehouse layer, this table is used to filter other data sources `infogreffe`, `siren` only on rows where this `siren` identifier is used

## decp_marches
|column|description|
|---|---|
|`decp_uid`|Unique identifier of the contract|
|`source`| Source of the contract. The data is coming from different data providers consolidated before the publication of the file|
|`decp_id`| Id of the contract for the source provider|
|`type`| Type of the contract|
|`nature`| Nature of the contract|
|`procedure`| Procedure used to award the contract|
|`objet`| description of the contract|
|`codecpv`| `codecpv`, classification system of the contract|
|`dureemois`| duration in month|
|`datenotification`| Date the contract was notified|
|`datepublicationdonnees`|  Date the data was published|
|`montant`| amount|
|`formeprix`| Type of price (fixed, ...) |
|`acheteur_id`| Id of the purchasing entity|
|`acheteur_name`| Name of the purhcasing entity |

## decp_titulaires
At the data warehouse level

|column|description|
|---|---|
|`decp_bridge_uid`|Primary key. Unique identifier of the (contract, titulaire) row. Md5 hash of `decp_uid`, `titulaire_typeidentifiant`, `titulaire_id`|
|`decp_uid`|Unique identifier of the contract|
|`euvat`|Unique european legal entity identifier for VAT (Value-added Tax) purpose. Calculated from `siren`, or given|
|`siren`|Unique french legal entity identifier. Only for french suppliers|
|`siret`|French local site identifier, derived from `siren`|
|`titulaire_name`|Name of the supplier as given in DECP|
|`titulaire_isocountry`|Country of the supplier, (ISO code, 2-digits), calculated from `euvat`|

### Transformations
- Converting the raw json array to Json lines format
- Extracting *decp_marches*, *decp_titulaires*, *decp_siren_used* from the raw Json file
- Only select `titulaire_typeidentifiant` = VAT , Siren, or Siret (removing exotic identifiers)
- Only select valid identifiers (not nulls, ..), respecting the standardized format (9 digits for `siren`, ..)
