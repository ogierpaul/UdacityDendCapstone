# Infogreffe Financial Statements
- Financial Statements and number of employees for french legal entities
- [Official Website](https://datainfogreffe.fr/offres)
- [API](https://opendata.datainfogreffe.fr/explore/dataset/chiffres-cles-2019/api/)
- [Description](https://opendata.datainfogreffe.fr/explore/dataset/chiffres-cles-2019/information/?dataChart=eyJxdWVyaWVzIjpbeyJjb25maWciOnsiZGF0YXNldCI6ImNoaWZmcmVzLWNsZXMtMjAxOSIsIm9wdGlvbnMiOnt9fSwiY2hhcnRzIjpbeyJhbGlnbk1vbnRoIjp0cnVlLCJ0eXBlIjoibGluZSIsImZ1bmMiOiJBVkciLCJ5QXhpcyI6ImNvZGVfcG9zdGFsIiwic2NpZW50aWZpY0Rpc3BsYXkiOnRydWUsImNvbG9yIjoiIzY2YzJhNSJ9XSwieEF4aXMiOiJkYXRlX2RlX2Nsb3R1cmVfZXhlcmNpY2VfMyIsIm1heHBvaW50cyI6IiIsInRpbWVzY2FsZSI6InllYXIiLCJzb3J0IjoiIn1dLCJkaXNwbGF5TGVnZW5kIjp0cnVlLCJhbGlnbk1vbnRoIjp0cnVlfQ%3D%3D)
- Published once a quarter
- 125 Mb in size

## Data description
At the Data Warehouse Level:

|column|description|
|---|---|
|`infogreffe_uid`|Primary Key, md5 hash of `siren`, `millesime`, `date_de_cloture`, `duree`|
|`siren`|Unique french legal entity identifier|
|`euvat`|Unique european legal entity identifier for VAT (Value-added Tax) purpose. Calculated from `siren`|
|millesime|Year of the financial statements|
|date_de_cloture|Date of the FS|
|duree|Duration in month of the period covered by the FS|
|tranche_ca_millesime|Tranch of revenue|
|ca|Revenue (if available)|
|resultat|Profit (if available)|
|effectif|Number of employees|

## Transformation applied
- Normalize the data
    - source data has multiple years for each row: `ca_1` and `ca_2` for year 1 & 2
    - the data is normalized to `ca`, and year is an attribute (un-pivot that data, like *melt* table in pandas)
- Calculate `euvat` from `siren`
- Deduplicate using `infogreffe_uid`
- Only select `duree` = 12 to maintain coherent comparison
- Only select `millesime` with 4 digits to remove incorrect values
- Only select rows which are present in the decp data (rows of interest)

