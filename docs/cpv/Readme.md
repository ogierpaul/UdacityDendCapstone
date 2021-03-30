# CPV contracting codes
- Classification of type of data
- [Official Website](https://simap.ted.europa.eu/cpv)
- Excel file format at first, transformed to csv
- Changing very slowly, last update 2013
- Number of rows: 9k
- size: 330kb

## Dag
### Ingestion
![IngestCPVDag](https://github.com/ogierpaul/UdacityDendCapstone/blob/master/dags/cpv/ingestion/ingest_cpv_dag.png)

### Transformation
![Transformation](https://github.com/ogierpaul/UdacityDendCapstone/blob/master/dags/cpv/transformation/refresh_cpv_dag.png)

## Data description
At the Data Warehouse Level:

|column|description|
|---|---|
|`codecpv`|8-digits code, unique identifier|
|`description`|description of code, in english, free text|

## Transformations applied
- No blocking data quality issue
- No transformations

## Future improvments
- The cpv code has a hierarchical structure, i.e. codes are grouped within a family
    - i.e. code 12345678 and code 12345670 are related and belong to the same family 123456
- a nice future improvment would be to get the description of this family 