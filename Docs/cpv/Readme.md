# CPV contracting codes
- Classification of type of data
- [Official Website](https://simap.ted.europa.eu/cpv)
- Excel file format at first, transformed to csv
- Changing very slowly, last update 2013

## Data description
At the Data Warehouse Level:

|column|description|
|---|---|
|`codecpv`|9-digits code, unique identifier|
|`description`|description of code, in english, free text|

## Transformations applied
- None

## Future improvments
- The cpv code has a hierarchical structure, i.e. codes are grouped within a family
    - i.e. code 123456789 and code 123456780 are related and belong to the same family 123456
- a nice future improvment would be to get the description of this family 