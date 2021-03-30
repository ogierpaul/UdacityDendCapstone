# Project Description
## Introduction
### Purpose
- Provide descriptive statistics on the french government procurement.
    - tenders lanched
    - beneficiaries of contracts
- Benefits:
    - understand who is contracting with the french government
    - find potential savings in identifying new suppliers
    - fraud & risk analyzis on the suppliers
    - monitor and improve the procurement strategy

## Architecture
### Functional Architecture: Data Sources used
#### Identification of data source needed
- We need both transactional / fact data (the contracts passed) and dimensional / master data (The attributes)
- On the dimensions, three main dimensions are interesting:
    - time
    - Supplier
    - Contract object / goods or service type
    
#### Data source description
- DECP (*Données Essentielles de la Commande Publique*)
    - Contracts signed, description and their beneficiaries
    - [Official Website](https://www.data.gouv.fr/fr/datasets/5cd57bf68b4c4179299eb0e9/)
    - Fact data: Axis: Contracts & Suppliers, Time
- Siren (French commerce registry)
    - Classification and legal information on french firms & legal entities
    - [Official Website](https://www.data.gouv.fr/en/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/#_)
    - Dimension: Axis: Suppliers
- Infogreffe (Financial Statements)
    - Financial Statements and number of employees for french legal entities
    - [Official Website](https://datainfogreffe.fr/offres)
    - Dimension: Axis: Supplier
- CPV (Goods and Service classification)
    - The CPV establishes a single european classification system for public procurement
    - [Official Website](https://simap.ted.europa.eu/cpv)
    - Dimension, Axis: Procurement Contracts
    
High Level Functional View
![HighLevelFunctionalView](https://github.com/ogierpaul/UdacityDendCapstone/blob/master/docs/images/high_level_functional_view.jpg)

### Technical Architecture Justification
#### Data sources technology
- API (Infogreffe)
- Files to be downloaded from the web (DECP, Siren)
- Local excel file transformed in csv (CPV)
    
#### Data flow
- Download from web >> copy to S3 >> staging area on Redshift >> datalake (Redshift) >> Data Warehouse (Redshift)

![HighLevelTechnicalView](https://github.com/ogierpaul/UdacityDendCapstone/blob/master/docs/images/high_level_architecture.jpg)


#### Redshift as DB Backend
- Redshift: column-oriented DBMS
- A bit overkill for the project (Redshift is for Tera-bytes of data, but here the max size of the data is 10 Gb)
- But since most of the DEND was around Redshift, it makes sense to showcase Redshift capacities
- Going forward, I would use either more traditional RDMS (Postgres, ...), or more flexible technologies (Snowflake)
- In particular, Redshift is not very flexible in manipulating JSON, and does not easily handles Merge
- 3 Schemas in Redshift:
    - Staging: For copying the files from S3
    - Datalake: copy as-is of the staging area, with some minor technical transformation (removal of duplicates, casting to the right type)
    - Data Warehouse (dwh): Data is prepared in a business context, is standardized, selected only according to relevant business filters
    
#### S3 as a staging area
- S3 has great file access functionalities with all of AWS suite
    - Easy to upload, copy, move files with S3
    - Handles large files

#### EC2 machines to perform file manipulation
- Unfortunately, the data sources chosen are NOT very standard:
    - The files are either zipped, or contained in a JSON array, etc...
    - It is necessary to unzip, or format to JSON Lines format before ingestion into Redshift
- I chose to use EC2 machines with bash operator to perform those actions
- Why not Airflow Bash Operator?
    - Because it uses my local computer
    - Because it adds additional processing on my local Airflow installation which is already quite demanding
    - And because my computer has low bandwith, has an unstable environment (depending on the projects I am working on)
    - etc...
- On the other hand, EC2 machines have the following advantages:
    - They are fast
    - They can be dedicated to one task only, have a reliable environment (vs my local computer where the software stack  is not stable)
    - They have great connectivity and bandwith, especially within the same AWS region (useful since I use a lot of bandwith during file transfers)
    - They can be turned off and on (Terminated, stopped and restarted) at will and thus costs can be minimized
    - They have less downtime than my Macbook
    - I only need to run those statements once a week, so the costs are fairly low

## Results
TODO: Write Results

    