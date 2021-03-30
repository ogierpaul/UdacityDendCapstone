

CREATE TABLE IF NOT EXISTS dwh.contracts_year_month (
    "year" INTEGER,
    "year_month" VARCHAR PRIMARY KEY ,
    n_contracts INTEGER
);


CREATE TABLE IF NOT EXISTS dwh.fact_per_ess (
    ess VARCHAR,
    total_amount FLOAT,
    n_contract INTEGER
);

CREATE TABLE IF NOT EXISTS dwh.fact_per_suppliername(
    sirenname VARCHAR ,
        total_amount FLOAT,
    n_contract INTEGER
);