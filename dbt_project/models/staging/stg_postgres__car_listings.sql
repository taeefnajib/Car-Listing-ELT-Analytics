{{ config(materialized='table', schema='staging') }}

with source as (
    select * from {{ source('raw', 'car_listings') }}
)

select
    area,
    body_typex,
    brandx,
    conditionx,
    engine_capacityx,
    fuel_typex,
    kilometers_runx,
    listing_title,
    modelx,
    posted_on,
    price,
    registration_yearx,
    sold_by,
    subarea,
    transmissionx,
    url,
    views,
    year_of_manufacturex,
    _dlt_load_id,
    _dlt_id,
    trim_editionx
from source
