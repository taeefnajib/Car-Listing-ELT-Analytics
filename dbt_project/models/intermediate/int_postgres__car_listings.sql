{{ config(materialized='table', schema='intermediate') }}

with source as (
    select * from {{ source('staging', 'stg_postgres__car_listings') }}
),

transformed as (
    select
        -- Split area by pilcrow (¶) and keep first part
        split_part(area, '¶', 1)::varchar as area,
        
        -- Rename columns and clean data
        body_typex::varchar as body_type,
        brandx::varchar as brand,
        conditionx::varchar as condition,
        
        -- Clean engine_capacity: remove 'cc' and commas, convert to int
        case 
            when engine_capacityx is null or trim(engine_capacityx) = '' then null
            else replace(replace(engine_capacityx, ' cc', ''), ',', '')::int
        end as engine_capacity,
        
        fuel_typex::varchar as fuel_type,
        
        -- Clean km_run: remove commas and ' km', convert to int
        case 
            when kilometers_runx is null or trim(kilometers_runx) = '' then null
            else replace(replace(kilometers_runx, ',', ''), ' km', '')::int
        end as km_run,
        
        listing_title::varchar as listing_title,
        modelx::varchar as model,
        
        -- Convert posted_on to datetime (handle DD/MM/YYYY HH24:MI format)
        case 
            when posted_on is null or trim(posted_on) = '' then null
            else to_timestamp(posted_on, 'DD/MM/YYYY HH24:MI')
        end as posted_on,
        
        -- Clean price: remove commas and 'Tk ', convert to int
        case 
            when price is null or trim(price) = '' then null
            else replace(replace(price, ',', ''), 'Tk ', '')::int
        end as price,
        
        -- Clean reg_year: remove commas, convert to int
        case 
            when registration_yearx is null then null
            else replace(registration_yearx::varchar, ',', '')::int
        end as reg_year,
        
        sold_by::varchar as sold_by,
        subarea::varchar as subarea,
        transmissionx::varchar as transmission,
        url::varchar as url,
        
        -- Clean views: remove commas, convert to int
        case 
            when views is null then null
            else replace(views::varchar, ',', '')::int
        end as views,
        
        -- Clean man_year: remove commas, convert to int
        case 
            when year_of_manufacturex is null then null
            else replace(year_of_manufacturex::varchar, ',', '')::int
        end as man_year,
        
        trim_editionx::varchar as trim_edition,
        
        -- Keep dlt columns as-is
        _dlt_load_id,
        _dlt_id
    from source
)

select * from transformed