{{ config(materialized='table', schema='STAGING') }}

with source_subscriptions as (
    select * from {{ source('raw_data', 'subscriptions') }}
),

subscription_basic_metrics as (
    select 
        subscription_id,
        amount as mrr,
        amount * 12 as arr,
        status,
        case 
            when canceled_at is not null 
            and canceled_at <= current_timestamp 
            then true 
            else false 
        end as is_canceled
    from source_subscriptions
)

select
    s.*,
    sm.mrr,
    sm.arr,
    sm.is_canceled
from source_subscriptions s
left join subscription_basic_metrics sm on s.subscription_id = sm.subscription_id 
