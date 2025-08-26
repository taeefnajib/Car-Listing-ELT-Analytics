{{ config(materialized='table', schema='STAGING') }}

with source_historical_subscriptions as (
    select * from {{ source('raw_data', 'historical_subscriptions') }}
),

historical_subscription_metrics as (
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
    from source_historical_subscriptions
)

select
    s.*,
    sm.mrr,
    sm.arr,
    sm.is_canceled
from source_historical_subscriptions s
left join historical_subscription_metrics sm on s.subscription_id = sm.subscription_id 