{{ config(materialized='table', schema='INTERMEDIATE') }}

with payment_base as (
    select * from {{ ref('stg_snowflake__payments') }}
),

payment_method_stats as (
    select 
        payment_method,
        count(*) as total_attempts,
        count(case when status = 'succeeded' then 1 end) as successful_attempts,
        round(100.0 * count(case when status = 'succeeded' then 1 end) / count(*), 2) as success_rate
    from {{ ref('stg_snowflake__payments') }}
    group by 1
),

invoice_payment_attempts as (
    select 
        invoice_id,
        count(*) as attempt_number
    from {{ ref('stg_snowflake__payments') }}
    group by 1
)

select
    p.*,
    pms.success_rate as payment_success_rate,
    ipa.attempt_number as retry_count,
    case 
        when p.status = 'succeeded' and p.refunded = false
        then datediff('second', p.created_at, p.created_at) -- assuming immediate processing for successful payments
        when p.status = 'failed'
        then datediff('second', p.created_at, p.created_at) + 60 -- assuming 1 minute for failed payments
        when p.refunded = true
        then datediff('second', p.created_at, p.refunded_at)
    end as processing_time_seconds
from payment_base p
left join payment_method_stats pms on p.payment_method = pms.payment_method
left join invoice_payment_attempts ipa on p.invoice_id = ipa.invoice_id 