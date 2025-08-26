{{ config(materialized='table', schema='INTERMEDIATE') }}

with invoice_base as (
    select * from {{ ref('stg_snowflake__invoices') }}
),

invoice_payments as (
    select 
        invoice_id,
        sum(amount) as total_paid,
        min(created_at) as first_payment_at,
        max(created_at) as last_payment_at,
        count(*) as payment_attempts
    from {{ ref('stg_snowflake__payments') }}
    group by 1
)

select
    i.*,
    case 
        when i.paid_at is not null 
        then datediff('day', i.created_at, i.paid_at)
    end as days_to_pay,
    case
        when i.status = 'paid' then 'paid'
        when i.due_date < current_timestamp then 'overdue'
        else 'pending'
    end as payment_status,
    i.amount_due - i.amount_paid as total_adjustments,
    case 
        when i.due_date < current_timestamp and i.status != 'paid' 
        then true 
        else false 
    end as is_overdue,
    coalesce(ip.payment_attempts, 0) as payment_attempts,
    ip.first_payment_at,
    ip.last_payment_at
from invoice_base i
left join invoice_payments ip on i.invoice_id = ip.invoice_id 