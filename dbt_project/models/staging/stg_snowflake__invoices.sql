{{ config(materialized='table', schema='STAGING') }}

with source as (
    select * from {{ source('raw_data', 'invoices') }}
)

select
    invoice_id,
    subscription_id,
    workspace_id,
    amount_due,
    amount_paid,
    currency,
    status,
    invoice_pdf,
    billing_period_start,
    billing_period_end,
    created_at,
    due_date,
    paid_at
from source 
