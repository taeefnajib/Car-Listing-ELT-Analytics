{{ config(materialized='table', schema='STAGING') }}

with source as (
    select * from {{ source('raw_data', 'payments') }}
)

select
    payment_id,
    invoice_id,
    workspace_id,
    amount,
    currency,
    status,
    payment_method,
    created_at,
    failure_reason,
    refunded,
    refunded_at,
    metadata
from source 
