{{ config(materialized='table', schema='STAGING') }}

with source as (
    select * from {{ source('raw_data', 'comments') }}
)

select
    comment_id,
    task_id,
    user_id,
    content,
    created_at,
    parent_comment_id,
    mentions,
    attachments,
    updated_at,
    deleted_at
from source 
