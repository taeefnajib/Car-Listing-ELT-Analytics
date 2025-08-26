{{ config(materialized='table', schema='STAGING') }}

with source as (
    select * from {{ source('raw_data', 'tasks') }}
)

select
    task_id,
    project_id,
    title,
    description,
    created_by,
    assigned_to,
    created_at,
    due_date,
    status,
    priority,
    estimate_hours,
    parent_task_id,
    labels,
    attachments,
    updated_at,
    completed_at,
    deleted_at
from source 
