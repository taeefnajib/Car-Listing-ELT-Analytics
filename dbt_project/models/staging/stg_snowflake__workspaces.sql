{{ config(materialized='table', schema='STAGING') }}

with source_workspaces as (
    select * from {{ source('raw_data', 'workspaces') }}
),

workspace_basic_metrics as (
    select 
        workspace_id,
        count(*) as total_projects,
        count(case when status = 'completed' then 1 end) as completed_projects,
        count(distinct created_by) as total_users
    from {{ source('raw_data', 'projects') }}
    where deleted_at is null
    group by 1
)

select
    w.*,
    coalesce(wm.total_projects, 0) as total_projects,
    coalesce(wm.completed_projects, 0) as completed_projects,
    coalesce(wm.total_users, 0) as total_users
from source_workspaces w
left join workspace_basic_metrics wm on w.workspace_id = wm.workspace_id 
