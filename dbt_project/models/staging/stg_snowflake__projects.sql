{{ config(materialized='table', schema='STAGING') }}

with source_projects as (
    select * from {{ source('raw_data', 'projects') }}
),

project_basic_metrics as (
    select 
        project_id,
        count(*) as total_tasks,
        count(case when status = 'completed' then 1 end) as completed_tasks,
        min(created_at) as first_task_created_at,
        max(completed_at) as last_task_completed_at
    from {{ source('raw_data', 'tasks') }}
    where deleted_at is null
    group by 1
)

select
    p.*,
    coalesce(pm.total_tasks, 0) as total_tasks,
    coalesce(pm.completed_tasks, 0) as completed_tasks,
    case 
        when coalesce(pm.total_tasks, 0) = 0 then 0
        else round(100.0 * coalesce(pm.completed_tasks, 0) / pm.total_tasks, 2)
    end as completion_rate,
    pm.first_task_created_at,
    pm.last_task_completed_at
from source_projects p
left join project_basic_metrics pm on p.project_id = pm.project_id 
