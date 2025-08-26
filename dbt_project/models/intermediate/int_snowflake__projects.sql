{{ config(materialized='table', schema='INTERMEDIATE') }}

with project_base as (
    select 
        project_id,
        workspace_id,
        name,
        description,
        created_by,
        created_at,
        due_date,
        status,
        priority,
        is_private,
        updated_at,
        completed_at,
        deleted_at
    from {{ ref('stg_snowflake__projects') }}
),

project_tasks as (
    select 
        project_id,
        count(*) as total_tasks,
        count(case when status = 'completed' then 1 end) as completed_tasks,
        avg(case 
            when completed_at is not null 
            then datediff('hour', created_at, completed_at) 
        end) as avg_completion_hours
    from {{ ref('stg_snowflake__tasks') }}
    where deleted_at is null
    group by 1
),

project_comments as (
    select 
        p.project_id,
        count(c.comment_id) as total_comments
    from {{ ref('stg_snowflake__projects') }} p
    left join {{ ref('stg_snowflake__tasks') }} t on p.project_id = t.project_id
    left join {{ ref('stg_snowflake__comments') }} c on t.task_id = c.task_id
    where c.deleted_at is null
    group by 1
)

select
    p.*,
    coalesce(pt.total_tasks, 0) as total_tasks,
    coalesce(pt.completed_tasks, 0) as completed_tasks,
    case 
        when coalesce(pt.total_tasks, 0) = 0 then 0
        else round(100.0 * coalesce(pt.completed_tasks, 0) / pt.total_tasks, 2)
    end as completion_rate,
    coalesce(pt.avg_completion_hours, 0) as avg_task_completion_time,
    coalesce(pc.total_comments, 0) as total_comments
from project_base p
left join project_tasks pt on p.project_id = pt.project_id
left join project_comments pc on p.project_id = pc.project_id 