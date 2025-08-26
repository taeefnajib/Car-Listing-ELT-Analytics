{{ config(materialized='table', schema='INTERMEDIATE') }}

with task_base as (
    select * from {{ ref('stg_snowflake__tasks') }}
),

task_comments as (
    select 
        task_id,
        count(*) as comments_count,
        max(created_at) as last_comment_at
    from {{ ref('stg_snowflake__comments') }}
    where deleted_at is null
    group by 1
),

task_subtasks as (
    select 
        parent_task_id,
        count(*) as subtasks_count,
        count(case when status = 'completed' then 1 end) as completed_subtasks
    from {{ ref('stg_snowflake__tasks') }}
    where parent_task_id is not null
    and deleted_at is null
    group by 1
)

select
    t.*,
    case 
        when t.completed_at is not null 
        then datediff('hour', t.created_at, t.completed_at)
    end as time_to_complete,
    coalesce(tc.comments_count, 0) as comments_count,
    coalesce(ts.subtasks_count, 0) as subtasks_count,
    case
        when t.status = 'completed' then 'completed'
        when t.due_date < current_timestamp then 'overdue'
        when t.due_date < dateadd(day, 1, current_timestamp) then 'due_today'
        when t.due_date < dateadd(day, 7, current_timestamp) then 'due_this_week'
        else 'upcoming'
    end as completion_status,
    greatest(
        t.updated_at,
        tc.last_comment_at
    ) as last_activity_at
from task_base t
left join task_comments tc on t.task_id = tc.task_id
left join task_subtasks ts on t.task_id = ts.parent_task_id 