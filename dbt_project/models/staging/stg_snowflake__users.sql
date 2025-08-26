{{ config(materialized='table', schema='STAGING') }}

with source_users as (
    select * from {{ source('raw_data', 'users') }}
),

user_basic_metrics as (
    select 
        created_by as user_id,
        count(*) as tasks_created_count,
        max(created_at) as last_task_at
    from {{ source('raw_data', 'tasks') }}
    where created_by is not null
    group by 1
),

user_comments as (
    select 
        user_id,
        count(*) as comments_count,
        max(created_at) as last_comment_at
    from {{ source('raw_data', 'comments') }}
    where user_id is not null
    group by 1
)

select
    u.*,
    coalesce(um.tasks_created_count, 0) as tasks_created_count,
    coalesce(uc.comments_count, 0) as comments_count,
    greatest(
        u.last_login_at,
        um.last_task_at,
        uc.last_comment_at
    ) as last_activity_at
from source_users u
left join user_basic_metrics um on u.user_id = um.user_id
left join user_comments uc on u.user_id = uc.user_id 
