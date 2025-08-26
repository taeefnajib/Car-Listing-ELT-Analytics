{{ config(materialized='table', schema='INTERMEDIATE') }}

with user_base as (
    select 
        user_id,
        first_name,
        last_name,
        full_name,
        email,
        department,
        title,
        created_at,
        updated_at,
        last_login_at,
        status,
        role,
        settings,
        deleted_at
    from {{ ref('stg_snowflake__users') }}
),

user_tasks_created as (
    select 
        created_by as user_id,
        count(*) as tasks_created_count,
        max(created_at) as last_task_created_at
    from {{ ref('stg_snowflake__tasks') }}
    where created_by is not null
    group by 1
),

user_tasks_assigned as (
    select 
        assigned_to as user_id,
        count(*) as tasks_assigned_count,
        max(updated_at) as last_task_assigned_at
    from {{ ref('stg_snowflake__tasks') }}
    where assigned_to is not null
    group by 1
),

user_comments as (
    select 
        user_id,
        count(*) as comments_count,
        max(created_at) as last_comment_at
    from {{ ref('stg_snowflake__comments') }}
    where user_id is not null
    group by 1
),

user_projects as (
    select 
        created_by as user_id,
        count(*) as projects_created_count,
        max(created_at) as last_project_created_at
    from {{ ref('stg_snowflake__projects') }}
    where created_by is not null
    group by 1
)

select
    u.*,
    coalesce(tc.tasks_created_count, 0) as tasks_created_count,
    coalesce(ta.tasks_assigned_count, 0) as tasks_assigned_count,
    coalesce(c.comments_count, 0) as comments_count,
    coalesce(p.projects_created_count, 0) as projects_created_count,
    greatest(
        u.last_login_at,
        tc.last_task_created_at,
        ta.last_task_assigned_at,
        c.last_comment_at,
        p.last_project_created_at
    ) as last_activity_at
from user_base u
left join user_tasks_created tc on u.user_id = tc.user_id
left join user_tasks_assigned ta on u.user_id = ta.user_id
left join user_comments c on u.user_id = c.user_id
left join user_projects p on u.user_id = p.user_id 