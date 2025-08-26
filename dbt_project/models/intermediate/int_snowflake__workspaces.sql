{{ config(materialized='table', schema='INTERMEDIATE') }}

with workspace_base as (
    select 
        workspace_id,
        name,
        description,
        created_by,
        created_at,
        plan_tier,
        settings,
        max_members,
        status,
        updated_at,
        deleted_at
    from {{ ref('stg_snowflake__workspaces') }}
),

workspace_projects as (
    select 
        workspace_id,
        count(*) as total_projects
    from {{ ref('stg_snowflake__projects') }}
    where deleted_at is null
    group by 1
),

workspace_tasks as (
    select 
        w.workspace_id,
        count(t.task_id) as total_tasks
    from {{ ref('stg_snowflake__projects') }} p
    join {{ ref('stg_snowflake__tasks') }} t on p.project_id = t.project_id
    join workspace_base w on p.workspace_id = w.workspace_id
    where t.deleted_at is null
    group by 1
),

workspace_active_users as (
    select 
        p.workspace_id,
        count(distinct coalesce(t.created_by, t.assigned_to, c.user_id)) as active_users_count
    from {{ ref('stg_snowflake__projects') }} p
    left join {{ ref('stg_snowflake__tasks') }} t on p.project_id = t.project_id
    left join {{ ref('stg_snowflake__comments') }} c on t.task_id = c.task_id
    where (t.created_at >= dateadd(day, -30, current_timestamp) 
           or t.updated_at >= dateadd(day, -30, current_timestamp)
           or c.created_at >= dateadd(day, -30, current_timestamp))
    group by 1
),

workspace_storage as (
    select 
        p.workspace_id,
        sum(
            coalesce(array_size(parse_json(t.attachments)), 0) +
            coalesce(array_size(parse_json(c.attachments)), 0)
        ) as total_attachments
    from {{ ref('stg_snowflake__projects') }} p
    left join {{ ref('stg_snowflake__tasks') }} t on p.project_id = t.project_id
    left join {{ ref('stg_snowflake__comments') }} c on t.task_id = c.task_id
    group by 1
),

current_subscriptions as (
    select 
        workspace_id,
        status as subscription_status
    from {{ ref('stg_snowflake__subscriptions') }}
    where current_period_end > current_timestamp
    qualify row_number() over (partition by workspace_id order by current_period_end desc) = 1
)

select
    w.*,
    coalesce(wp.total_projects, 0) as project_count,
    coalesce(wt.total_tasks, 0) as task_count,
    coalesce(wa.active_users_count, 0) as active_users_count,
    coalesce(ws.total_attachments * 5, 0) as total_storage_used_mb, -- Assuming 5MB per attachment
    coalesce(cs.subscription_status, 'inactive') as subscription_status
from workspace_base w
left join workspace_projects wp on w.workspace_id = wp.workspace_id
left join workspace_tasks wt on w.workspace_id = wt.workspace_id
left join workspace_active_users wa on w.workspace_id = wa.workspace_id
left join workspace_storage ws on w.workspace_id = ws.workspace_id
left join current_subscriptions cs on w.workspace_id = cs.workspace_id 