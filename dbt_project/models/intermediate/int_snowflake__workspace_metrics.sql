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
        deleted_at,
        total_projects,
        completed_projects
    from {{ ref('stg_snowflake__workspaces') }}
),

current_subscription as (
    select 
        workspace_id,
        mrr,
        arr,
        is_canceled,
        status as subscription_status
    from {{ ref('stg_snowflake__subscriptions') }}
    where current_period_end > current_timestamp
    qualify row_number() over (partition by workspace_id order by current_period_end desc) = 1
),

workspace_growth as (
    select
        p.workspace_id,
        -- Total Active Users
        count(distinct coalesce(t.created_by, t.assigned_to)) as active_user_count,
        -- New Users
        count(distinct case 
            when t.created_at >= dateadd('month', -1, current_timestamp)
            then coalesce(t.created_by, t.assigned_to)
        end) as new_users_last_month,
        -- Daily Active Users
        count(distinct case 
            when t.created_at >= current_date
            or t.updated_at >= current_date
            or c.created_at >= current_date
            then coalesce(t.created_by, t.assigned_to, c.user_id)
        end) as daily_active_users,
        -- Weekly Active Users
        count(distinct case 
            when t.created_at >= dateadd('day', -7, current_date)
            or t.updated_at >= dateadd('day', -7, current_date)
            or c.created_at >= dateadd('day', -7, current_date)
            then coalesce(t.created_by, t.assigned_to, c.user_id)
        end) as weekly_active_users,
        -- Monthly Active Users
        count(distinct case 
            when t.created_at >= dateadd('day', -30, current_date)
            or t.updated_at >= dateadd('day', -30, current_date)
            or c.created_at >= dateadd('day', -30, current_date)
            then coalesce(t.created_by, t.assigned_to, c.user_id)
        end) as monthly_active_users
    from {{ ref('stg_snowflake__projects') }} p
    left join {{ ref('stg_snowflake__tasks') }} t 
        on p.project_id = t.project_id
        and t.deleted_at is null
    left join {{ ref('stg_snowflake__comments') }} c
        on t.task_id = c.task_id
        and c.deleted_at is null
    where p.deleted_at is null
    group by 1
),

project_metrics as (
    select
        w.workspace_id,
        avg(p.completion_rate) as avg_project_completion_rate,
        count(case 
            when t.completed_at >= dateadd('day', -7, current_timestamp)
            then t.task_id 
        end) as tasks_completed_last_week
    from {{ ref('stg_snowflake__workspaces') }} w
    left join {{ ref('stg_snowflake__projects') }} p 
        on p.workspace_id = w.workspace_id
        and p.deleted_at is null
    left join {{ ref('stg_snowflake__tasks') }} t
        on p.project_id = t.project_id
        and t.deleted_at is null
    group by 1
)

select
    -- Workspace Base Metrics
    w.workspace_id,
    w.name,
    w.description,
    w.created_by,
    w.created_at,
    w.plan_tier,
    w.settings,
    w.max_members,
    w.status,
    w.updated_at,
    w.deleted_at,
    
    -- User Activity Metrics
    coalesce(wg.daily_active_users, 0) as daily_active_users,
    coalesce(wg.weekly_active_users, 0) as weekly_active_users,
    coalesce(wg.monthly_active_users, 0) as monthly_active_users,
    coalesce(wg.active_user_count, 0) as active_user_count,
    coalesce(wg.new_users_last_month, 0) as new_users_last_month,
    case 
        when coalesce(wg.monthly_active_users, 0) > 0 and coalesce(wg.active_user_count, 0) > 0
        then round(100.0 * wg.monthly_active_users / wg.active_user_count, 2)
        else 0 
    end as mau_percentage,
    
    -- Project and Task Metrics
    coalesce(w.total_projects, 0) as total_projects,
    coalesce(w.completed_projects, 0) as completed_projects,
    coalesce(pm.avg_project_completion_rate, 0) as avg_project_completion_rate,
    coalesce(pm.tasks_completed_last_week, 0) as tasks_completed_last_week,
    
    -- Financial Metrics
    coalesce(cs.mrr, 0) as mrr,
    coalesce(cs.arr, 0) as arr,
    cs.is_canceled,
    cs.subscription_status
    
from workspace_base w
left join workspace_growth wg on w.workspace_id = wg.workspace_id
left join project_metrics pm on w.workspace_id = pm.workspace_id
left join current_subscription cs on w.workspace_id = cs.workspace_id 