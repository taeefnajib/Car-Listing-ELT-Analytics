{{ config(materialized='table', schema='INTERMEDIATE') }}

with subscription_base as (
    select * from {{ ref('stg_snowflake__subscriptions') }}
),

subscription_invoices as (
    select 
        subscription_id,
        sum(amount_due) as total_amount_billed
    from {{ ref('stg_snowflake__invoices') }}
    group by 1
),

workspace_users as (
    select 
        p.workspace_id,
        count(distinct coalesce(t.created_by, t.assigned_to)) as total_seats_used
    from {{ ref('stg_snowflake__projects') }} p
    left join {{ ref('stg_snowflake__tasks') }} t on p.project_id = t.project_id
    where t.created_at >= dateadd(month, -1, current_timestamp)
    group by 1
),

workspace_activity as (
    select 
        p.workspace_id,
        count(distinct t.task_id) as tasks_last_month,
        count(distinct c.comment_id) as comments_last_month
    from {{ ref('stg_snowflake__projects') }} p
    left join {{ ref('stg_snowflake__tasks') }} t 
        on p.project_id = t.project_id
        and t.created_at >= dateadd(month, -1, current_timestamp)
    left join {{ ref('stg_snowflake__comments') }} c 
        on t.task_id = c.task_id
        and c.created_at >= dateadd(month, -1, current_timestamp)
    group by 1
)

select
    s.*,
    coalesce(si.total_amount_billed, 0) as total_amount_billed,
    datediff('month', s.created_at, current_timestamp) as subscription_age_months,
    coalesce(wu.total_seats_used, 0) as total_seats_used,
    case 
        when s.quantity > 0 
        then round(100.0 * coalesce(wu.total_seats_used, 0) / s.quantity, 2)
        else 0 
    end as seats_utilization,
    case
        when wa.tasks_last_month = 0 and wa.comments_last_month = 0 then 'High'
        when (wa.tasks_last_month + wa.comments_last_month) < 10 then 'Medium'
        else 'Low'
    end as churn_risk
from subscription_base s
left join subscription_invoices si on s.subscription_id = si.subscription_id
left join workspace_users wu on s.workspace_id = wu.workspace_id
left join workspace_activity wa on s.workspace_id = wa.workspace_id 