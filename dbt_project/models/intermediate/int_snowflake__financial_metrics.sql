{{ config(materialized='table', schema='INTERMEDIATE') }}

with subscription_base as (
    select * from {{ ref('stg_snowflake__subscriptions') }}
),

historical_subscription_base as (
    select * from {{ ref('stg_snowflake__historical_subscriptions') }}
),

workspace_base as (
    select * from {{ ref('stg_snowflake__workspaces') }}
),

all_subscriptions as (
    select * from subscription_base
    union all
    select * from historical_subscription_base
),

subscription_changes as (
    select 
        workspace_id,
        created_at,
        mrr,
        lag(mrr) over (
            partition by workspace_id 
            order by created_at
        ) as previous_mrr
    from all_subscriptions
),

mrr_changes as (
    select 
        workspace_id,
        date_trunc('month', created_at) as month,
        sum(mrr) as new_mrr,
        sum(case 
            when previous_mrr is not null and mrr > previous_mrr
            then mrr - previous_mrr
            else 0 
        end) as expansion_mrr,
        sum(case 
            when canceled_at is not null 
            and date_trunc('month', canceled_at) = date_trunc('month', created_at)
            then mrr 
            else 0 
        end) as churned_mrr
    from subscription_changes
    left join all_subscriptions using (workspace_id, mrr)
    group by 1, 2
),

workspace_financial_metrics as (
    select 
        w.workspace_id,
        w.name as workspace_name,
        sum(s.mrr) as current_mrr,
        sum(s.arr) as current_arr,
        count(distinct case 
            when s.is_canceled = false 
            and s.current_period_end > current_timestamp
            then s.subscription_id 
        end) as active_subscriptions,
        max(s.created_at) as last_subscription_start,
        sum(mc.new_mrr) as total_new_mrr,
        sum(mc.expansion_mrr) as total_expansion_mrr,
        sum(mc.churned_mrr) as total_churned_mrr,
        -- Historical metrics
        count(distinct hs.subscription_id) as total_historical_subscriptions,
        sum(case when hs.is_canceled then 1 else 0 end) as total_churned_subscriptions
    from workspace_base w
    left join subscription_base s on w.workspace_id = s.workspace_id
    left join historical_subscription_base hs on w.workspace_id = hs.workspace_id
    left join mrr_changes mc on w.workspace_id = mc.workspace_id
    group by 1, 2
)

select
    wfm.*,
    case 
        when wfm.current_mrr > 0 then false
        when wfm.total_churned_mrr > 0 then true
        else false
    end as is_churned,
    round(100.0 * wfm.total_expansion_mrr / nullif(wfm.total_new_mrr, 0), 2) as expansion_rate,
    round(100.0 * wfm.total_churned_mrr / nullif(wfm.total_new_mrr, 0), 2) as churn_rate,
    -- Historical churn rate
    round(100.0 * wfm.total_churned_subscriptions / nullif(wfm.total_historical_subscriptions, 0), 2) as historical_churn_rate
from workspace_financial_metrics wfm 