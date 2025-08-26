{{ config(materialized='table', schema='INTERMEDIATE') }}

with user_base as (
    select * from {{ ref('stg_snowflake__users') }}
),

user_activity_last_30d as (
    select 
        created_by as user_id,
        count(*) as tasks_created_30d,
        count(distinct date(created_at)) as active_days_30d
    from {{ ref('stg_snowflake__tasks') }}
    where created_at >= dateadd('day', -30, current_timestamp)
    and created_by is not null
    group by 1
),

user_comments_last_30d as (
    select 
        user_id,
        count(*) as comments_30d,
        count(distinct date(created_at)) as comment_days_30d
    from {{ ref('stg_snowflake__comments') }}
    where created_at >= dateadd('day', -30, current_timestamp)
    group by 1
),

feature_adoption as (
    select
        u.user_id,
        -- Task Creation Adoption
        case 
            when u.tasks_created_count > 0 then true 
            else false 
        end as has_created_task,
        -- Comment Feature Adoption
        case 
            when u.comments_count > 0 then true 
            else false 
        end as has_used_comments,
        -- Active User Status
        case
            when ua.active_days_30d >= 20 then 'power_user'
            when ua.active_days_30d >= 10 then 'active_user'
            when ua.active_days_30d >= 1 then 'casual_user'
            else 'inactive'
        end as user_status,
        -- Engagement Score (0-100)
        greatest(least(
            (coalesce(ua.active_days_30d, 0) * 2) + -- Up to 60 points for daily activity
            (coalesce(ua.tasks_created_30d, 0) * 0.5) + -- Up to 20 points for task creation
            (coalesce(uc.comments_30d, 0) * 0.5) -- Up to 20 points for comments
        , 100), 0) as engagement_score
    from user_base u
    left join user_activity_last_30d ua on u.user_id = ua.user_id
    left join user_comments_last_30d uc on u.user_id = uc.user_id
)

select
    u.*,
    coalesce(ua.tasks_created_30d, 0) as tasks_created_30d,
    coalesce(ua.active_days_30d, 0) as active_days_30d,
    coalesce(uc.comments_30d, 0) as comments_30d,
    coalesce(uc.comment_days_30d, 0) as comment_days_30d,
    fa.has_created_task,
    fa.has_used_comments,
    fa.user_status,
    fa.engagement_score
from user_base u
left join user_activity_last_30d ua on u.user_id = ua.user_id
left join user_comments_last_30d uc on u.user_id = uc.user_id
left join feature_adoption fa on u.user_id = fa.user_id 