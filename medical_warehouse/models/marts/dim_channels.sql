{{ config(
    materialized='table'
) }}

with channels as (
    select
        channel_id,
        channel_username,
        channel_title,
        -- assign surrogate key
        dense_rank() over(order by channel_id) as channel_key
    from {{ ref('stg_telegram_messages') }}
    group by channel_id, channel_username, channel_title
)

select
    channel_key,
    channel_id,
    channel_username,
    channel_title,
    count(*) as total_posts,
    avg(views) as avg_views,
    min(message_date) as first_post_date,
    max(message_date) as last_post_date
from {{ ref('stg_telegram_messages') }}
join channels using(channel_id, channel_username, channel_title)
group by channel_key, channel_id, channel_username, channel_title
