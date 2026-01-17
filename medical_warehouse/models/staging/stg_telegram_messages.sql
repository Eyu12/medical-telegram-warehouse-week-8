{{ config(
    materialized='table'
) }}

with source as (

    select *
    from raw.telegram_messages

)

select
    message_id,
    channel_id,
    channel_username,
    channel_title,
    message_date,
    message_text,
    views,
    forwards,
    replies,
    media_type,
    media_file,
    message_url,
    scraped_at,
    length(message_text) as message_length,
    case when media_file is not null then true else false end as has_image
from source
where message_text is not null
