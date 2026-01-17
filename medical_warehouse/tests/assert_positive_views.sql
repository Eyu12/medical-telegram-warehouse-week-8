SELECT *
FROM {{ ref('fct_messages') }}
WHERE view_count < 0;
