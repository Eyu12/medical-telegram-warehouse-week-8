SELECT *
FROM {{ ref('fct_messages') }}
WHERE date_key > CURRENT_DATE;
