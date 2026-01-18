-- fct_image_detections.sql
-- Integrates YOLO detection results with messages and classifies images

WITH yolo AS (
    SELECT
        id AS detection_id,
        message_id,
        detected_object,
        confidence,
        detected_at
    FROM raw.image_detections
),

messages AS (
    SELECT
        message_id,
        channel_key,
        date_key
    FROM fct_messages
)

SELECT
    m.message_id,
    m.channel_key,
    m.date_key,
    y.detected_object AS detected_class,
    y.confidence AS confidence_score,
    
    -- Image classification logic
    CASE
        WHEN y.detected_object ILIKE '%person%' AND y.detected_object ILIKE '%bottle%' THEN 'promotional'
        WHEN y.detected_object ILIKE '%bottle%' AND y.detected_object NOT ILIKE '%person%' THEN 'product_display'
        WHEN y.detected_object ILIKE '%person%' AND y.detected_object NOT ILIKE '%bottle%' THEN 'lifestyle'
        ELSE 'other'
    END AS image_category

FROM yolo y
LEFT JOIN messages m
    ON y.message_id = m.message_id
;
