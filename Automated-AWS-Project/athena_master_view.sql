-- Master Analytics Layer View
CREATE OR REPLACE VIEW yt_pipeline_gold.vw_global_youtube_master AS
SELECT 
    v.video_id,
    v.title,
    v.channel_title,
    v.published_at,
    v.view_count,
    v.like_count,
    v.comment_count,
    v.category_id,
    COALESCE(c.category_name, 'Unknown / Other') AS category_name
FROM 
    yt_pipeline_gold.global_youtube_analytics v
LEFT JOIN 
    yt_pipeline_gold.youtube_category_reference c
    ON v.category_id = CAST(c.category_id AS VARCHAR);
    