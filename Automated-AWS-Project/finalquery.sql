SELECT 
    category_name,
    COUNT(DISTINCT video_id) AS total_videos,
    SUM(view_count) AS total_views,
    -- Calculates how many likes a category gets per 100 views
    ROUND((CAST(SUM(like_count) AS DOUBLE) / SUM(view_count)) * 100, 2) AS likes_per_100_views,
    -- Calculates how many comments a category gets per 100 views
    ROUND((CAST(SUM(comment_count) AS DOUBLE) / SUM(view_count)) * 100, 2) AS comments_per_100_views
FROM 
    yt_pipeline_gold.vw_global_youtube_master
WHERE 
    view_count > 0
GROUP BY 
    category_name
ORDER BY 
    likes_per_100_views DESC;