Bucket Name -abhayytdatabuket

Script Bucket - yt-data-pipeline-script-ap-south-1-dev

Glue Bronze - yt_pipeline_bronze_dev
Glue Silver - yt_pipeline_silver_dev
Glue Gold -  yt_pipeline_gold_dev


--bronze_database yt_pipeline_bronze_dev
--bronze_table raw_statistics
--silver_bucket yt-data-pipeline-silver-ap-south-1-dev
--silver_database yt_pipeline_silver_dev
--silver_table clean_statistics

--silver_database yt_pipeline_silver_dev
--gold_bucket yt-data-pipeline-gold-ap-south-1-dev
--gold_database yt_pipeline_gold_dev
