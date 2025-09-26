
  
    
    

    create  table
      memory."fct_stats_app"."players__dbt_tmp"
  
    as (
      select
    *
from
    's3://move-united/stats_app_to_s3/raw_stats_app/player_classifications_player/*.parquet'
    );
  
  