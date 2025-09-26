
  
    
    

    create  table
      memory."fct_stats_app"."sa_stats__dbt_tmp"
  
    as (
      with stage as (
    select
        id,
        nullif(a.name, '') as stat_name,
        cast(a.value as int) as stat_value,
        cast(is_active as boolean) as is_active,
        cast(a.created as timestamp) as created,
        cast(a.updated as timestamp) as updated,
        row_number() over(partition by a.id order by a.updated desc) as updated_order
    from
        's3://move-united/stats_app_to_s3/raw_stats_app/player_classifications_stat/*.parquet' a
)

select
    id,
    stat_name,
    stat_value,
    is_active,
    created,
    updated
from
    stage
where
    updated_order = 1
    );
  
  