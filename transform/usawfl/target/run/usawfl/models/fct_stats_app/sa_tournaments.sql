
  
    
    

    create  table
      memory."fct_stats_app"."sa_tournaments__dbt_tmp"
  
    as (
      with stage as (
    select
        id,
        cast(a.city as varchar) as tournament_city,
        cast(a.state as varchar) as tournament_state,
        cast(a.date as date) as tournament_date,
        cast(a.year as int) as season,
        cast(a.number as int) as tournament_number,
        cast(a.created as timestamp) as created,
        cast(a.updated as timestamp) as updated,
        row_number() over(partition by a.id order by a.updated desc) as updated_order
    from
        's3://move-united/stats_app_to_s3/raw_stats_app/player_classifications_tournament/*.parquet' a
)

select
    id,
    tournament_city,
    tournament_state,
    tournament_date,
    season,
    tournament_number,
    created,
    updated
from
    stage
where
    updated_order = 1
    );
  
  