with stage as (
    select
        a.id,
        nullif(a.name, '') as team_name,
        nullif(a.city, '') as team_city,
        nullif(a.state, '') as team_state,
        cast(a.created as timestamp) as created,
        cast(a.updated as timestamp) as updated,
        row_number() over(partition by a.id order by a.updated desc) as updated_order
    from
        's3://move-united/stats_app_to_s3/raw_stats_app/player_classifications_team/*.parquet' a
)

select
    id,
    team_name,
    team_city,
    team_state,
    created,
    updated
from
    stage
where
    updated_order = 1