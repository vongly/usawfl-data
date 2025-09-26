with stage as (
    select
        a.id,
        nullif(a.first_name, '') as first_name,
        nullif(a.last_name, '') as last_name,
        a.team_id,
        t.team_name,
        t.team_city,
        t.team_state,
        cast(a.created as timestamp) as created,
        cast(a.updated as timestamp) as updated,
        row_number() over(partition by a.id order by a.updated desc) as updated_order
    from
        's3://move-united/stats_app_to_s3/raw_stats_app/player_classifications_player/*.parquet' a

    left join
        memory."fct_stats_app"."sa_teams" t
    on
        a.team_id = t.id
)

select
    id,
    first_name,
    last_name,
    team_id,
    team_name,
    team_city,
    team_state,
    created,
    updated
from
    stage
where
    updated_order = 1