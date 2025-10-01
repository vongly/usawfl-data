with load_id as (
    select
        _dlt_load_id,
        _dlt_processed_utc,
        row_number() over(order by _dlt_processed_utc desc) as load_id_order
    from
        {{ source('stats_app_raw', 'tournament_players') }} a
    group by
        1,2
),

stage as (
    select
        a.id,
        p.first_name,
        p.last_name,
        cast(a.player_number as int) as player_number,
        cast(a.classification_value as int) as classification,
        p.team_name,
        p.team_city,
        p.team_state,
        t.tournament_city,
        t.tournament_state,
        t.tournament_date,
        t.season,
        t.tournament_number,
        cast(a.created as timestamp) as created,
        cast(a.updated as timestamp) as updated,
        row_number() over(partition by a.id order by a.updated desc) as updated_order
    from
        {{ source('stats_app_raw', 'tournament_players') }} a

    join load_id li on a._dlt_load_id = li._dlt_load_id land li.load_id_order = 1

    left join
        {{ ref('sa_players') }} as p
    on
        a.player_id = p.id

    left join
        {{ ref('sa_tournaments') }} as t
    on
        a.tournament_id = t.id
)

select
    id,
    player_number,
    classification,
    first_name,
    last_name,
    team_name,
    team_city,
    team_state,
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