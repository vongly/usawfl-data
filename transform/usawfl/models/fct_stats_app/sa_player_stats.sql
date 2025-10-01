with load_id as (
    select
        _dlt_load_id,
        _dlt_processed_utc,
        row_number() over(order by _dlt_processed_utc desc) as load_id_order
    from
        {{ source('stats_app_raw', 'player_stats') }} a
    group by
        1,2
),

stage as (
    select
        a.id,
        tp.first_name,
        tp.last_name,
        tp.team_name,
        tp.team_city,
        tp.team_state,
        s.stat_name,
        s.stat_value,
        opp.team_name as opponent_name,
        opp.team_city as opponent_city,
        opp.team_state as opponent_state,
        tp.tournament_city,
        tp.tournament_state,
        tp.tournament_date,
        tp.season,
        tp.tournament_number,
        cast(a.created as timestamp) as created,
        cast(a.updated as timestamp) as updated,
        row_number() over(partition by a.id order by a.updated desc) as updated_order
    from
        {{ source('stats_app_raw', 'player_stats') }} a

    join load_id li on a._dlt_load_id = li._dlt_load_id land li.load_id_order = 1

    join
        {{ ref('sa_tournament_players') }} as tp
    on
        a.tournament_player_id = tp.id

    left join
        {{ ref('sa_stats') }} as s
    on
        a.stat_id = s.id

    left join
        {{ ref('sa_teams') }} as opp
    on
        a.opponent_id = opp.id
)

select 
    id,
    first_name,
    last_name,
    team_name,
    team_city,
    team_state,
    stat_name,
    stat_value,
    opponent_name,
    opponent_city,
    opponent_state,
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