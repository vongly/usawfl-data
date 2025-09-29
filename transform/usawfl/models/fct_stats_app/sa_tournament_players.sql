with stage as (
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
        {{ source("{{ env_var('SA_RAW_NAME') }}", 'tournament_players') }} a

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