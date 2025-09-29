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
        {{ source("{{ env_var('SA_RAW_NAME') }}", 'tournaments') }} a
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