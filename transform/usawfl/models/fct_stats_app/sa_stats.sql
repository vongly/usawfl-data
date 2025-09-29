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
        {{ source("{{ env_var('SA_RAW_NAME') }}", 'stats') }} a
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