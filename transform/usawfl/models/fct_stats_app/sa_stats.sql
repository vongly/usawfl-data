with load_id as (
    select
        _dlt_load_id,
        _dlt_processed_utc,
        row_number() over(order by _dlt_processed_utc desc) as load_id_order
    from
        {{ source('stats_app_raw', 'stats') }} a
    group by
        1,2
),

stage as (
    select
        id,
        nullif(a.name, '') as stat_name,
        cast(a.value as int) as stat_value,
        cast(is_active as boolean) as is_active,
        cast(a.created as timestamp) as created,
        cast(a.updated as timestamp) as updated,
        row_number() over(partition by a.id order by a.updated desc) as updated_order
    from
        {{ source('stats_app_raw', 'stats') }} a

    join load_id li on a._dlt_load_id = li._dlt_load_id and li.load_id_order = 1
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