with stage as (
    select
        id,
        nullif(owner_id, '') as owner_id,
        nullif(year__c, '') as season,
        nullif(name, '') as tournament_name,
        split(location__c,', ')[1] as city,
        split(location__c,', ')[2] as state,
        cast(
            strptime(year__c || ' ' || {{ parse_tournament_date_string("dates__c") }}[1] || ' ' || {{ parse_tournament_date_string("dates__c") }}[2], '%Y %B %d' )::date
            as varchar
        ) as start_date,
        cast(
            strptime(year__c || ' ' || {{ parse_tournament_date_string("dates__c") }}[1] || ' ' || {{ parse_tournament_date_string("dates__c") }}[3], '%Y %B %d' )::date
            as varchar
        ) as end_date,
        cast(is_deleted as boolean) as is_deleted,
        cast(created_date as timestamp) as created,
        cast(_dlt_processed_utc as timestamp) as processed,
        cast(system_modstamp as timestamp) as updated,
        row_number() over(partition by id order by system_modstamp desc) as updated_order
    from
        {{ source('salesforce_raw', 'tournaments') }}
    where
        lower(is_deleted) = 'false'
)

select
    id,
    owner_id,
    season,
    tournament_name,
    city,
    state,
    start_date,
    end_date,
    created,
    processed,
    updated
from
    stage
where
    updated_order = 1