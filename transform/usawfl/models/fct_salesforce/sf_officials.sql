with official_links_stage as (
    select
        ol.official__c as official_id,
        cast(ol.is_deleted as boolean) as is_deleted,
        nullif(t.tournament_name, '') as tournament_name,
        nullif(ol.games_officiated__c, '') as games_officiated,
        nullif(ol.days_assigned__c, '') as days_assigned,
        row_number() over(partition by ol.id order by ol.system_modstamp desc) as updated_order
    from 
        {{ source("{{ env_var('SF_RAW_NAME') }}", 'official_links') }} ol
    left join
        {{ ref('sf_tournaments') }} t
    on
        ol.usawfl_tournament__c = t.id
    where
        ol.is_deleted = false

),

official_links as (
    select
        official_id,
        json_group_array(
            json_object(
                'tournament_name', tournament_name,
                'games_officiated', games_officiated,
                'days_assigned', days_assigned
            )
        ) as tournament_officiated_details
    from
        official_links_stage
    where
        updated_order = 1
    group by
        1
),

officials_stage as (
    select
        o.id,
        nullif(c.first_name, '') as first_name,
        nullif(c.last_name, '') as last_name,
        nullif(o.type_of_official__c, '') as type,
        o.status__c as status,
        string_split(
            replace(replace(o.field_position_preferences__c, ';', '|'), '/', '|'),
            '|'
        ) as preference,
        nullif(o.type_of_official_details__c, '') as details,
        nullif(o.safe_sport_course_completed__c, '') as safe_sport_course_completed,
        nullif(o.safe_sport_certificate_expires__c, '') as safe_sport_certificate_expires,
        nullif(o.background_check_determination__c, '') as background_check_determination,
        nullif(o.background_check_expires__c, '') as background_check_expires,
        nullif(o.usawfl_rules_exam_pass_date__c, '') as usawfl_rules_exam_pass_date,
        cast(o.is_deleted as boolean) as is_deleted,
        if(c.id is null, false, true) as has_contact,
        nullif(c.phone, '') as phone,
        nullif(c.mobile_phone, '') as mobile_phone,
        nullif(c.home_phone, '') as home_phone,
        nullif(c.work_phone, '') as work_phone,
        nullif(c.email, '') as email,
        nullif(c.street, '') as street,
        nullif(c.city, '') as city,
        nullif(c.state, '') as state,
        nullif(c.postal_code, '') as postal_code,
        nullif(c.country, '') as country,
--        nullif(ol.tournament_officiated_details, '') as tournament_officiated_details,
        cast(o.created_date as timestamp) as created,
        cast(o._dlt_processed_utc as timestamp) as processed,
        cast(o.system_modstamp as timestamp) as updated,
        row_number() over(partition by o.id order by o.system_modstamp desc) as updated_order,
        row_number() over(partition by o.contact__c order by o.system_modstamp desc) as contact_updated_order,
    from
        {{ source("{{ env_var('SF_RAW_SCHEMA') }}", 'officials') }} o

    left join
        {{ ref('sf_contacts') }} c
    on
        o.contact__c = c.id

    left join
        official_links ol
    on
        o.id = ol.official_id

    where
        lower(o.is_deleted) = 'false'
        and o.name != 'Test Official'
)


select
    id,
    first_name,
    last_name,        
    type,
    status,
    preference,
    details,
    has_contact,
    phone,
    mobile_phone,
    home_phone,
    work_phone,
    email,
    street,
    city,
    state,
    postal_code,
    country,
    safe_sport_course_completed,
    safe_sport_certificate_expires,
    background_check_determination,
    background_check_expires,
    usawfl_rules_exam_pass_date,
--    tournament_officiated_details
    created,
    processed,
    updated
from
    officials_stage
where
    updated_order = 1
    and contact_updated_order = 1