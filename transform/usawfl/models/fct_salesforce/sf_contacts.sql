with stage as (
    select
        c.id,
        nullif({{ title_name("c.last_name") }}, '') as last_name,
        nullif({{ title_name("c.first_name") }}, '') as first_name,
        nullif(c.birthdate, '') as birthdate,
        nullif({{ return_numbers_from_string("c.phone") }}, '') as phone,
        nullif({{ return_numbers_from_string("c.mobile_phone") }}, '') as mobile_phone,
        nullif({{ return_numbers_from_string("c.home_phone") }}, '') as home_phone,
        nullif({{ return_numbers_from_string("c.work_phone__c") }}, '') as work_phone,
        nullif(c.email, '') as email,
        nullif(c.mailing_street, '') as street,
        nullif(c.mailing_city, '') as city,
        coalesce(sa.abbreviation, ss.abbreviation, nullif(c.mailing_state, '')) as state,
        nullif({{ return_numbers_from_string("c.mailing_postal_code") }}, '') as postal_code,
        nullif(c.mailing_country, '') as country,
        nullif(c.family_contact_name__c, '') as family_contact,
        nullif(c.family_contact_phone__c, '') as family_contact_phone,
        case
            when c.warfighter__c = true
                or c.warfighter_pre_2001_veteran__c = true 
                or c.warfighter_post_2001_veteran__c = true 
            then true
            else false
            end as warfighter,
        nullif(c.shirt_size__c, '') as shirt_size,
        cast(c.is_deleted as boolean) as is_deleted,
        cast(c.created_date as timestamp) as created,
        cast(c._dlt_processed_utc as timestamp) as processed,
        cast(c.system_modstamp as timestamp) as updated,
        row_number() over(partition by c.id order by c.system_modstamp desc) as updated_order
    from
        read_parquet({{ source("{{ env_var('SF_RAW_NAME') }}", 'contacts') }}) c

    left join
        {{ ref('states') }} sa
    on
        lower({{ clean_punct("c.mailing_state") }}) = lower(sa.abbreviation)

    left join
        {{ ref('states') }} ss
    on
        lower({{ clean_punct("c.mailing_state") }}) = lower(ss.state)

    where
        lower(c.is_deleted) = 'false'
)

select
    id,
    last_name,
    first_name,
    birthdate,
    {{ format_phone_number("phone") }} as phone,
    {{ format_phone_number("mobile_phone") }} as mobile_phone,
    {{ format_phone_number("home_phone") }} as home_phone,
    {{ format_phone_number("work_phone") }} as work_phone,
    email,
    street,
    city,
    {{ clean_contact_states("state") }} as state,
    {{ clean_to_5_digit_zip("postal_code") }} as postal_code,
    {{ clean_contact_countries("country", "state") }} as country,
    family_contact,
    family_contact_phone,
    warfighter,
    shirt_size,
    created,
    processed,
    updated
from
    stage
where
    updated_order = 1