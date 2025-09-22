with stage as (
    select
        t.id,
        nullif(t.name, '') as team_name,
        cast(t.is_deleted as boolean) as is_deleted,
        if(c1.id is null, false, true) as has_primary_contact,
        nullif(c1.first_name, '') as primary_contact_first_name,
        nullif(c1.last_name, '') as primary_contact_last_name,
        nullif(c1.phone, '') as primary_contact_phone,
        nullif(c1.mobile_phone, '') as primary_contact_mobile_phone,
        nullif(c1.home_phone, '') as primary_contact_home_phone,
        nullif(c1.work_phone, '') as primary_contact_work_phone,
        nullif(c1.email, '') as primary_contact_email,
        nullif(c1.street, '') as primary_contact_street,
        nullif(c1.city, '') as primary_contact_city,
        nullif(c1.state, '') as primary_contact_state,
        nullif(c1.postal_code, '') as primary_contact_postal_code,
        nullif(c1.country, '') as primary_contact_country,
        cast(t.created_date as timestamp) as created,
        cast(t._dlt_processed_utc as timestamp) as processed,
        cast(t.system_modstamp as timestamp) as updated,
        row_number() over(partition by t.id order by t.system_modstamp desc) as updated_order,
    from
        {{ source('salesforce_raw', 'teams') }} t

    left join
        {{ ref('contacts') }} c1
    on
        t.primary_contact__c = c1.id
)

select
    id,
    team_name,
    has_primary_contact,
    primary_contact_first_name,
    primary_contact_last_name,        
    primary_contact_phone,
    primary_contact_mobile_phone,
    primary_contact_home_phone,
    primary_contact_work_phone,
    primary_contact_email,
    primary_contact_street,
    primary_contact_city,
    primary_contact_state,
    primary_contact_postal_code,
    primary_contact_country,
    created,
    processed,
    updated
from
    stage
where
    updated_order = 1