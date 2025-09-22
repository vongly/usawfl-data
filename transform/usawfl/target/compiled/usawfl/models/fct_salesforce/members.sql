with stage as (
    select
        m.id,
        nullif(m.name, '') as name,
        nullif(t.team_name, '') as team_name,
        nullif(m.contact_type__c, '') as contact_type,
        nullif(m.contact_subtype__c, '') as contact_subtype,
        nullif(m.type__c, '') as type,
        nullif(m.status__c, '') as status,
        nullif(m.classification_proposed__c, '') as classification_proposed,
        nullif(m.classification_proposed_year__c, '') as classification_proposed_year,
        nullif(m.classification_confirmed__c, '') as classification,
        nullif(m.classification_status_final__c, '') as classification_status,
        nullif(m.classification_confirmed_tournament__c, '') as classification_updated_at,
        nullif(m.classification_medical_team__c, '') as classifiers,
        nullif(m.safe_sport_certified__c, '') as safe_sport_certified,
        nullif(m.rules_exam_passed__c, '') as rules_exam_passed,
        cast(m.is_deleted as boolean) as is_deleted,
        if(c.id is null, false, true) as has_contact,
        nullif(c.first_name, '') as first_name,
        nullif(c.last_name, '') as last_name,
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
        cast(m.created_date as timestamp) as created,
        cast(m._dlt_processed_utc as timestamp) as processed,
        cast(m.system_modstamp as timestamp) as updated,
        row_number() over(partition by m.id order by m.system_modstamp desc) as updated_order,
        row_number() over(partition by m.contact__c order by m.system_modstamp desc) as contact_updated_order
    from
        's3://move-united/usawfl_salesforce_to_s3_file/raw_salesforce/usawfl__c/*.parquet' m

    left join
        memory."fct_salesforce"."contacts" c
    on
        m.contact__c = c.id

    left join
        memory."fct_salesforce"."teams" t
    on
        m.team__c = t.id

    where
        lower(m.is_deleted) = 'false'
)

select
    id,
    name,
    team_name,
    contact_type,
    contact_subtype,
    type,
    status,
    classification_proposed,
    classification_proposed_year,
    classification,
    classification_status,
    classification_updated_at,
    classifiers,
    safe_sport_certified,
    rules_exam_passed,
    has_contact,
    first_name,
    last_name,        
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
    created,
    processed,
    updated
from
    stage
where
    updated_order = 1
    and contact_updated_order = 1