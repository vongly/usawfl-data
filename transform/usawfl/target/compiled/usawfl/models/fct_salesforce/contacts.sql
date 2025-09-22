with stage as (
    select
        c.id,
        nullif(
    case
        when lower(left(c.last_name, 2)) = 'mc'
        then concat(
                upper(substring(c.last_name, 1, 1)),
                lower(substring(c.last_name, 2, 1)),
                upper(substring(c.last_name, 3, 1)),
                lower(substring(c.last_name, 4, length(c.last_name)))
            )
        else concat(
                upper(substring(c.last_name, 1, 1)),
                lower(substring(c.last_name, 2, length(c.last_name)))
            )
        end
, '') as last_name,
        nullif(
    case
        when lower(left(c.first_name, 2)) = 'mc'
        then concat(
                upper(substring(c.first_name, 1, 1)),
                lower(substring(c.first_name, 2, 1)),
                upper(substring(c.first_name, 3, 1)),
                lower(substring(c.first_name, 4, length(c.first_name)))
            )
        else concat(
                upper(substring(c.first_name, 1, 1)),
                lower(substring(c.first_name, 2, length(c.first_name)))
            )
        end
, '') as first_name,
        nullif(c.birthdate, '') as birthdate,
        nullif(
    right(regexp_replace(cast(c.phone as varchar), '[^0-9]', '', 'g'), 10)
, '') as phone,
        nullif(
    right(regexp_replace(cast(c.mobile_phone as varchar), '[^0-9]', '', 'g'), 10)
, '') as mobile_phone,
        nullif(
    right(regexp_replace(cast(c.home_phone as varchar), '[^0-9]', '', 'g'), 10)
, '') as home_phone,
        nullif(
    right(regexp_replace(cast(c.work_phone__c as varchar), '[^0-9]', '', 'g'), 10)
, '') as work_phone,
        nullif(c.email, '') as email,
        nullif(c.mailing_street, '') as street,
        nullif(c.mailing_city, '') as city,
        coalesce(sa.abbreviation, ss.abbreviation, nullif(c.mailing_state, '')) as state,
        nullif(
    right(regexp_replace(cast(c.mailing_postal_code as varchar), '[^0-9]', '', 'g'), 10)
, '') as postal_code,
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
        read_parquet('s3://move-united/usawfl_salesforce_to_s3_file/raw_salesforce/contact/*.parquet') c

    left join
        memory."fct_references"."states" sa
    on
        lower(
    regexp_replace(c.mailing_state, '[\.`]', '', 'g')
) = lower(sa.abbreviation)

    left join
        memory."fct_references"."states" ss
    on
        lower(
    regexp_replace(c.mailing_state, '[\.`]', '', 'g')
) = lower(ss.state)

    where
        lower(c.is_deleted) = 'false'
)

select
    id,
    last_name,
    first_name,
    birthdate,
    
    if(
        cast(phone as varchar) is null,
        null,
        '(' || substring(cast(phone as varchar) from 1 for 3) || ') ' || substring(cast(phone as varchar) from 3 for 3) || '-' || substring(cast(phone as varchar) from 7 for 4)
    )
 as phone,
    
    if(
        cast(mobile_phone as varchar) is null,
        null,
        '(' || substring(cast(mobile_phone as varchar) from 1 for 3) || ') ' || substring(cast(mobile_phone as varchar) from 3 for 3) || '-' || substring(cast(mobile_phone as varchar) from 7 for 4)
    )
 as mobile_phone,
    
    if(
        cast(home_phone as varchar) is null,
        null,
        '(' || substring(cast(home_phone as varchar) from 1 for 3) || ') ' || substring(cast(home_phone as varchar) from 3 for 3) || '-' || substring(cast(home_phone as varchar) from 7 for 4)
    )
 as home_phone,
    
    if(
        cast(work_phone as varchar) is null,
        null,
        '(' || substring(cast(work_phone as varchar) from 1 for 3) || ') ' || substring(cast(work_phone as varchar) from 3 for 3) || '-' || substring(cast(work_phone as varchar) from 7 for 4)
    )
 as work_phone,
    email,
    street,
    city,
    
    case
        when right(state, 4) = ', UK'
        then replace(state, ', UK', '')
        when right(state, 3) = '/UK'
        then replace(state, '/UK', '')
        when right(state, 8) = ', Canada'
        then replace(state, ', Canada', '')
        when state in ('SÃ£o Paulo', 'São Paulo')
        then 'Sao Paulo'
        else state
        end
 as state,
    
    case
        when length(cast(postal_code as varchar)) = 5
        then cast(postal_code as varchar)
        when length(cast(postal_code as varchar)) < 5
        then right('0000' || cast(postal_code as varchar), 5)
        when length(cast(postal_code as varchar)) > 5
        then left(right('0000' || cast(postal_code as varchar), 9), 5)
        end
 as postal_code,
    
    case
        when replace(lower(country), '.', '') in ('united states of america', 'united states', 'usa', 'us', 'usaa')
        then 'United States'
        when lower(country) = 'uk'
        then 'United Kingdom'
        when right(state, 4) = ', UK'
        then 'United Kingdom'
        when right(state, 3) = '/UK'
        then 'United Kingdom'
        when right(state, 8) = ', Canada'
        then 'Canada'
        when lower(country) = 'can'
        then 'Canada'
        when lower(country) = 'mex'
        then 'Mexico'
        when lower(country) = 'per'
        then 'Peru'
        when lower(country) in ('france', 'fra')
        then 'France'
        when state in ('SÃ£o Paulo', 'São Paulo')
        then 'Brazil'
        else country
        end
 as country,
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