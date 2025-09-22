with members as (
    select
        m.*,
        nullif(c.last_name, '') as last_name,
        nullif(c.first_name, '') as first_name,
        nullif(t.team_name, '') as team_name,
        row_number() over(partition by m.id order by m.system_modstamp desc) as updated_order
    from
        's3://move-united/usawfl_salesforce_to_s3_file/raw_salesforce/usawfl__c/*.parquet' m

    left join
        memory."fct_salesforce"."teams" t
    on
        m.team__c = t.id

    left join
        memory."fct_salesforce"."contacts" c
    on
        m.contact__c = c.id

    where
        lower(m.is_deleted) = 'false'
),

stage as (
    select * from members where updated_order = 1
),

stats as (
    select
        id,
        last_name,
        first_name,
        name,
        team_name,
        replace(
            replace(
                'x2021_t_ds__c',
                substring('x2021_t_ds__c' from 1 for strpos('x2021_t_ds__c','_')),
                ''
            ),
            '__c',
            ''
        ) as stat,
        cast(x2021_t_ds__c as int) as amount,
        cast(substring('x2021_t_ds__c' from 2 for 4) as int) as year
    from
        stage
    where
        lower(is_deleted) = 'false'
        and x2021_t_ds__c != ''

    
        
    
        

    union all

    select
        id,
        last_name,
        first_name,
        name,
        team_name,
        replace(
            replace(
                'x2021_in_ts__c',
                substring('x2021_in_ts__c' from 1 for strpos('x2021_in_ts__c','_')),
                ''
            ),
            '__c',
            ''
        ) as stat,
        cast(x2021_in_ts__c as int),
        cast(substring('x2021_in_ts__c' from 2 for 4) as int) as year
    from
        stage
    where
        lower(is_deleted) = 'false'
        and x2021_in_ts__c != ''

        
    
        

    union all

    select
        id,
        last_name,
        first_name,
        name,
        team_name,
        replace(
            replace(
                'x2021_sacks__c',
                substring('x2021_sacks__c' from 1 for strpos('x2021_sacks__c','_')),
                ''
            ),
            '__c',
            ''
        ) as stat,
        cast(x2021_sacks__c as int),
        cast(substring('x2021_sacks__c' from 2 for 4) as int) as year
    from
        stage
    where
        lower(is_deleted) = 'false'
        and x2021_sacks__c != ''

        
    
        

    union all

    select
        id,
        last_name,
        first_name,
        name,
        team_name,
        replace(
            replace(
                'x2022_safeties__c',
                substring('x2022_safeties__c' from 1 for strpos('x2022_safeties__c','_')),
                ''
            ),
            '__c',
            ''
        ) as stat,
        cast(x2022_safeties__c as int),
        cast(substring('x2022_safeties__c' from 2 for 4) as int) as year
    from
        stage
    where
        lower(is_deleted) = 'false'
        and x2022_safeties__c != ''

        
    
        

    union all

    select
        id,
        last_name,
        first_name,
        name,
        team_name,
        replace(
            replace(
                'x2022_in_ts__c',
                substring('x2022_in_ts__c' from 1 for strpos('x2022_in_ts__c','_')),
                ''
            ),
            '__c',
            ''
        ) as stat,
        cast(x2022_in_ts__c as int),
        cast(substring('x2022_in_ts__c' from 2 for 4) as int) as year
    from
        stage
    where
        lower(is_deleted) = 'false'
        and x2022_in_ts__c != ''

        
    
        

    union all

    select
        id,
        last_name,
        first_name,
        name,
        team_name,
        replace(
            replace(
                'x2022_sacks__c',
                substring('x2022_sacks__c' from 1 for strpos('x2022_sacks__c','_')),
                ''
            ),
            '__c',
            ''
        ) as stat,
        cast(x2022_sacks__c as int),
        cast(substring('x2022_sacks__c' from 2 for 4) as int) as year
    from
        stage
    where
        lower(is_deleted) = 'false'
        and x2022_sacks__c != ''

        
    
        

    union all

    select
        id,
        last_name,
        first_name,
        name,
        team_name,
        replace(
            replace(
                'x2022_t_ds__c',
                substring('x2022_t_ds__c' from 1 for strpos('x2022_t_ds__c','_')),
                ''
            ),
            '__c',
            ''
        ) as stat,
        cast(x2022_t_ds__c as int),
        cast(substring('x2022_t_ds__c' from 2 for 4) as int) as year
    from
        stage
    where
        lower(is_deleted) = 'false'
        and x2022_t_ds__c != ''

        
    
        

    union all

    select
        id,
        last_name,
        first_name,
        name,
        team_name,
        replace(
            replace(
                'x2023_safeties__c',
                substring('x2023_safeties__c' from 1 for strpos('x2023_safeties__c','_')),
                ''
            ),
            '__c',
            ''
        ) as stat,
        cast(x2023_safeties__c as int),
        cast(substring('x2023_safeties__c' from 2 for 4) as int) as year
    from
        stage
    where
        lower(is_deleted) = 'false'
        and x2023_safeties__c != ''

        
    
        

    union all

    select
        id,
        last_name,
        first_name,
        name,
        team_name,
        replace(
            replace(
                'x2023_in_ts__c',
                substring('x2023_in_ts__c' from 1 for strpos('x2023_in_ts__c','_')),
                ''
            ),
            '__c',
            ''
        ) as stat,
        cast(x2023_in_ts__c as int),
        cast(substring('x2023_in_ts__c' from 2 for 4) as int) as year
    from
        stage
    where
        lower(is_deleted) = 'false'
        and x2023_in_ts__c != ''

        
    
        

    union all

    select
        id,
        last_name,
        first_name,
        name,
        team_name,
        replace(
            replace(
                'x2023_sacks__c',
                substring('x2023_sacks__c' from 1 for strpos('x2023_sacks__c','_')),
                ''
            ),
            '__c',
            ''
        ) as stat,
        cast(x2023_sacks__c as int),
        cast(substring('x2023_sacks__c' from 2 for 4) as int) as year
    from
        stage
    where
        lower(is_deleted) = 'false'
        and x2023_sacks__c != ''

        
    
        

    union all

    select
        id,
        last_name,
        first_name,
        name,
        team_name,
        replace(
            replace(
                'x2023_t_ds__c',
                substring('x2023_t_ds__c' from 1 for strpos('x2023_t_ds__c','_')),
                ''
            ),
            '__c',
            ''
        ) as stat,
        cast(x2023_t_ds__c as int),
        cast(substring('x2023_t_ds__c' from 2 for 4) as int) as year
    from
        stage
    where
        lower(is_deleted) = 'false'
        and x2023_t_ds__c != ''

        
    
        

    union all

    select
        id,
        last_name,
        first_name,
        name,
        team_name,
        replace(
            replace(
                'x2024_safeties__c',
                substring('x2024_safeties__c' from 1 for strpos('x2024_safeties__c','_')),
                ''
            ),
            '__c',
            ''
        ) as stat,
        cast(x2024_safeties__c as int),
        cast(substring('x2024_safeties__c' from 2 for 4) as int) as year
    from
        stage
    where
        lower(is_deleted) = 'false'
        and x2024_safeties__c != ''

        
    
        

    union all

    select
        id,
        last_name,
        first_name,
        name,
        team_name,
        replace(
            replace(
                'x2024_in_ts__c',
                substring('x2024_in_ts__c' from 1 for strpos('x2024_in_ts__c','_')),
                ''
            ),
            '__c',
            ''
        ) as stat,
        cast(x2024_in_ts__c as int),
        cast(substring('x2024_in_ts__c' from 2 for 4) as int) as year
    from
        stage
    where
        lower(is_deleted) = 'false'
        and x2024_in_ts__c != ''

        
    
        

    union all

    select
        id,
        last_name,
        first_name,
        name,
        team_name,
        replace(
            replace(
                'x2024_sacks__c',
                substring('x2024_sacks__c' from 1 for strpos('x2024_sacks__c','_')),
                ''
            ),
            '__c',
            ''
        ) as stat,
        cast(x2024_sacks__c as int),
        cast(substring('x2024_sacks__c' from 2 for 4) as int) as year
    from
        stage
    where
        lower(is_deleted) = 'false'
        and x2024_sacks__c != ''

        
    
        

    union all

    select
        id,
        last_name,
        first_name,
        name,
        team_name,
        replace(
            replace(
                'x2024_t_ds__c',
                substring('x2024_t_ds__c' from 1 for strpos('x2024_t_ds__c','_')),
                ''
            ),
            '__c',
            ''
        ) as stat,
        cast(x2024_t_ds__c as int),
        cast(substring('x2024_t_ds__c' from 2 for 4) as int) as year
    from
        stage
    where
        lower(is_deleted) = 'false'
        and x2024_t_ds__c != ''

        
    
)

select
    last_name,
    first_name,
    team_name,
    case
        when right(stat, 1) = 's'
        then left(replace(stat, '_', ''), len(replace(stat, '_', '')) - 1 )
        else replace(stat, '_', '')
        end as stat,
    amount,
    year
from
    stats