with members as (
    select
        m.*,
        nullif(c.last_name, '') as last_name,
        nullif(c.first_name, '') as first_name,
        nullif(t.team_name, '') as team_name,
        row_number() over(partition by m.id order by m.system_modstamp desc) as updated_order
    from
        {{ source('salesforce_raw', 'members') }} m

    left join
        {{ ref('teams') }} t
    on
        m.team__c = t.id

    left join
        {{ ref('contacts') }} c
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

    {% for col in var('stat_fields') %}
        {% if not loop.first %}

    union all

    select
        id,
        last_name,
        first_name,
        name,
        team_name,
        replace(
            replace(
                '{{ col }}',
                substring('{{ col }}' from 1 for strpos('{{ col }}','_')),
                ''
            ),
            '__c',
            ''
        ) as stat,
        cast({{ col }} as int),
        cast(substring('{{ col }}' from 2 for 4) as int) as year
    from
        stage
    where
        lower(is_deleted) = 'false'
        and {{ col }} != ''

        {% endif %}
    {% endfor %}
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

