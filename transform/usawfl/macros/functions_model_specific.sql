-- Contact model

{% macro clean_contact_states(state) %}
    case
        when right({{ state }}, 4) = ', UK'
        then replace({{ state }}, ', UK', '')
        when right({{ state }}, 3) = '/UK'
        then replace({{ state }}, '/UK', '')
        when right({{ state }}, 8) = ', Canada'
        then replace({{ state }}, ', Canada', '')
        when {{ state }} in ('SÃ£o Paulo', 'São Paulo')
        then 'Sao Paulo'
        else {{ state }}
        end
{% endmacro %}

{% macro clean_contact_countries(country, state) %}
    case
        when replace(lower({{ country }}), '.', '') in ('united states of america', 'united states', 'usa', 'us', 'usaa')
        then 'United States'
        when lower({{ country }}) = 'uk'
        then 'United Kingdom'
        when right({{ state }}, 4) = ', UK'
        then 'United Kingdom'
        when right({{ state }}, 3) = '/UK'
        then 'United Kingdom'
        when right({{ state }}, 8) = ', Canada'
        then 'Canada'
        when lower({{ country }}) = 'can'
        then 'Canada'
        when lower({{ country }}) = 'mex'
        then 'Mexico'
        when lower({{ country }}) = 'per'
        then 'Peru'
        when lower({{ country }}) in ('france', 'fra')
        then 'France'
        when state in ('SÃ£o Paulo', 'São Paulo')
        then 'Brazil'
        else {{ country }}
        end
{% endmacro %}

-- Tournaments

{% macro parse_tournament_date_string(date_string) %}
    split(
        replace(
            regexp_replace(
                replace(
                    replace({{ date_string }}, ' and ', ' '),
                    '-',
                    ' '
                ),
                '([0-9])(st|nd|rd|th)',
                '\\\1',
                'gi'
            ),
            '\',
            ''
        ),
        ' '
    )
{% endmacro %}