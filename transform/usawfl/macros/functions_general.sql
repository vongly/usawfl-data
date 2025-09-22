{% macro return_numbers_from_string(string) %}
    right(regexp_replace(cast({{ string }} as varchar), '[^0-9]', '', 'g'), 10)
{% endmacro %}

{% macro format_phone_number(phone) %}
    if(
        cast({{ phone }} as varchar) is null,
        null,
        '(' || substring(cast({{ phone }} as varchar) from 1 for 3) || ') ' || substring(cast({{ phone }} as varchar) from 3 for 3) || '-' || substring(cast({{ phone }} as varchar) from 7 for 4)
    )
{% endmacro %}

{% macro clean_to_5_digit_zip(zipcode) %}
    case
        when length(cast({{ zipcode }} as varchar)) = 5
        then cast({{ zipcode }} as varchar)
        when length(cast({{ zipcode }} as varchar)) < 5
        then right('0000' || cast({{ zipcode }} as varchar), 5)
        when length(cast({{ zipcode }} as varchar)) > 5
        then left(right('0000' || cast({{ zipcode }} as varchar), 9), 5)
        end
{% endmacro %}

{% macro clean_punct(string) %}
    regexp_replace({{ string }}, '[\.`]', '', 'g')
{% endmacro %}

{% macro title_name(string) %}
    case
        when lower(left({{ string }}, 2)) = 'mc'
        then concat(
                upper(substring({{ string }}, 1, 1)),
                lower(substring({{ string }}, 2, 1)),
                upper(substring({{ string }}, 3, 1)),
                lower(substring({{ string }}, 4, length({{ string }})))
            )
        else concat(
                upper(substring({{ string }}, 1, 1)),
                lower(substring({{ string }}, 2, length({{ string }})))
            )
        end
{% endmacro %}