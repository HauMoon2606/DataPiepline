{% macro get_service_description(service_type)%}
    case {{service_type}}
        when 1 then 'Yellow'
        when 2 then 'Green'
    end
{% endmacro %}