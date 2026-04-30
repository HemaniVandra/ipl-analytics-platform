-- This macro ensures dbt uses ipl_catalog as the database
-- for ALL models regardless of what is set in profiles.yml
-- Without this, dbt may default to a wrong catalog

{% macro generate_database_name(custom_database_name, node) -%}

    {%- set default_database = target.database -%}

    {%- if custom_database_name is none -%}
        {{ default_database }}

    {%- else -%}
        {{ custom_database_name | trim }}

    {%- endif -%}

{%- endmacro %}