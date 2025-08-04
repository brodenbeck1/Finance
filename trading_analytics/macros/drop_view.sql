{% macro drop_view(database, schema, view_name) %}
  {% set query %}
    DROP VIEW IF EXISTS {{ database }}.{{ schema }}.{{ view_name }};
  {% endset %}
  {{ log(query, info=true) }} {# This logs the query for verification #}
  {% do run_query(query) %} {# This executes the query #}
{% endmacro %}