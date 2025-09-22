{% macro list_schemas_and_models() %}
  {%- set schema_map = {} -%}

  {%- for node in graph.nodes.values() if node.resource_type == 'model' -%}
    {%- set schema = node.schema -%}
    {%- set model_name = node.name -%}
    {%- if schema not in schema_map -%}
      {%- do schema_map.update({schema: []}) -%}
    {%- endif -%}
    {%- do schema_map[schema].append(model_name) -%}
  {%- endfor -%}

  {%- set schema_list = [] -%}
  {%- for schema, models in schema_map.items() -%}
    {%- do schema_list.append({'schema': schema, 'models': models}) -%}
  {%- endfor -%}

  {# Instead of SQL copy hack, just print clean JSON #}
  {{ log(tojson(schema_list), info=True) }}

{% endmacro %}