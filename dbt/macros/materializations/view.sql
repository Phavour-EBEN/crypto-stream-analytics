{#
  Replaces dbt-synapse's view materialization for the serverless SQL pool.
  The adapter swaps views in with RENAME OBJECT, which only dedicated pools support,
  so re-running a model on serverless fails with "Incorrect syntax near 'rename'".
  CREATE OR ALTER VIEW replaces the view in one statement instead.
#}
{% materialization view, adapter='synapse' %}
  {%- set target_relation = this.incorporate(type='view') -%}

  {{ run_hooks(pre_hooks) }}

  {% call statement('main') -%}
    create or alter view {{ target_relation.include(database=False) }} as
    {{ sql }}
  {%- endcall %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
