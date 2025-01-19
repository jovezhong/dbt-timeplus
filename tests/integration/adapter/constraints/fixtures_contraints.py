contract_model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: int32
        description: hello
      - name: color
        data_type: string
      - name: date_day
        data_type: date
  - name: my_model_error
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: int32
        description: hello
        tests:
          - unique
      - name: color
        data_type: string
      - name: date_day
        data_type: Date
  - name: my_model_wrong_order
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: uint32
        description: hello
        tests:
          - unique
      - name: color
        data_type: string
      - name: date_day
        data_type: date
  - name: my_model_wrong_name
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: int32
        description: hello
      - name: color
        data_type: string
      - name: date_day
        data_type: date
"""


# model columns in a different order to schema definitions
my_model_wrong_order_sql = """
{{
  config(
    materialized = "table"
  )
}}

select
  'blue' as color,
  1::uint32 as id,
  to_date('2019-01-01') as date_day
"""


# model columns name different to schema definitions
my_model_wrong_name_sql = """
{{
  config(
    materialized = "table"
  )
}}

select
  'blue' as color,
  1 as error,
  '2019-01-01' as date_day
"""


my_model_data_type_sql = """
{{{{
  config(
    materialized = "table"
  )
}}}}

select
  {sql_value} as wrong_data_type_column_name
"""


model_data_type_schema_yml = """
version: 2
models:
  - name: my_model_data_type
    config:
      contract:
        enforced: true
    columns:
      - name: wrong_data_type_column_name
        data_type: {data_type}
"""

my_model_view_wrong_name_sql = """
{{
  config(
    materialized = "view"
  )
}}

select
  'blue' as color,
  1 as error,
  to_date('2019-01-01') as date_day
"""

my_model_view_wrong_order_sql = """
{{
  config(
    materialized = "view"
  )
}}

select
  'blue' as color,
  1::uint32 as id,
  to_date('2019-01-01') as date_day
"""


my_model_incremental_wrong_order_sql = """
{{
  config(
    materialized = "incremental",
    on_schema_change='append_new_columns'
  )
}}

select
  'blue' as color,
  1::uint32 as id,
  to_date('2019-01-01') as date_day
"""

my_model_incremental_wrong_name_sql = """
{{
  config(
    materialized = "incremental",
    on_schema_change='append_new_columns'
  )
}}

select
  'blue' as color,
  1 as error,
  '2019-01-01' as date_day
"""

constraint_model_schema_yml = """
version: 2
models:
  - name: bad_column_constraint_model
    materialized: table
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: int32
        constraints:
          - type: check
            expression: '> 0'
      - name: color
        data_type: string
      - name: date_day
        data_type: date
  - name: bad_foreign_key_model
    config:
      contract:
        enforced: true
    constraints:
      - type: foreign_key
        columns: [ id ]
        expression: 'foreign_key_model (id)'
    columns:
      - name: id
        data_type: int32
  - name: check_constraints_model
    config:
      contract:
        enforced: true
    constraints:
      - type: check
        name: valid_id
        expression: 'id > 100 and id < 200'
    columns:
      - name: id
        data_type: int32
      - name: color
        data_type: string
      - name: date_day
        data_type: date
"""

bad_column_constraint_model_sql = """
{{
  config(
    materialized = "table"
  )
}}

SELECT 5::int32 as id, 'black' as color, to_date('2023-01-01') as date_day
"""

bad_foreign_key_model_sql = """
{{
  config(
    materialized = "table"
  )
}}

SELECT 1::int32 as id
"""

check_constraints_model_sql = """
{{
  config(
    materialized = "table",
  )
}}

select
  'blue' as color,
  101::int32 as id,
  to_date('2019-01-01') as date_day
"""

check_constraints_model_fail_sql = """
{{
  config(
    materialized = "table",
  )
}}

select
  'blue' as color,
  1::int32 as id,
  to_date('2019-01-01') as date_day
"""
