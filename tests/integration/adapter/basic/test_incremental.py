import pytest
from dbt.tests.adapter.basic.test_incremental import BaseIncremental, BaseIncrementalNotSchemaChange


class TestIncremental(BaseIncremental):
    pass


incremental_not_schema_change_sql = """
{{ config(materialized="incremental", unique_key="user_id_current_time",on_schema_change="append_new_columns") }}
select
    to_string(1) || '-' || to_string(now64()) as user_id_current_time,
    {% if is_incremental() %}
        'thisis18characters' as platform
    {% else %}
        'okthisis20characters' as platform
    {% endif %}
"""


class TestIncrementalNotSchemaChange(BaseIncrementalNotSchemaChange):
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_not_schema_change.sql": incremental_not_schema_change_sql}
