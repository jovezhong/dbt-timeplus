from dbt.adapters.clickhouse import ClickHouseColumn


class TestColumn:
    def test_base_types(self):
        verify_column('name', 'uint8', False, False, False, True)
        verify_column('name', 'uint16', False, False, False, True)
        verify_column('name', 'uint32', False, False, False, True)
        verify_column('name', 'uint64', False, False, False, True)
        verify_column('name', 'uint128', False, False, False, True)
        verify_column('name', 'uint256', False, False, False, True)
        verify_column('name', 'int8', False, False, False, True)
        verify_column('name', 'int16', False, False, False, True)
        verify_column('name', 'int32', False, False, False, True)
        verify_column('name', 'int64', False, False, False, True)
        verify_column('name', 'int128', False, False, False, True)
        verify_column('name', 'int256', False, False, False, True)
        str_col = verify_column('name', 'string', True, False, False, False)
        assert str_col.string_size() == 256
        fixed_str_col = verify_column('name', 'fixed_string', True, False, False, False)
        assert fixed_str_col.string_size() == 256
        fixed_str_col = verify_column('name', 'fixed_string(16)', True, False, False, False)
        assert fixed_str_col.string_size() == 16
        verify_column('name', 'decimal(6, 6)', False, True, False, False)
        verify_column('name', 'float32', False, False, True, False)
        verify_column('name', 'float64', False, False, True, False)
        verify_column('name', 'float64', False, False, True, False)
        verify_column('name', 'date', False, False, False, False)
        verify_column('name', 'date32', False, False, False, False)
        verify_column('name', "dateTime('Asia/Istanbul')", False, False, False, False)
        verify_column('name', "uuid", False, False, False, False)

    def test_array_type(self):
        # Test Array of Strings type
        col = ClickHouseColumn(column='name', dtype='array(string)')
        verify_column_types(col, False, False, False, False)
        assert repr(col) == '<ClickhouseColumn name (array(string), is nullable: False)>'

        # Test Array of Nullable Strings type
        col = ClickHouseColumn(column='name', dtype='array(nullable(string))')
        verify_column_types(col, False, False, False, False)
        assert repr(col) == '<ClickhouseColumn name (array(nullable(string)), is nullable: False)>'

        # Test Array of Nullable FixedStrings type
        col = ClickHouseColumn(column='name', dtype='array(nullable(fixed_string(16)))')
        verify_column_types(col, False, False, False, False)
        assert (
            repr(col)
            == '<ClickhouseColumn name (array(nullable(fixed_string(16))), is nullable: False)>'
        )

    def test_low_cardinality_nullable_type(self):
        col = ClickHouseColumn(column='name', dtype='low_cardinality(nullable(string))')
        verify_column_types(col, True, False, False, False)
        assert (
            repr(col)
            == '<ClickhouseColumn name (low_cardinality(nullable(string)), is nullable: True)>'
        )
        col = ClickHouseColumn(column='name', dtype='low_cardinality(nullable(fixed_string(16)))')
        verify_column_types(col, True, False, False, False)
        assert (
            repr(col)
            == '<ClickhouseColumn name (low_cardinality(nullable(string)), is nullable: True)>'
        )

    def test_map_type(self):
        col = ClickHouseColumn(column='name', dtype='map(string, uint64)')
        verify_column_types(col, False, False, False, False)
        assert repr(col) == '<ClickhouseColumn name (map(string, uint64), is nullable: False)>'
        col = ClickHouseColumn(column='name', dtype='map(string, decimal(6, 6))')
        verify_column_types(col, False, False, False, False)
        assert (
            repr(col) == '<ClickhouseColumn name (map(string, decimal(6, 6)), is nullable: False)>'
        )


def verify_column(
    name: str, dtype: str, is_string: bool, is_numeric: bool, is_float: bool, is_int: bool
) -> ClickHouseColumn:
    data_type = 'string' if is_string else dtype
    col = ClickHouseColumn(column=name, dtype=dtype)
    verify_column_types(col, is_string, is_numeric, is_float, is_int)
    assert repr(col) == f'<ClickhouseColumn {name} ({data_type}, is nullable: False)>'

    # Test Nullable dtype.
    nullable_col = ClickHouseColumn(column=name, dtype=f'nullable({dtype})')
    verify_column_types(nullable_col, is_string, is_numeric, is_float, is_int)
    assert (
        repr(nullable_col)
        == f'<ClickhouseColumn {name} (nullable({data_type}), is nullable: True)>'
    )

    # Test low cardinality dtype
    low_cardinality_col = ClickHouseColumn(column=name, dtype=f'low_cardinality({dtype})')
    verify_column_types(low_cardinality_col, is_string, is_numeric, is_float, is_int)
    assert (
        repr(low_cardinality_col)
        == f'<ClickhouseColumn {name} (low_cardinality({data_type}), is nullable: False)>'
    )
    return col


def verify_column_types(
    col: ClickHouseColumn, is_string: bool, is_numeric: bool, is_float: bool, is_int: bool
):
    assert col.is_string() == is_string
    assert col.is_numeric() == is_numeric
    assert col.is_float() == is_float
    assert col.is_integer() == is_int
