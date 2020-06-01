from unittest import TestCase
from unittest.mock import Mock, patch
from pathlib import Path
import sqlite3
import sys
import adbi
from adbi import ADBI, ADBICursor


class TestADBICursor(TestCase):

    def test_initialization(self):
        conn = sqlite3.connect(':memory:')
        curs = conn.cursor()
        adbi_curs = ADBICursor(curs, 'qmark')

        self.assertEqual(adbi_curs._cursor, curs, "Cursor stored correctly")
        self.assertEqual(adbi_curs.wrapped_db_param_style, 'qmark', "Got provided param style set")

    def test_description(self):
        mock_curs = Mock()
        curs = ADBICursor(mock_curs, 'qmark')

        val = curs.description
        self.assertEqual(val, mock_curs.description, "Got unuderlying value from cursor")

    def test_rowcount(self):
        mock_curs = Mock()
        curs = ADBICursor(mock_curs, 'qmark')

        val = curs.rowcount
        self.assertEqual(val, mock_curs.rowcount, "Got unuderlying value from cursor")

    def test_callproc(self):
        mock_curs = Mock(spec=[])
        curs = ADBICursor(mock_curs, 'qmark')

        with self.assertRaises(SystemError):
            curs.callproc('one', 'two', 'three')

        mock_curs = Mock(spec=['callproc'])
        curs = ADBICursor(mock_curs, 'qmark')
        rtn = curs.callproc('one', 'two', 'three')
        mock_curs.callproc.assert_called_with('one', 'two', 'three')

        self.assertEqual(rtn, mock_curs.callproc.return_value, "Got expected return value")

    def test_close(self):
        mock_curs = Mock()
        curs = ADBICursor(mock_curs, 'qmark')
        curs.close()
        mock_curs.close.assert_called_with()

    def test_get_operation_parts(self):
        mock_curs = Mock()
        curs = ADBICursor(mock_curs, 'qmark')

        # String with no parameters.
        sql = "SELECT * FROM table_name;"
        parts = curs._get_operation_parts(sql, None)
        self.assertEqual(parts, ['SELECT * FROM table_name;'], "Only one part returned")

        # String with no parameters.
        sql = "SELECT * FROM table_name;"
        parts = curs._get_operation_parts(sql, [])
        self.assertEqual(parts, ['SELECT * FROM table_name;'], "Only one part returned")

        # String with one positional param
        sql = "SELECT * FROM table_name WHERE foo = %s;"
        parts = curs._get_operation_parts(sql, ['param'])
        self.assertEqual(parts, [
            'SELECT * FROM table_name WHERE foo = ',
            '0',
            ';'
        ], "Got expected parts - single positional param")

        # Multiple positional params
        sql = "SELECT * FROM '%s' WHERE foo = %s and bar like %s;"
        parts = curs._get_operation_parts(sql, ['param1', 'param2', 'param3'])
        self.assertEqual(parts, [
            "SELECT * FROM '",
            '0',
            "' WHERE foo = ",
            '1',
            ' and bar like ',
            '2',
            ';'
        ], "Got expected parts - single positional param")

        # String with one named param
        sql = "SELECT * FROM table_name WHERE foo = %(foo var)s;"
        parts = curs._get_operation_parts(sql, {'foo var': 'param'})
        self.assertEqual(parts, [
            'SELECT * FROM table_name WHERE foo = ',
            'foo var',
            ';'
        ], "Got expected parts - single named param")
        # Multiple named params
        sql = "SELECT * FROM '%(table)s' WHERE foo = %(foo_var)s and bar like %(bar_var)s;"
        parts = curs._get_operation_parts(sql, {
            'table': 'param1',
            'bar_var': 'param2',
            'foo_var': 'param3'
        })
        self.assertEqual(parts, [
            "SELECT * FROM '",
            'table',
            "' WHERE foo = ",
            'foo_var',
            ' and bar like ',
            'bar_var',
            ';'
        ], "Got expected parts - single positional param")


    def test_format_operation_parts_named(self):
        mock_curs = Mock()
        curs = ADBICursor(mock_curs, 'qmark')

        # Only one part given.
        sql, mappings = curs._format_operation_parts_named(['SELECT * FROM foo;'])
        self.assertEqual(sql, 'SELECT * FROM foo;', "Got expected SQL generate - one part")
        self.assertEqual(mappings, {}, "No mappings have been defined")

        # One named variable given.
        sql, mappings = curs._format_operation_parts_named([
            'SELECT * FROM foo WHERE bar = ',
            'named_var',
            ';'
        ])
        self.assertEqual(sql, 'SELECT * FROM foo WHERE bar = :var1;',
            "Got expected generated SQL statement - one named var")
        self.assertEqual(mappings, {'named_var': 'var1'}, "Got expected mappings defined - 1 mapping")

        # More than one named variable provided.
        sql, mappings = curs._format_operation_parts_named([
            "SELECT * FROM '",
            'table_name',
            "' WHERE foo = ",
            'foo_var',
            ' AND bar = ',
            'bar_var'
        ])
        self.assertEqual(sql, "SELECT * FROM ':var1' WHERE foo = :var2 AND bar = :var3",
            "Got expected generated SQL statement - multiple named vars")
        self.assertEqual(mappings, {
            'table_name': 'var1',
            'foo_var': 'var2',
            'bar_var': 'var3'
        }, "Got expected mappings defined - 3 mappings")

    def test_format_operation_parts_char_no_char_given(self):
        mock_curs = Mock()
        curs = ADBICursor(mock_curs, 'qmark')

        # Only one part given - no char given.
        sql, mappings = curs._format_operation_parts_char(['SELECT * FROM foo;'])
        self.assertEqual(sql, 'SELECT * FROM foo;', "Got expected SQL generate - one part")
        self.assertEqual(mappings, [], "No mappings have been defined")

        # One named variable given, no char given.
        sql, mappings = curs._format_operation_parts_char([
            'SELECT * FROM foo WHERE bar = ',
            'named_var',
            ';'
        ])
        self.assertEqual(sql, 'SELECT * FROM foo WHERE bar = :0;',
            "Got expected generated SQL statement - one named var")
        self.assertEqual(mappings, ['named_var'], "Got expected mappings defined - 1 mapping")

        # More than one named variable provided.
        sql, mappings = curs._format_operation_parts_char([
            "SELECT * FROM '",
            'table_name',
            "' WHERE foo = ",
            'foo_var',
            ' AND bar = ',
            'bar_var'
        ])
        self.assertEqual(sql, "SELECT * FROM ':0' WHERE foo = :1 AND bar = :2",
            "Got expected generated SQL statement - multiple named vars")
        self.assertEqual(mappings, ['table_name', 'foo_var', 'bar_var'], "Got expected mappings defined - 3 mappings")

    def test_format_operation_parts_char_char_given(self):
        mock_curs = Mock()
        curs = ADBICursor(mock_curs, 'qmark')

        # Only one part given - no char given.
        sql, mappings = curs._format_operation_parts_char(['SELECT * FROM foo;'], char='?')
        self.assertEqual(sql, 'SELECT * FROM foo;', "Got expected SQL generate - one part")
        self.assertEqual(mappings, [], "No mappings have been defined")

        # One named variable given, no char given.
        sql, mappings = curs._format_operation_parts_char([
            'SELECT * FROM foo WHERE bar = ',
            'named_var',
            ';'
        ], char='?')
        self.assertEqual(sql, 'SELECT * FROM foo WHERE bar = ?;',
            "Got expected generated SQL statement - one named var")
        self.assertEqual(mappings, ['named_var'], "Got expected mappings defined - 1 mapping")

        # More than one named variable provided.
        sql, mappings = curs._format_operation_parts_char([
            "SELECT * FROM '",
            'table_name',
            "' WHERE foo = ",
            'foo_var',
            ' AND bar = ',
            'bar_var'
        ], char='%s')
        self.assertEqual(sql, "SELECT * FROM '%s' WHERE foo = %s AND bar = %s",
            "Got expected generated SQL statement - multiple named vars")
        self.assertEqual(mappings, ['table_name', 'foo_var', 'bar_var'], "Got expected mappings defined - 3 mappings")

    def test_map_params(self):
        mock_curs = Mock()
        curs = ADBICursor(mock_curs, 'qmark')

        # List params:
        params = ['one', 'two', 'three', 'four']
        # Mapping a list using a dict mapping
        #  This is the case where we origainlly had %s used in our string, and
        #  are replacing with an ordered listing. The order should not change.
        mapping = {'0': 0, '1': 1, '2': 2}
        mapped = curs._map_params(params, mapping)
        self.assertEqual(mapped, {0: 'one', 1: 'two', 2: 'three'},
            "Got expected mapping - list using dict")
        # Mapping a list using a list mapping
        mapping = ['1', '2', '0']
        mapped = curs._map_params(params, mapping)
        self.assertEqual(mapped, ['two', 'three', 'one'],
            "Got expected mapping - list using list")

        # Dict Params:
        params = {'foo': 'one', 'bar': 'two', 'baz': 'three', 'qux': 'four'}
        # Mapping a dict using a dict mapping
        mapping = {'foo': 'var1', 'bar': 'var2', 'baz': 'var0'}
        mapped = curs._map_params(params, mapping)
        self.assertEqual(mapped, {'var2': 'two', 'var0': 'three', 'var1': 'one'},
            "Got expected mapping - list using dict")

        # Mapping a dict using a list mapping
        mapping = ['bar', 'baz', 'foo']
        mapped = curs._map_params(params, mapping)
        self.assertEqual(mapped, ['two', 'three', 'one'],
            "Got expected mapping - list using list")

    def test_convert_operation_with_params_named_params(self):
        mock_curs = Mock()
        mock_curs.__class__.__module__ = 'test_db_module'
        curs = ADBICursor(mock_curs, 'unknown')

        sql = "SELECT * FROM '%(table_name)s' WHERE foo = %(foo_var)s AND bar LIKE %(bar_var)s;"
        params = {
            'table_name': 'T_NAME',
            'foo_var': 'FOO',
            'bar_var': 'BAR%'
        }

        # Test an unknown format type
        curs.wrapped_db_param_style = 'unknown'
        with self.assertRaises(SystemError):
            curs._convert_operation_with_params(sql, params)

        # Test a pyformat database conversion.
        curs.wrapped_db_param_style = 'pyformat'
        new_operation, mapping = curs._convert_operation_with_params(sql, params)
        self.assertEqual(new_operation, sql, "SQL remains untouched - pyformat")
        self.assertIsNone(mapping, "No mapping information provided - pyformat")

        # Test a numeric format conversion
        curs.wrapped_db_param_style = 'numeric'
        new_operation, mapping = curs._convert_operation_with_params(sql, params)
        self.assertEqual(
            new_operation,
            "SELECT * FROM ':0' WHERE foo = :1 AND bar LIKE :2;",
            "SQL updated correctly - numeric"
        )
        self.assertEqual(
            mapping,
            [
                'table_name',
                'foo_var',
                'bar_var',
            ],
            "Correct mapping infomraiton provided - numeric"
        )

        # Test a named format conversion
        curs.wrapped_db_param_style = 'named'
        new_operation, mapping = curs._convert_operation_with_params(sql, params)
        self.assertEqual(
            new_operation,
            "SELECT * FROM ':var1' WHERE foo = :var2 AND bar LIKE :var3;",
            "SQL updated correctly - numeric"
        )
        self.assertEqual(
            mapping,
            {
                'table_name': 'var1',
                'foo_var': 'var2',
                'bar_var': 'var3',
            },
            "Correct mapping infomraiton provided - named"
        )

        # Test a format format conversion
        curs.wrapped_db_param_style = 'format'
        new_operation, mapping = curs._convert_operation_with_params(sql, params)
        self.assertEqual(
            new_operation,
            "SELECT * FROM '%s' WHERE foo = %s AND bar LIKE %s;",
            "SQL updated correctly - format"
        )
        self.assertEqual(
            mapping,
            [
                'table_name',
                'foo_var',
                'bar_var',
            ],
            "Correct mapping infomraiton provided - format"
        )

        # Test a qmark format conversion
        curs.wrapped_db_param_style = 'qmark'
        new_operation, mapping = curs._convert_operation_with_params(sql, params)
        self.assertEqual(
            new_operation,
            "SELECT * FROM '?' WHERE foo = ? AND bar LIKE ?;",
            "SQL updated correctly - qmark"
        )
        self.assertEqual(
            mapping,
            [
                'table_name',
                'foo_var',
                'bar_var',
            ],
            "Correct mapping infomraiton provided - qmark"
        )

    def test_convert_operation_with_params_positional_params(self):
        mock_curs = Mock()
        mock_curs.__class__.__module__ = 'test_db_module'
        curs = ADBICursor(mock_curs, 'unknown')

        sql = "SELECT * FROM '%s' WHERE foo = %s AND bar LIKE %s;"
        params = [
            'T_NAME', 'FOO','BAR%'
            ]

        # Test an unknown format type
        curs.wrapped_db_param_style = 'unknown'
        with self.assertRaises(SystemError):
            curs._convert_operation_with_params(sql, params)

        # Test a pyformat database conversion.
        curs.wrapped_db_param_style = 'pyformat'
        new_operation, mapping = curs._convert_operation_with_params(sql, params)
        self.assertEqual(new_operation, sql, "SQL remains untouched - pyformat")
        self.assertIsNone(mapping, "No mapping information provided - pyformat")

        # Test a numeric format conversion
        curs.wrapped_db_param_style = 'numeric'
        new_operation, mapping = curs._convert_operation_with_params(sql, params)
        self.assertEqual(
            new_operation,
            "SELECT * FROM ':0' WHERE foo = :1 AND bar LIKE :2;",
            "SQL updated correctly - numeric"
        )
        self.assertEqual(
            mapping,
            ['0', '1', '2'],
            "Correct mapping infomraiton provided - numeric"
        )

        # Test a named format conversion
        curs.wrapped_db_param_style = 'named'
        new_operation, mapping = curs._convert_operation_with_params(sql, params)
        self.assertEqual(
            new_operation,
            "SELECT * FROM ':var1' WHERE foo = :var2 AND bar LIKE :var3;",
            "SQL updated correctly - numeric"
        )
        self.assertEqual(
            mapping,
            {
                '0': 'var1',
                '1': 'var2',
                '2': 'var3',
            },
            "Correct mapping infomraiton provided - named"
        )

        # Test a format format conversion
        curs.wrapped_db_param_style = 'format'
        new_operation, mapping = curs._convert_operation_with_params(sql, params)
        self.assertEqual(
            new_operation,
            "SELECT * FROM '%s' WHERE foo = %s AND bar LIKE %s;",
            "SQL updated correctly - format"
        )
        self.assertEqual(
            mapping,
            ['0', '1', '2'],
            "Correct mapping infomraiton provided - format"
        )

        # Test a qmark format conversion
        curs.wrapped_db_param_style = 'qmark'
        new_operation, mapping = curs._convert_operation_with_params(sql, params)
        self.assertEqual(
            new_operation,
            "SELECT * FROM '?' WHERE foo = ? AND bar LIKE ?;",
            "SQL updated correctly - qmark"
        )
        self.assertEqual(
            mapping,
            ['0', '1', '2'],
            "Correct mapping infomraiton provided - qmark"
        )

    @patch('adbi.ADBICursor._convert_operation_with_params')
    @patch('adbi.ADBICursor._map_params')
    def test_execute(self, mock_map_params, mock_convert_op):
        mock_curs = Mock()
        curs = ADBICursor(mock_curs, 'qmark')

        # No params provided.
        mock_convert_op.return_value = ('test sql', None)
        curs.execute("SOME SQL")
        mock_convert_op.assert_called_with("SOME SQL", None)
        mock_map_params.assert_not_called()
        mock_curs.execute.assert_called_with('test sql')


        # No mapping parameters returned.
        mock_convert_op.return_value = ('test sql', None)
        curs.execute("SOME SQL", ['param'])
        mock_convert_op.assert_called_with("SOME SQL", ['param'])
        mock_map_params.assert_not_called()
        mock_curs.execute.assert_called_with('test sql', ['param'])

        # Mapping parameters returned.
        mock_curs.reset_mock()
        mock_convert_op.return_value = ('test sql', [0, 1])
        mock_map_params.return_value = ['foo']
        curs.execute("SOME SQL", ['param'])
        mock_convert_op.assert_called_with("SOME SQL", ['param'])
        mock_map_params.assert_called_with(['param'], [0, 1])
        mock_curs.execute.assert_called_with('test sql', ['foo'])

    @patch('adbi.ADBICursor._convert_operation_with_params')
    @patch('adbi.ADBICursor._map_params')
    def test_executemany(self, mock_map_params, mock_convert_op):
        mock_curs = Mock()
        curs = ADBICursor(mock_curs, 'qmark')

        # No mapping parameters returned.
        mock_convert_op.return_value = ('test sql', None)
        curs.executemany("SOME SQL", [['param'], ['param2']])
        mock_convert_op.assert_called_with("SOME SQL", ['param'])
        mock_map_params.assert_not_called()
        mock_curs.executemany.assert_called_with('test sql', [['param'], ['param2']])

        # Mapping parameters returned.
        mock_curs.reset_mock()
        mock_convert_op.return_value = ('test sql', [0, 1])
        mock_map_params.return_value = ['foo']
        curs.executemany("SOME SQL", [['param'], ['param2']])
        mock_convert_op.assert_called_with("SOME SQL", ['param'])
        mock_map_params.assert_called_with(['param2'], [0, 1])
        mock_curs.executemany.assert_called_with('test sql', [['foo'], ['foo']])

    def test_fetchone(self):
        mock_curs = Mock()
        curs = ADBICursor(mock_curs, 'qmark')
        rtn = curs.fetchone()
        mock_curs.fetchone.assert_called_with()

        self.assertEqual(rtn, mock_curs.fetchone.return_value, "Got underlying cursors return value")

    def test_fetchmany(self):
        mock_curs = Mock()
        mock_curs.arraysize = 10
        curs = ADBICursor(mock_curs, 'qmark')
        rtn = curs.fetchmany()
        mock_curs.fetchmany.assert_called_with(10)
        self.assertEqual(rtn, mock_curs.fetchmany.return_value, "Got underlying cursors return value")

        # Providing a size
        rtn = curs.fetchmany(20)
        mock_curs.fetchmany.assert_called_with(20)
        self.assertEqual(rtn, mock_curs.fetchmany.return_value, "Got underlying cursors return value")

    def test_fetchall(self):
        mock_curs = Mock()
        curs = ADBICursor(mock_curs, 'qmark')
        rtn = curs.fetchall()
        mock_curs.fetchall.assert_called_with()

        self.assertEqual(rtn, mock_curs.fetchall.return_value, "Got underlying cursors return value")

    def test_nextset(self):
        mock_curs = Mock(spec=[])
        curs = ADBICursor(mock_curs, 'qmark')

        with self.assertRaises(SystemError):
            curs.nextset()

        mock_curs = Mock(spec=['nextset'])
        curs = ADBICursor(mock_curs, 'qmark')
        rtn = curs.nextset()
        mock_curs.nextset.assert_called_with()

        self.assertEqual(rtn, mock_curs.nextset.return_value, "Got expected return value")

    def test_arraysize_property(self):
        mock_curs = Mock()
        curs = ADBICursor(mock_curs, 'qmark')

        curs.arraysize = 10
        self.assertEqual(mock_curs.arraysize, 10, "Underlying cursor size set correctly")
        self.assertEqual(curs.arraysize, 10, "arraysize value returned correctly")

        curs.arraysize = 20
        self.assertEqual(mock_curs.arraysize, 20, "Underlying cursor size set correctly")
        self.assertEqual(curs.arraysize, 20, "arraysize value returned correctly")

    def test_setinputsize(self):
        mock_curs = Mock()
        curs = ADBICursor(mock_curs, 'qmark')
        curs.setinputsize(10, 20)
        mock_curs.setinputsize.assert_called_with(10, 20)

    def test_setoutputsize(self):
        mock_curs = Mock()
        curs = ADBICursor(mock_curs, 'qmark')
        curs.setoutputsize(10, 'column')
        mock_curs.setoutputsize.assert_called_with(10, 'column')

    def test_executescript(self):
        # executescript is not defined by underlying cursor.
        mock_curs = Mock(spec=['execute'])
        curs = ADBICursor(mock_curs, 'qmark')
        curs.executescript('test script')
        mock_curs.execute.assert_called_with('test script')

        # execute script is defined.
        mock_curs = Mock(spec=['execute', 'executescript'])
        curs = ADBICursor(mock_curs, 'qmark')
        curs.executescript('test script')
        mock_curs.executescript.assert_called_with('test script')
        mock_curs.execute.assert_not_called()

    def test_executefile(self):
        mock_curs = Mock(spec=['execute'])
        curs = ADBICursor(mock_curs, 'qmark')

        exec_file = Path('tests/sql/schema-current.sql')
        exec_data = exec_file.read_text()

        # Pass in a Path object.
        curs.executefile(exec_file)
        mock_curs.execute.assert_called_with(exec_data)

        # Pass in a string.
        mock_curs.reset_mock()
        curs.executefile(str(exec_file))
        mock_curs.execute.assert_called_with(exec_data)
