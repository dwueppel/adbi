from unittest import TestCase
from unittest.mock import Mock
from pathlib import Path
import sqlite3
import adbi
from adbi import ADBI, ADBICursor


class TestADBI(TestCase):

    def test_connect(self):
        conn = sqlite3.connect(':memory:')
        adbi_conn = adbi.connect(conn)

        self.assertIsInstance(adbi_conn, ADBI, "Got correct object returned")
        self.assertEqual(adbi_conn.connection, conn, "Connection has been saved")
        self.assertEqual(adbi_conn.wrapped_db_param_style, sqlite3.paramstyle, "Got expected param style")

        # Try again, forcing the param style.
        adbi_conn = adbi.connect(conn, 'my_style')
        self.assertIsInstance(adbi_conn, ADBI, "Got correct object returned")
        self.assertEqual(adbi_conn.connection, conn, "Connection has been saved")
        self.assertEqual(adbi_conn.wrapped_db_param_style, 'my_style', "Got provided param style")

    def test_initialization(self):
        conn = sqlite3.connect(':memory:')
        adbi_conn = ADBI(conn)

        self.assertIsInstance(adbi_conn, ADBI, "Got correct object returned")
        self.assertEqual(adbi_conn.connection, conn, "Connection has been saved")
        self.assertEqual(adbi_conn.wrapped_db_param_style, sqlite3.paramstyle, "Got expected param style")

        # Try again, forcing the param style.
        adbi_conn = ADBI(conn, 'my_style')
        self.assertIsInstance(adbi_conn, ADBI, "Got correct object returned")
        self.assertEqual(adbi_conn.connection, conn, "Connection has been saved")
        self.assertEqual(adbi_conn.wrapped_db_param_style, 'my_style', "Got provided param style")

        # Once more for a connection that is a submodule.
        mock_conn = Mock()
        mock_conn.__class__.__module__ = 'sqlite3.testing.thing'
        adbi_conn = ADBI(conn)
        self.assertEqual(adbi_conn.connection, conn, "Connection has been saved")
        self.assertEqual(adbi_conn.wrapped_db_param_style, sqlite3.paramstyle, "Got expected param style")

    def test_close(self):
        mock_db = Mock()
        adbi_conn = ADBI(mock_db, 'qmark')

        adbi_conn.close()
        mock_db.close.assert_called_with()

    def test_commit(self):
        mock_db = Mock()
        adbi_conn = ADBI(mock_db, 'qmark')

        adbi_conn.commit()
        mock_db.commit.assert_called_with()

    def test_rollback(self):
        mock_db = Mock(spec=[])
        adbi_conn = ADBI(mock_db, 'qmark')

        # No rollback method exists.
        self.assertTrue(adbi_conn.rollback(), "rollback succeeds")

        # Rollback method exists.
        mock_db = Mock(spec=['rollback'])
        mock_db.rollback = Mock()
        adbi_conn = ADBI(mock_db, 'qmark')
        self.assertTrue(adbi_conn.rollback(), "rollback succeeds")
        mock_db.rollback.assert_called_with()

    def test_cursor(self):
        mock_db = Mock()
        adbi_conn = ADBI(mock_db, 'qmark')

        curs = adbi_conn.cursor()
        self.assertIsInstance(curs, ADBICursor, "Got expected ADBICursor object")
        self.assertEqual(curs._cursor, mock_db.cursor.return_value, "Cursor from original DB connection was used")
        mock_db.cursor.assert_called_with()

    def test_schema_dir_property(self):
        mock_db = Mock()
        adbi_conn = ADBI(mock_db, 'qmark')

        self.assertIsNone(adbi_conn.schema_dir, "No schema directory given to start with")
        # Assign a value.
        adbi_conn.schema_dir = 'tests/sql'
        self.assertEqual(adbi_conn.schema_dir, Path('tests/sql'), "Got expected schema_dir set")

        # What about a path that doesn't exist.
        with self.assertRaises(ValueError):
            adbi_conn.schema_dir = '/foo/bar/'
        # Or we give it a file instead of a dir.
        with self.assertRaises(ValueError):
            adbi_conn.schema_dir = 'tests/test_ADBI.py'

    def test_schema_file_format_propety(self):
        mock_db = Mock()
        adbi_conn = ADBI(mock_db, 'qmark')

        self.assertEqual(adbi_conn.schema_file_format, 'schema-{version}.sql', "Got default schema file format")
        # Adjust the file format.
        adbi_conn.schema_file_format = 'foo_{version}_sql'
        self.assertEqual(adbi_conn.schema_file_format, 'foo_{version}_sql', "Got updated file format")

        # Provide an invalid format.
        with self.assertRaises(ValueError):
            adbi_conn.schema_file_format = 'foo_{0}_sql'

        # Provide a static string
        with self.assertRaises(ValueError):
            adbi_conn.schema_file_format = 'foo_sql'

    def test_validate_schema_table(self):
        conn = sqlite3.connect(':memory:')
        curs = conn.cursor()
        adbi_conn = adbi.connect(conn)

        # Build our schema, it can't exist yet.
        adbi_conn._validate_schema_table()
        # Make sure it exists now by querying.
        curs.execute("SELECT * from _schema_info")
        # Try to build it again, should be a no-op
        adbi_conn._validate_schema_table()
        # Insert an item into the table.
        curs.execute("INSERT INTO _schema_info (variable, value) VALUES ('foo', 'bar')")
        # Query it.
        curs.execute("SELECT value FROM _schema_info WHERE variable = 'foo'")
        row = curs.fetchone()
        self.assertEqual(row[0], 'bar', "Got expected value stored in the schema table")
        # Clean up.
        curs.close()
        conn.close()

    def test_current_schema_version(self):
        conn = sqlite3.connect(':memory:')
        curs = conn.cursor()
        adbi_conn = adbi.connect(conn)

        current_ver = adbi_conn.current_schema_version()
        self.assertIsNone(current_ver, "No current schema version found")

        # Put a version in place, the table should have been auto-created.
        curs.execute("INSERT INTO _schema_info (variable, value) VALUES ('schema_version', '1.0.0')")

        # Query again.
        current_ver = adbi_conn.current_schema_version()
        self.assertEqual(current_ver, '1.0.0', "Got expected version")

        # Clean up
        curs.close()
        conn.close()

    def test_get_upgrade_path(self):
        conn = sqlite3.connect(':memory:')
        curs = conn.cursor()
        adbi_conn = adbi.connect(conn)
        adbi_conn.schema_dir = 'tests/sql'

        # No current schema.
        schemas, version = adbi_conn._get_upgrade_path()
        self.assertEqual(schemas, [Path('tests/sql/schema-current.sql')], "Got expected list of schema files")
        self.assertEqual(version, '1.0.0', "Got expected latest version value")

        # We have an existing version, but it's old.
        curs.execute("INSERT INTO _schema_info (variable, value) VALUES ('schema_version', '0.1.0')")
        schemas, version = adbi_conn._get_upgrade_path()
        self.assertEqual(schemas, [
            Path('tests/sql/schema-0.2.0.sql'),
            Path('tests/sql/schema-1.0.0.sql')
        ], "Got expected list of schema files")
        self.assertEqual(version, '1.0.0', "Got expected latest version value")

        # We are up to date.
        curs.execute("UPDATE _schema_info set value = '1.0.0' WHERE variable = 'schema_version'")
        schemas, version = adbi_conn._get_upgrade_path()
        self.assertEqual(schemas, [], "Nothing to do")
        self.assertEqual(version, '1.0.0', "Got expected latest version value")

    def validate_test_schema(self, curs):
        # Check the contents of all our tables.
        try:
            curs.execute("SELECT id, value FROM table_one")
            table_one = {}
            for row in curs.fetchall():
                table_one[row[0]] = row[1]
            curs.execute("SELECT t_id, value FROM table_two")
            table_two = {}
            for row in curs.fetchall():
                table_two[row[0]] = row[1]
            self.assertEqual(table_one, {1: 'foo', 2: 'bar', 3: 'baz'}, "Got expected values in table_one")
            self.assertEqual(table_two, {4: 'foofoo', 5: 'foobar'}, "Got expected values in table_two")
        except:
            # We are missing a table, or the query cannot be completed. Either
            # way, this is a failure case.
            self.assertTrue(False, "Database schema is not what is expected")

    def test_update_schema_clean(self):
        conn = sqlite3.connect(':memory:')
        curs = conn.cursor()
        adbi_conn = adbi.connect(conn)
        adbi_conn.schema_dir = 'tests/sql'

        # Fresh database.
        adbi_conn.update_schema()
        self.validate_test_schema(curs)

    def test_update_schema_previous_version(self):
        conn = sqlite3.connect(':memory:')
        curs = conn.cursor()
        adbi_conn = adbi.connect(conn)
        adbi_conn.schema_dir = 'tests/sql'

        # Apply a previous version.
        curs.executescript(Path('tests/sql/schema-0.1.0.sql').read_text())
        adbi_conn._validate_schema_table()
        curs.execute("INSERT INTO _schema_info (variable, value) VALUES ('schema_version', '0.1.0')")

        # Database from previous version.
        adbi_conn.update_schema()
        self.validate_test_schema(curs)
