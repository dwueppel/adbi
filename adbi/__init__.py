'''
The *agnostic database interface* (*adbi*) module provides the freedom of hand
crafting your SQL queries while remaining database agnostic. The goal of this
module is to provide an abstraction layer that remains database agnostic.
Using this module should allow you to swap out the database being used without
having to adjst your codebase. Provided you stick to standard SQL statements
and do not use any database specific features you should be able to adjust
your connection call and use an alternative database.

The adbi module also incorporates a basic database versioning framework. By
providing the current full set of SQL statements to create the current
database and a separate file for each version update, ADBI is able to validate
and perform upgrades from previous versions to the current version.
'''
from pathlib import Path
import re
import sys


apilevel = '2.0'
threadsafety = 1
paramstyle = 'pyformat'


def connect(conn, paramstyle=None):
    '''
    Create a new ADBI connection object.
    :param conn: the connection object to use when connecting to the
        database.
    '''
    return ADBI(conn, paramstyle)


class ADBI :
    '''
    The ADBI object is an extra abstraction around the DBI 2.0 specification.
    This object allows you to connect to various databases while still using
    the same query placehold strategies. In addition, the ADBI object provides
    a standardized way of maintaining database versioning and validating that
    a database is currently at the latest schema version.
    '''

    def __init__(self, conn, paramstyle=None):
        '''
        Initialize a DBN object. Optionally provide a connection object to
        antoher database. If the connection object is provided this ADBI object
        will be initalized connected with that connection object.
        '''
        self.connection = conn
        self.wrapped_db_param_style = paramstyle
        if not self.wrapped_db_param_style:
            parts = conn.__class__.__module__.split('.')
            parts_used = len(parts)
            while parts_used > 0 and not self.wrapped_db_param_style:
                module = sys.modules['.'.join(parts[:parts_used])]
                if hasattr(module, 'paramstyle'):
                    self.wrapped_db_param_style = module.paramstyle
                parts_used -= 1
        if not self.wrapped_db_param_style:
            raise SystemError("Unable to determine a paramstyle for the given connection")
        self._schema_directory = None
        self._schema_file_format = "schema-{version}.sql"

    def close(self):
        '''
        close the current database connection
        '''
        return self.connection.close()

    def commit(self):
        '''
        Commit any pending transaction to the database.
        '''
        return self.connection.commit()

    def rollback(self):
        '''
        Rollback a transaction.
        '''
        if hasattr(self.connection, 'rollback'):
            return self.connection.rollback()
        return True

    def cursor(self):
        '''
        Return a ADBICursor object for this ADBI object.
        '''
        return ADBICursor(self.connection.cursor(), self.wrapped_db_param_style)

    ## Schema management methods ##

    @property
    def schema_dir(self):
        '''
        Return the currently set schema directory. This is the location where
        schema SQL files can be found.
        '''
        return self._schema_directory

    @schema_dir.setter
    def schema_dir(self, value):
        '''
        Sets the schema_dir value. Validates the directory exists and converts
        the value into a Path object if it is not already one.
        '''
        value = Path(value)
        if not value.is_dir():
            raise ValueError("Given path value is not a directory that exists")
        self._schema_directory = value

    @property
    def schema_file_format(self):
        '''
        Return the currently set schema file format.
        '''
        return self._schema_file_format

    @schema_file_format.setter
    def schema_file_format(self, value):
        '''
        Set the file format to be used for the schema files. This must contain
        one format parameter of {version}. This is replaced with the version
        of the schema to use. An ValueError is raised if the format value does
        not comply.
        '''
        try:
            test_match = value.format(version="--TEST-VERSION--")
            if '--TEST-VERSION--' not in test_match:
                raise ValueError()
        except (IndexError, KeyError, ValueError):
            raise ValueError("Given format does not have a valid {version} format param")
        self._schema_file_format = value

    def _validate_schema_table(self):
        '''
        Create the table to hold schema information.
        '''
        curs = self.cursor()
        try:
            # Attemtp to get the schema_version from the _scheam_info table.
            # If this fails, then the table does not exist. We should then
            # create it.
            curs.execute("SELECT value FROM _schema_info WHERE variable = 'schema_version'")
        except Exception:
            # Create the _schema_info table.
            curs.execute("""
                CREATE TABLE _schema_info (
                    variable VARCHAR(64) NOT NULL PRIMARY KEY,
                    value varchar(128) NOT NULL
                )
            """)
            curs.close()
            self.commit()

    def current_schema_version(self):
        '''
        Returns the current schema version of the database.
        '''
        self._validate_schema_table()
        # Build the query we are going to use.
        sql = "SELECT value FROM _schema_info WHERE variable = 'schema_version'"
        curs = self.cursor()
        curs.execute(sql)
        row = curs.fetchone()
        curs.close()

        if row:
            return row[0]
        return None

    def _get_upgrade_path(self):
        '''
        Return an ordered list of the schema files to apply in order to
        upgrade the database to the current version.
        '''
        # Get all of the files in the schema_path, and parse them for version
        # information.
        version_re = re.compile(self.schema_file_format.format(version='(.*?)'))
        schema_files = {}
        latest_version = None
        for schema_file in sorted(self.schema_dir.iterdir()):
            match = version_re.match(str(schema_file.name))
            if match:
                # Skip the 'current' version.
                if match.group(1) == 'current':
                    continue
                # Append the schema to our schema list.
                schema_files[match.group(1)] = schema_file
                latest_version = match.group(1)

        # Get our current schema version.
        current_version = self.current_schema_version()
        schemas = []
        if not current_version:
            # If we don't have a current version, then we just return the
            # 'current' schema file.
            schema_file = self.schema_file_format.format(version='current')
            schema = self.schema_dir.joinpath(schema_file)
            if not schema.exists():
                raise SystemError("Cannot find the current schema ({0}) in the schema_dir ({1})".format(
                    schema_file, self.schema_dir))
            schemas.append(schema)
        else:
            # Now we just have to sort the schema files by version, and collect
            # all that are greater than the current version.
            for schema_version in sorted(schema_files):
                if schema_version > current_version:
                    schemas.append(schema_files[schema_version])
        return schemas, latest_version

    def update_schema(self):
        '''
        Upgrade the database to the most schema version. Scan the schema_dir
        for the available schema files. If no current schema exists, use the
        'current' version schema. Otherwise apply the versioned schemas in
        order (textualy sorted) until we reach the current version.
        '''
        schemas, latest_version = self._get_upgrade_path()
        curs = self.cursor()
        for schema in schemas:
            curs.executefile(schema)
        curs.execute("UPDATE _schema_info SET value = %s WHERE variable = 'schema_version'", (latest_version,))
        curs.close()
        self.commit()


class ADBICursor:
    '''
    The ADBICursor object is a warpper around an existing database cursor
    object. Most method calls are directly passed through to the undlerlying
    database object. However, database queries are intercepted so that a
    unified query placehold replacement strategy can be used (pyformat).
    '''

    def __init__(self, cursor, paramstyle):
        '''
        Initlaize a cursor. An exsiting database cursor is required.
        '''
        self._cursor = cursor
        self.wrapped_db_param_style = paramstyle

    @property
    def description(self):
        '''
        Return the current description of the cursor.
        '''
        return self._cursor.description

    @property
    def rowcount(self):
        '''
        Return the current rowcount.
        '''
        return self._cursor.rowcount

    def callproc(self, procname, *params):
        '''
        Call a stored database procedure with the given name. The sequence of
        parameters must contain one entry for each argument that the procedure
        expects. The result of the call is returned as modified copy of the
        input sequence. Input parameters are left untouched, output and
        input/output parameters replaced with possibly new values.
        '''
        if hasattr(self._cursor, 'callproc'):
            return self._cursor.callproc(procname, *params)
        raise SystemError("Underlying database cursor does not support the callproc method.")

    def close(self):
        '''
        Close the cursor now (rather than whenever __del__ is called).
        '''
        return self._cursor.close()

    def _get_operation_parts(self, operation, params):
        '''
        Return an array of the parts of the operation split apart so that a
        new operation can be generated.
        '''
        if not params:
            return [operation]
        # Do we have a list or a dict for vars?
        if isinstance(params, dict):
            parse_vars = {var: '<!--VAR:{0}-->'.format(var) for var in params}
        else:
            # We have a list or tuple for our vars.
            parse_vars = tuple([
                '<!--VAR:{0}-->'.format(idex) for idex in range(len(params))
            ])

        # Format the operation using the formatted placeholders.
        formatted_string = operation % parse_vars
        # Split the operation so that we can get the unformatted parts, and
        # the variable place holders.
        operation_parts = re.split('<!--VAR:(.*?)-->', formatted_string)

        return operation_parts

    def _format_operation_parts_named(self, parts):
        '''
        Format the parts of the operation to replace the original placeholders
        with named placeholders of the format :name. Also, return a mapping of
        original placeholder names to the new variable names.
        '''
        query = ''
        mappings = {}
        var_count = 0
        while len(parts) >= 2:
            (part, var) = parts[:2]
            parts = parts[2:]
            if var not in mappings:
                var_count += 1
                var_name = 'var{0}'.format(var_count)
                mappings[var] = var_name
            else:
                var_name = mappings[var]

            query += part + ':{0}'.format(var_name)
        if parts:
            query += parts[0]

        return query, mappings

    def _format_operation_parts_char(self, parts, char=None):
        '''
        Format the operation replaceing the original placehodlers with static
        character placeholders if a char is provided (can be more than one
        character). If no char is given, replace with incremented numeric
        placeholders such as :0.
        '''
        query = ''
        idex = 0
        var_lookups = []
        while len(parts) >= 2:
            (part, var) = parts[:2]
            parts = parts[2:]
            if char:
                query += part + char
            else:
                query += part + ':{0}'.format(idex)
                idex += 1
            var_lookups.append(var)
        if parts:
            query += parts[0]

        return query, var_lookups

    def _map_params(self, params, mapping):
        '''
        Take the given parameter list and build a new list to match the given
        mapping. Mappings can be a list of dictionary lookup names or a
        dictionary representing the original name and the new mapped names.
        '''
        # If the params is not a dict, convert to a dict with index keys.
        if not isinstance(params, dict):
            params = {str(idex): params[idex] for idex in range(len(params))}
        if isinstance(mapping, dict):
            # We have a dictionary lookup mapping.
            new_params = {mapping[orig_name]: params[orig_name] for orig_name in mapping}
        else:
            # We have a direct list mapping.
            new_params = [params[orig_name] for orig_name in mapping]

        return new_params

    def _convert_operation_with_params(self, operation, params):
        '''
        Given an operation in pyformat format. Convert this into the format
        desired by the underlying database format. Optionally, the given
        params or sequence of params will be converted to function alongside
        the newly created operation string.
        '''
        # If we are converting to pyformat, we've got nothing to do. Just
        # return our relevant pieces.
        if self.wrapped_db_param_style == 'pyformat':
            return operation, None

        # Collect the variables that we could use in the operation and create
        # placeholders that are easily parsed.
        parts = self._get_operation_parts(operation, params)

        # Now determine how we want to reformat the operation.
        new_operation = ''
        var_lookup = None
        if self.wrapped_db_param_style == 'qmark':
            return self._format_operation_parts_char(parts, char='?')
        elif self.wrapped_db_param_style == 'numeric':
            return self._format_operation_parts_char(parts)
        elif self.wrapped_db_param_style == 'named':
            return self._format_operation_parts_named(parts)
        elif self.wrapped_db_param_style == 'format':
            return self._format_operation_parts_char(parts, char='%s')

        raise SystemError("An unhandled type of format style has been found: {0}".format(self.wrapped_db_param_style))

    def execute(self, operation, params=None):
        '''
        Prepare and execute a database operation (query or command).

        Parameters may be provided as sequence or mapping and will be bound to
        variables in the operation. Variables are provided as named
        parameters. These are substituted into the operation string using the
        pyformat formatting method.

        Return values are not defined.
        '''
        # Adjust our operation and parameters.
        (operation, mapping) = self._convert_operation_with_params(operation, params)
        if mapping:
            params = self._map_params(params, mapping)
        # Now execute the given operation.
        if params:
            self._cursor.execute(operation, params)
        else:
            self._cursor.execute(operation)

    def executemany(self, operation, seq_of_params):
        '''
        Prepare a database operation (query or command) and then execute it
        against all parameter sequences or mappings found in the sequence
        seq_of_parameters.
        '''
        (operation, mapping) = self._convert_operation_with_params(operation, seq_of_params[0])
        if mapping:
            for idex, params in enumerate(seq_of_params):
                seq_of_params[idex] = self._map_params(params, mapping)
        self._cursor.executemany(operation, seq_of_params)

    def fetchone(self):
        '''
        Fetch the next row of a query result set, returning a single sequence,
        or None when no more data is available.
        '''
        return self._cursor.fetchone()

    def fetchmany(self, size=None):
        '''
        Fetch the next set of rows of a query result, returning a sequence of
        sequences (e.g. a list of tuples). An empty sequence is returned when
        no more rows are available.
        '''
        if not size:
            size = self._cursor.arraysize
        return self._cursor.fetchmany(size)

    def fetchall(self):
        '''
        Fetch all (remaining) rows of a query result, returning them as a
        sequence of sequences (e.g. a list of tuples). Note that the cursor's
        arraysize attribute can affect the performance of this operation.
        '''
        return self._cursor.fetchall()

    def nextset(self):
        '''
        This method will make the cursor skip to the next available set,
        discarding any remaining rows from the current set.
        '''
        if hasattr(self._cursor, 'nextset'):
            return self._cursor.nextset()
        raise SystemError("Underlying database cursor does not support the nextset method.")

    @property
    def arraysize(self):
        '''
        Return the arraysize property.
        '''
        return self._cursor.arraysize

    @arraysize.setter
    def arraysize(self, value):
        '''
        Set the arraysize property.
        '''
        self._cursor.arraysize = value

    def setinputsize(self, *sizes):
        '''
        This can be used before a call to execute() to predefine memory
        areas for the operation's parameters.

        sizes is specified as a sequence — one item for each input parameter.
        The item should be a Type Object that corresponds to the input that
        will be used, or it should be an integer specifying the maximum length
        of a string parameter. If the item is None, then no predefined memory
        area will be reserved for that column (this is useful to avoid
        predefined areas for large inputs).
        '''
        return self._cursor.setinputsize(*sizes)

    def setoutputsize(self, size, *column):
        '''
        Set a column buffer size for fetches of large columns (e.g. LONGs,
        BLOBs, etc.). The column is specified as an index into the result
        sequence. Not specifying the column will set the default size for all
        large columns in the cursor.
        '''
        self._cursor.setoutputsize(size, *column)

    def executescript(self, script):
        '''
        Execute the given script. Some databases natively support this method
        already. Otherwise do our best to find a suitable alternative.
        '''
        # Is this natively supported?
        if hasattr(self._cursor, 'executescript'):
            self._cursor.executescript(script)
        else:
            # Fallback of just trying execute.
            self._cursor.execute(script)

    def executefile(self, path):
        '''
        Read the contents of the given path and execute the script held within.
        '''
        if not isinstance(path, Path):
            path = Path(path)
        script = path.read_text()
        self.executescript(script)
