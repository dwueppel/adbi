# Abstract Database Interface #

The Abstract Database Interface (adbi) is a library to abstract out the
interaction between your application and the specific database you are using.
adbi has been designed to allow you to write database agnostic code. When
using adbi you should be able to change the underlying database by simply
providing a different database connection to the adbi manager.

## Requirements ##

 * Python 3.6+
 * Another DB API 2.0 compliant database library (such as sqlite3).

## Features ##

 * A generic DB API 2.0 compliant database interface
 * Integrated database versioning and upgrading

## Installation ##

Standard python3 installation.

```
python3 setup.py install
```

## Usage ##

```
use adbi
use sqlite3

sqlite_conn = sqlite3.connect(':memory:')
conn = adbi.connect(sqlite3)

curs = conn.cursor()

curs.execute("SELECT 1")
row = curs.fetchone()

print("Selected", row[0])
