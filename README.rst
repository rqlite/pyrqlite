=======
pyrqlite
=======
This package contains a pure-Python rqlite client library.

.. contents::

Requirements
-------------

* Python -- one of the following:

  - CPython_ >= 2.7 or >= 3.3

* rqlite Server


Installation
------------

The last stable release is available on github and can be installed with ``pip``::

    $ cd
    $ git clone https://github.com/rqlite/pyrqlite.git
    $ pip install ./pyrqlite

Alternatively (e.g. if ``pip`` is not available), a tarball can be downloaded
from GitHub and installed with Setuptools::

    $ # X.X is the desired pyrqlite version (e.g. 0.5 or 0.6).
    $ curl -L https://github.com/rqlite/tarball/pyrqlite-X.X | tar xz
    $ cd pyrqlite*
    $ python setup.py install
    $ # The folder pyrqlite* can be safely removed now.

Test Suite
----------

To run all the tests, execute the script ``setup.py``::

    $ python setup.py test

pytest (https://pytest.org/) and pytest-cov are required to run the test
suite. They can both be installed with ``pip``

Example
-------

The following code creates a connection and executes some statements:

.. code:: python

    import pyrqlite.dbapi2 as dbapi2

    # Connect to the database
    connection = dbapi2.connect(
        host='localhost',
        port=4001,
    )

    try:
        with connection.cursor() as cursor:
            cursor.execute('CREATE TABLE foo (id integer not null primary key, name text)')
            cursor.executemany('INSERT INTO foo(name) VALUES(?)', seq_of_parameters=(('a',), ('b',)))

        with connection.cursor() as cursor:
            # Read a single record with qmark parameter style
            sql = "SELECT `id`, `name` FROM `foo` WHERE `name`=?"
            cursor.execute(sql, ('a',))
            result = cursor.fetchone()
            print(result)
            # Read a single record with named parameter style
            sql = "SELECT `id`, `name` FROM `foo` WHERE `name`=:name"
            cursor.execute(sql, {'name': 'b'})
            result = cursor.fetchone()
            print(result)
    finally:
        connection.close()

.. code:: python

This example will print:


    OrderedDict([('id', 1), ('name', 'a')])
    OrderedDict([('id', 2), ('name', 'b')])
    
Paramstyle
---------

Only qmark and named paramstyles (as defined in PEP 249) are supported. 

Limitations
---------
Transactions are not supported.

Resources
---------
DB-API 2.0: http://www.python.org/dev/peps/pep-0249


License
-------
pyrqlite is released under the MIT License. See LICENSE for more information.
