-------------
pyrqlite
-------------

.. image:: https://circleci.com/gh/rqlite/pyrqlite.svg?style=svg
    :target: https://circleci.com/gh/rqlite/pyrqlite

This package contains a pure-Python rqlite client library.

.. contents::

Requirements
-------------

* Python -- CPython_ >= 3.3

* rqlite Server


Installation
------------

The last stable release is available on github and can be installed with ``pip``::

    $ pip install git+https://github.com/rqlite/pyrqlite.git

You can also just clone the repo and install it from source::

    $ git clone https://github.com/rqlite/pyrqlite.git
    $ cd pyrqlite
    $ pip install .

Finally, a tarball of a tagged release can be downloaded from GitHub and
installed the same way::

    $ # X.Y.Z is the desired pyrqlite version (e.g. 2.2.3).
    $ curl -L https://github.com/rqlite/pyrqlite/archive/refs/tags/vX.Y.Z.tar.gz | tar xz
    $ cd pyrqlite*
    $ pip install .
    $ # The folder pyrqlite* can be safely removed now.

You may need to run the installation process with ``root`` privileges.

Test Suite
----------

The test suite starts its own ``rqlited`` process, so a copy of the rqlite
binary is required. Point ``RQLITED_PATH`` at it, then run pytest
(https://pytest.org/) from the repository root::

    $ pip install pytest
    $ RQLITED_PATH=/path/to/rqlited PYTHONPATH=src pytest src/test

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

This example will print::

    (1, 'a')
    (2, 'b')

Paramstyle
-------------

Only qmark and named paramstyles (as defined in PEP 249) are supported. 

Limitations
-------------
Transactions are not supported.

Resources
-------------
rqlite: https://rqlite.io/
DB-API 2.0: http://www.python.org/dev/peps/pep-0249


License
-------------
pyrqlite is released under the MIT License. See LICENSE for more information.

.. _CPython: https://www.python.org/
