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

Often they are released on PyPI too, able to be installed via the package named ``rqlite``::

    $ pip install rqlite

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
            # Read a single record
            sql = "SELECT `id`, `name` FROM `foo` WHERE `name`=?"
            cursor.execute(sql, ('a',))
            result = cursor.fetchone()
            print(result)
    finally:
        connection.close()

This example will print:

.. code:: python

    OrderedDict([('id', 1), ('name', 'a')])


Resources
---------
DB-API 2.0: http://www.python.org/dev/peps/pep-0249


License
-------
pyrqlite is released under the MIT License. See LICENSE for more information.
