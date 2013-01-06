"""
PostgreSQL users and databases
==============================

This module provides tools for creating PostgreSQL users and databases.

"""
from __future__ import with_statement
from uuid import uuid4

from fabric.api import *  # NOQA
from fabric.operations import get, put
from fabric.contrib import files


def _run_as_pg(command):
    """
    Run command as 'postgres' user
    """
    with cd('/var/lib/postgresql'):
        return sudo('sudo -u postgres %s' % command)


def user_exists(name):
    """
    Check if a PostgreSQL user exists.
    """
    with settings(hide('running', 'stdout', 'stderr', 'warnings'), warn_only=True):
        res = _run_as_pg('''psql -t -A -c "SELECT COUNT(*) FROM pg_user WHERE usename = '%(name)s';"''' % locals())
    return (res == "1")


def create_user(name, password):
    """
    Create a PostgreSQL user.

    Example::

        import fabtools

        # Create DB user if it does not exist
        if not fabtools.postgres.user_exists('dbuser'):
            fabtools.postgres.create_user('dbuser', password='somerandomstring')

    """
    _run_as_pg('''psql -c "CREATE USER %(name)s WITH PASSWORD '%(password)s';"''' % locals())


def database_exists(name):
    """
    Check if a PostgreSQL database exists.
    """
    with settings(hide('running', 'stdout', 'stderr', 'warnings'), warn_only=True):
        return _run_as_pg('''psql -d %(name)s -c ""''' % locals()).succeeded


def create_database(name, owner, template='template0', encoding='UTF8', locale='en_US.UTF-8'):
    """
    Create a PostgreSQL database.

    Example::

        import fabtools

        # Create DB if it does not exist
        if not fabtools.postgres.database_exists('myapp'):
            fabtools.postgres.create_database('myapp', owner='dbuser')

    """
    _run_as_pg('''createdb --owner %(owner)s --template %(template)s --encoding=%(encoding)s\
 --lc-ctype=%(locale)s --lc-collate=%(locale)s %(name)s''' % locals())


def dump_database(name, owner, output='dump.sql'):
    tmpname = uuid4()
    run('''pg_dump -w -h localhost -U %(owner)s --format plain --no-owner --no-acl --file "/tmp/%(tmpname)s" %(name)s''' % locals())
    get('/tmp/%s' % tmpname, output)


def import_database(source='dump.sql'):
    tmpname = uuid4()
    put(source, '/tmp/%s' % tmpname)


def set_passwordfile(username, password, database, host='localhost', port='5432'):
    files.append(
        '/home/`whoami`/.pgpass',
        '%(host)s:%(port)s:%(database)s:%(username)s:%(password)s' % locals()
    )
    run('chmod 0600 /home/`whoami`/.pgpass')
