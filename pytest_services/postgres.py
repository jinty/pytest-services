import os
import hashlib
import shutil
from subprocess import PIPE, Popen, check_call
from distutils.spawn import find_executable

import pytest
import psycopg2

from .process import (
    CalledProcessWithOutputError,
    check_output,
)

def postgres_data_dir(request, memory_base_dir, services_log):
    """The root directory for the PostgreSQL instance.

    `initdb` is run in that directory.

    """
    path = os.path.join(memory_base_dir, 'postgres')
    services_log.debug('Making postgres base dir in {path}'.format(path=path))

    def finalizer():
        shutil.rmtree(path, ignore_errors=True)

    finalizer()
    request.addfinalizer(finalizer)
    os.mkdir(path)
    return path

def postgres_initdb(postgres_data_dir, services_log):
    """Install database to given path."""
    new_env = os.environ.copy()

    initdb = find_executable('initdb')
    assert initdb, "Can't find PostgreSQL initdb"

    new_env = os.environ.copy()
    new_env['PGOPTIONS'] = '-F'

    services_log.debug('Creating PostgreSQL cluster with initdb.')
    try:
        check_output([
            initdb,
            '-E', 'utf-8',
            '-D', postgres_data_dir,
            '-A', 'trust'],
            env=new_env
        )
    except CalledProcessWithOutputError as e:
        services_log.error(
            '{e.cmd} failed with output:\n{e.output}\nand erorr:\n{e.err}. '
            'Please ensure you disabled apparmor for /run/shm/** or for PostgreSQL'.format(e=e))
        raise
    finally:
        services_log.debug('PostgreSQL cluster was initialized.')

def _checker(postgres_data_dir, database='postgres'):
    def checker():
        # mysql just waits for the socket to appear, maybe we can do the same for
        # postgres.  But I think postgres starts listening before it's fully started.
        args = ['psql', '-h', postgres_data_dir, '-c', "SELECT 'YAY';", '-t', '-A', database]
        p = Popen(args, stdout=PIPE, stderr=PIPE)
        result, psql_err = p.communicate()
        if p.returncode == 0 and b'YAY' in result:
            return True
        return False
    return checker

def start_postgres_service(request, watcher_getter, postgres_data_dir):
    watcher_getter('postgres',
            arguments=['-D', postgres_data_dir, '-k', postgres_data_dir, '-F', '-h', '', '--log_min_messages=PANIC'],
            checker=_checker(postgres_data_dir))

@pytest.fixture(scope='session')
def postgres_host(request, memory_base_dir, watcher_getter, services_log):
    pghost = os.environ.get('PGHOST', None) 
    if pghost:
        return pghost
    else:
        data_dir = postgres_data_dir(request, memory_base_dir, services_log)
        postgres_initdb(data_dir, services_log)
        start_postgres_service(request, watcher_getter, data_dir)
        return data_dir

@pytest.fixture(scope='session')
def postgres_database_setup_sql(request, memory_temp_dir):
    path = os.path.join(mysql_data_dir, 'setup.sql')
    with open(defaults_path, 'w+') as fd:
        fd.write("CREATE TABLE example (x int)")
    return path

def get_database_name(postgres_database_setup_sql):
    with open(postgres_database_setup_sql, 'rb') as f:
        data = f.read()
    m = hashlib.md5(data)
    return 'pytest-services-template-{}'.format(m.hexdigest()), teardown

def database_exists(host, database):
    # check if database exists in the WORST way possible ;)
    checker = _checker(host, database=database)
    return checker()

@pytest.fixture(scope='session')
def postgres_template_database(request, postgres_host, postgres_database_setup_sql):
    """Return a setup function which sets up a template database"""
    dbname = get_database_name(postgres_database_setup_sql)
    if not database_exists(postgres_host, dbname):
        tmp_dbname = dbname + '-tmp'
        try:
            check_call(['createdb', tmp_dbname])
        except:
            check_call(['dropdb', tmp_dbname])
            check_call(['createdb', tmp_dbname])
        # TODO: check that psql REALLY DOES return nonzero returncode if there is an error
        check_call(['psql', '-1', '-h', host, '-f', postgres_database_setup_sql, tmp_dbname])
        check_call(['psql', '-h', host, '-c', 'ALTER DATABASE RENAME "{}" TO "{}"'.format(tmp_dbname, dbname), 'postgres'])
    return dbname

def postgres_database(postgres_host, postgres_template_database):
    dbname = postgres_template_database + '-running'
    # TODO: idea here is to create a database for a test, and ONLY clean it up if a transaction was committed during the test
    # else, rollback is done
    #
    # I am pretty sure 

