from contextlib import contextmanager

import sqlalchemy
from sqlalchemy import create_engine, pool, orm
from sqlalchemy.orm import create_session, scoped_session, sessionmaker

import os
import socket
import warnings
import yaml

from autocnet import config

def new_connection(db_config):
    """
    Using the user supplied config create a NullPool database connection.

    Parameters
    ----------
    db_config : dict
                In the form: {'username':'somename',
                              'password':'somepassword',
                              'host':'somehost',
                              'pgbouncer_port':6543,
                              'name':'somename'}
                If None, the default database from the config will be used.

    Returns
    -------
    Session : object
              An SQLAlchemy session object

    engine : object
             An SQLAlchemy engine object
    """
    if db_config is None:
        if config:
            db_config = config['database']
        else:
            raise Exception("A database must be specified in either the config file or db_config argument.")

    db_uri = 'postgresql://{}:{}@{}:{}/{}'.format(db_config['username'],
                                                  db_config['password'],
                                                  db_config['host'],
                                                  db_config['pgbouncer_port'],
                                                  db_config['name'])
    hostname = socket.gethostname()
    engine = create_engine(db_uri, poolclass=pool.NullPool,
                    connect_args={"application_name":"AutoCNet_{}".format(hostname)},
                    isolation_level="AUTOCOMMIT")
    Session = orm.sessionmaker(bind=engine, autocommit=True)
    return Session, engine
