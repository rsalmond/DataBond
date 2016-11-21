import argparse
import sys
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative.api import DeclarativeMeta

#Create and engine and get the metadata

classfile = 'classes.py'
dbconfigfile = 'db.py'

def create_orm_classes():
    """ create a python file with sqlalchemy class for each table in the db """
    classes = []
    with open(classfile, 'w') as f:
        f.write('from db import Base, source_metadata\n')
        f.write('from sqlalchemy import Table\n')
        for table in source_metadata.tables.keys():
            if table == 'sqlite_sequence':
                continue
            classname = table.lower().capitalize()
            classes.append(classname)
            f.write('class {}(Base):\n'.format(classname))
            f.write('\t__table__ = Table(\'{}\', source_metadata, autoload=True)\n'.format(table))

    return classes

def create_db_config(args):
    db_name = args.sqlite_file.split('.')[0]
    with open(dbconfigfile, 'w') as f:
        f.write('from sqlalchemy import * \n')
        f.write('from sqlalchemy.orm import create_session\n')
        f.write('from sqlalchemy.ext.declarative import declarative_base\n')
        f.write('Base = declarative_base()\n')
        f.write('source_engine = create_engine(\'sqlite:///{}\')\n'.format(args.sqlite_file))
        f.write('source_metadata = MetaData(bind=source_engine)\n')
        f.write('source_metadata.reflect(source_engine)\n')
        f.write('source_session = create_session(bind=source_engine)\n')
        f.write('target_engine = create_engine(\'mysql+pymysql://{}:{}@{}:{}/{}\')\n'.format(args.user, args.password, args.host, args.port, db_name))
        f.write('target_metadata = MetaData(bind=target_engine)\n')
        f.write('target_session = create_session(bind=target_engine)\n')

def connect_sqlite(database_file):
    Base = declarative_base()
    engine = create_engine('sqlite:///{}'.format(database_file))
    metadata = MetaData(bind=engine)
    metadata.reflect(engine)
    session = create_session(bind=engine)

def create_mysql_db(args):
    """ create a mysql database using the sqlite filename """
    target_engine = create_engine('mysql+pymysql://{}:{}@{}:{}'.format(args.user, args.password, args.host, args.port))
    conn = target_engine.connect()
    if '.' in args.sqlite_file:
        db_name = args.sqlite_file.split('.')[0]

    result = conn.execute('create database {}'.format(db_name))
    conn.close()

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="SQLite database exporter.")
    parser.add_argument('sqlite_file', action='store')
    parser.add_argument('--host', action='store', default='localhost', help='MySQL Host')
    parser.add_argument('--user', action='store', default='root', help='MySQL User')
    parser.add_argument('--password', action='store', default='', help='MySQL Password')
    parser.add_argument('--port', action='store', default='3306', help='MySQL Port')
    args = parser.parse_args()

    # try to import a db config, if one doesn't exist yet create it
    try:
        from db import *
    except ImportError:
        create_db_config(args)
        print('Created database config for {}, run me again.'.format(args.sqlite_file))
        sys.exit(0)


    print('Creating target database.')
    create_mysql_db(args)

    classes = create_orm_classes()

    from classes import *

    # some sqlite thing we dont need
    source_metadata.remove(source_metadata.tables.get('sqlite_sequence'))

    # create tables in the target db
    print('Creating tables in target database.')
    source_metadata.create_all(bind=target_engine)

    for var in dict(locals()):
        # XXX: I *think* this is safe ...
        if locals().get(var).__class__ is DeclarativeMeta:
            baseclass = locals().get(var)
            print('Importing data for {}.'.format(baseclass))

            import pdb
            pdb.set_trace()

            for row in source_session.query(baseclass).all():
                target_session.add(row)
                db.session.commit()

