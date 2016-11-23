import argparse
import sys
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative.api import DeclarativeMeta

classfile = 'classes.py'
dbconfigfile = 'db.py'

def create_orm_classes():
    """ create a python file with sqlalchemy class for each table in the db """
    with open(classfile, 'w') as f:
        f.write('from db import Base, source_metadata\n')
        f.write('from sqlalchemy import Table\n')
        for table in source_metadata.tables.keys():
            if table == 'sqlite_sequence':
                continue
            classname = table.lower().capitalize()
            f.write('class {}(Base):\n'.format(classname))
            f.write('\t__table__ = Table(\'{}\', source_metadata, autoload=True)\n'.format(table))

def create_db_config(args):
    """ create a python file with db config and sqlalchemy init """
    db_name = args.sqlite_file.split('.')[0]
    with open(dbconfigfile, 'w') as f:
        f.write('from sqlalchemy import * \n')
        f.write('from sqlalchemy.orm import create_session\n')
        f.write('from sqlalchemy.ext.declarative import declarative_base\n')
        f.write('classreg = {}\n')
        f.write('Base = declarative_base(class_registry=classreg)\n')
        f.write('source_engine = create_engine(\'sqlite:///{}\')\n'.format(args.sqlite_file))
        f.write('source_metadata = MetaData(bind=source_engine)\n')
        f.write('source_metadata.reflect(source_engine)\n')
        f.write('source_session = create_session(bind=source_engine)\n')
        f.write('target_engine = create_engine(\'mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8\', encoding=\'utf-8\')\n'.format(args.user, args.password, args.host, args.port, db_name))
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


def sort_mappers(classes):
    """ try to put mapper objects / tables into an insert order which will allow foreign key constraints to be satisfied """
    order = []

    #iterate over a copy of the list
    classlist = dict(classes)

    #not sure what this is, probably dont need it?
    del classlist['_sa_module_registry']

    # because tables with foreign key constraints may come up in the list prior to the tables they rely on
    # we may skip some tables one (or more) times and need to re-iterate the list until we have them all
    #
    #XXX: circular dependencies could put this into an endless loop?!
    #
    while True:
        for name, cls in classlist.items():
            # tables with no foreign key constraints can be inserted into the new db at any time
            if len(cls.__table__.foreign_key_constraints) == 0:
                #lower case it for later lookup by table name
                order.append(name.lower())
                del classlist[name]
            else:
                foreign_tables = [fkc.referred_table.name for fkc in cls.__table__.foreign_key_constraints]
                # if all tables with foreign keys are ahead of this one its safe to add it to the queue
                if set(foreign_tables).issubset(set(order)):
                    order.append(name.lower())
                    del classlist[name]

        if len(classlist.items()) == 0:
            break

    return order

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

    create_orm_classes()

    from classes import *

    # some sqlite thing we dont need
    source_metadata.remove(source_metadata.tables.get('sqlite_sequence'))

    # create tables in the target db
    print('Creating tables in target database.')
    source_metadata.create_all(bind=target_engine)

    sorted_mappers = sort_mappers(classreg)

    for mappername in sorted_mappers:
        mapperobj = classreg[mappername.capitalize()]
        print('Importing data for table {}.'.format(mappername))
        for row in source_session.query(mapperobj).all():
            print('Inserting row {}'.format(row.id))
            #source_session.expunge(row)
            target_session.merge(row)
            target_session.flush()

