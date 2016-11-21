import argparse
import sys

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

def create_db_config(database_file):
    with open(dbconfigfile, 'w') as f:
        f.write('from sqlalchemy import * \n')
        f.write('from sqlalchemy.orm import create_session\n')
        f.write('from sqlalchemy.ext.declarative import declarative_base\n')
        f.write('Base = declarative_base()\n')
        f.write('source_engine = create_engine(\'sqlite:///{}\')\n'.format(database_file))
        f.write('source_metadata = MetaData(bind=source_engine)\n')
        f.write('source_metadata.reflect(source_engine)\n')
        f.write('source_session = create_session(bind=source_engine)\n')

def connect_sqlite(database_file):
    Base = declarative_base()
    engine = create_engine('sqlite:///{}'.format(database_file))
    metadata = MetaData(bind=engine)
    metadata.reflect(engine)
    session = create_session(bind=engine)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="SQLite database exporter.")
    parser.add_argument('sqlite_file', action='store')
    parser.add_argument('--host', action='store', default='localhost', help='MySQL Host')
    parser.add_argument('--user', action='store', default='root', help='MySQL User')
    parser.add_argument('--pass', action='store', required=True, help='MySQL Pass')
    args = parser.parse_args()

    # try to import a db config, if one doesn't exist yet create it
    try:
        from db import source_metadata
    except ImportError:
        create_db_config(args.sqlite_file)
        print('Created database config for {}, run me again.'.format(args.sqlite_file))
        sys.exit(0)

    classes = create_orm_classes()

    from classes import *

    import pdb
    pdb.set_trace()
