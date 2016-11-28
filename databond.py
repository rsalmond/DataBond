import sys
import argparse
import tqdm
import logging
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base

def create_dest_db(engine, name):
    """ create a new db in dest db and returns a new connection to that db"""

    # don't need to bother with sqlite
    if engine.url.drivername != 'sqlite':
        print('Creating destination database.')
        conn = engine.connect()
        result = conn.execute('create database {}'.format(name))
        conn.close()

        # reconnect to the destination database addressing the newly created database
        del engine
        engine = create_engine('{}/{}'.format(args.destdb, dbname), connect_args=connect_args)

    return engine

def get_db_name(engine):
    if engine.name == 'sqlite':
        return engine.url.database.rsplit('.')[0]
    else:
        return engine.url.database

def sort_mappers(classes):
    """ try to put mapper objects into an insert order which will allow foreign key constraints to be satisfied """
    order = []

    #iterate over a copy of the list
    classlist = dict(classes)

    # not sure what it is but it mucks things up
    if '_sa_module_registry' in classlist:
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
                order.append(name)
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

def set_loglevel(args):
    # note: verbosity == 1 (databond -v) just prints the row ID for every insert)
    levels = {
        2: logging.WARNING,
        3: logging.INFO,
        4: logging.DEBUG
    }
    # when you reach max level you stop leveling
    if args.verbose > 4:
        level = 4
    else:
        level = args.verbose

    if args.verbose >= 2:
        logging.basicConfig()
        logging.getLogger('sqlalchemy.engine').setLevel(levels[level])


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='SQL database pipe.')
    parser.add_argument('-s', '--sourcedb', action='store', required=True)
    parser.add_argument('-d', '--destdb', action='store', required=True)
    parser.add_argument('--skip-dest-create', action='store_true', help='Do not automatically create a database in the destination DB.')
    parser.add_argument('-e', '--encoding', action='store', help='Specify a character encoding for dest database, eg. utf8, latin1, etc.')
    parser.add_argument('-v', '--verbose', action='count', help='Increase verbosity, use more to increase SQLAlchemy log level (up to four == DEBUG).')
    args = parser.parse_args()
    xargs = args
    print('Running at log level {}').format(args.verbose)
    set_loglevel(args)

    if args.encoding is not None:
        connect_args = {'charset': args.encoding}
    else:
        connect_args = {}

    # connect to databases and reflect source tables 
    source_engine = create_engine(args.sourcedb)
    dest_engine = create_engine(args.destdb, connect_args=connect_args)
    Base = automap_base()
    Base.prepare(source_engine, reflect=True)

    if dest_engine.url.database is not None:
        if not args.skip_dest_create:
            print(
                'You have specified a destination DB connections string which includes a database ' \
                'name. If you have already created the destination database use --skip-dest-create, ' \
                'otherwise do not specify a database name in your connection string and it ' \
                'will be created.')
            sys.exit(1)
    else:
        # determine the db name of the source db and create it in the destination db
        dbname = get_db_name(source_engine)
        dest_engine = create_dest_db(dest_engine, dbname)

    # get rid of this sqlite specific thing we wont need to export
    if 'sqlite_sequence' in Base.metadata.tables:
        Base.metadata.remove(Base.metadata.tables.get('sqlite_sequence'))

    source_session = Session(source_engine)
    dest_session = Session(dest_engine)

    print('Creating tables in dest database.')
    Base.metadata.create_all(bind=dest_engine)

    sorted_mappers = sort_mappers(Base.classes.items())


    for mappername in sorted_mappers:
        mapperobj = Base.classes.get(mappername)
        print('Importing table {}.'.format(mappername))
        to_import = source_session.query(mapperobj).all()
        for row in to_import:
            # print a new line for verbose mode
            if args.verbose >= 1:
                print('\rImporting row id {}'.format(row.id))
            else:
                print('\rImporting row id {}'.format(row.id)),
            dest_session.merge(row)
            dest_session.commit()
        if len(to_import) > 0:
            print('')

