import sys
import argparse
import tqdm
import logging
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base

VERIFICATION_SUCCESS = 1
VERIFICATION_FATAL = 2
VERIFICATION_DIFF = 3

def create_dest_db(engine, name):
    """ create a new db in dest db and returns a new connection to that db"""

    # don't need to bother with sqlite
    if engine.url.drivername != 'sqlite':
        log.info(u'Creating destination database.')
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

    applevel = logging.INFO
    sqllevel = logging.WARNING

    if args.verbose is not None:
        if args.verbose == 1:
            applevel = logging.DEBUG
        else:
            applevel = logging.DEBUG
            sqllevel = logging.DEBUG

    # set sqlalchemy log level
    logging.basicConfig()
    logging.getLogger('sqlalchemy.engine').setLevel(sqllevel)

    # create our own logging object and set its level
    logger = logging.getLogger(__name__)
    logger.setLevel(applevel)
    return logger

def copy(source, dest, base):
    """ copy rows from source db to dest db """

    sorted_mappers = sort_mappers(base.classes.items())

    for mapper_name in sorted_mappers:
        mapper_obj = base.classes.get(mapper_name)
        log.info(u'Importing table `{}`.'.format(mapper_name))
        to_import = source.query(mapper_obj).all()
        imported_count = 0
        for row in to_import:
            imported_count += 1
            log.debug(u'Importing row id: {}'.format(row.id))
            dest.merge(row)
            dest.commit()

def verify(source_session, dest_session, dest_engine, source_base):
    """ ensure everything in the destination database matches whats in the source database """
    destbase = automap_base()
    destbase.prepare(dest_engine, reflect=True)

    retval = VERIFICATION_SUCCESS

    def log_diff(source_row, dest_row, column):
        """ log the differences detected between source and dest rows """
        table = source_row.__table__.name
        source_val = getattr(source_row, column)
        dest_val = getattr(dest_row, column)

        if hasattr(source_row, 'id'):
            source_id = getattr(source_row, 'id')
            dest_id = getattr(dest_row, 'id')
            log.error(u'SOURCE Table: {table} Row ID: {id}, Column: {col}, Value: {value}'.format(table=table, id=source_id, col=column, value=source_val))
            log.error(u'DEST   Table: {table} Row ID: {id}, Column: {col}, Value: {value}'.format(table=table, id=dest_id, col=column, value=dest_val))
        else:
            log.error(u'SOURCE Table: {table} Column: {col}, Value: {value}'.format(table=table, col=column, value=source_val))
            log.error(u'DEST   Table: {table} Column: {col}, Value: {value}'.format(table=table, col=column, value=dest_val))


    # verify dest database has same tables / columns present in source database, this test returns immediately on failure
    # because a missing table / column means we seriously screwed up.
    for table, table_obj in source_base.metadata.tables.items():
        log.debug(u'Validating table `{}`.'.format(table))
        for column, column_obj in table_obj._columns.items():
            log.debug(u'Validating column `{}` on table `{}`.'.format(column, table))
            desttable = destbase.metadata.tables.get(table)
            if desttable is None:
                log.error(u'Verification FAILED: destination table `{}` missing.'.format(table))
                return VERIFICATION_FATAL
            else:
                destcolumn = desttable._columns.get(column)
                if destcolumn is None:
                    log.error(u'Verification FAILD: destination column `{}` is missing from table `{}`.'.format(column, table))
                    return VERIFICATION_FATAL

    # verify data in source tables matches data in dest tables, this test logs differences and continues because data 
    # differences may have to do with db specifics (eg. unicode handling).
    for source_mapper, source_mapper_obj in source_base.classes.items():
        if source_mapper_obj.__table__.primary_key is None:
            log.warn(u'Cannot verify the contents of `{}` as it has no primary key.')
            continue
        else:
            log.info(u'Verifying the contents of table `{}`.'.format(source_mapper))

            source_rows = source_session.query(source_mapper_obj).all()
            verified_rows = 0
            for source_row in source_rows:
                mismatch = False
                dest_mapper = destbase.classes.get(source_mapper)
                dest_query = dest_session.query(source_mapper_obj)
                # iterate over the primary keys for this table
                # XXX: test this on tables with compound keys
                for column, column_obj in source_mapper_obj.__table__.primary_key.columns.items():
                    # create a query against the destination table for each primary key value set on the source row
                    dest_query = dest_query.filter(getattr(source_mapper_obj, column) == getattr(source_row, column))

                # XXX: pretty sure this should be the case
                dest_row = dest_query.one()

                for column in source_row.__mapper__.columns.keys():
                    if getattr(dest_row, column) != getattr(source_row, column):
                        log.error(u'Verification FAILED: source row / dest row mismatch.')
                        log_diff(source_row, dest_row, column)
                        mismatch = True

                if not mismatch:
                    verified_rows += 1
                else:
                    retval = VERIFICATION_DIFF

            log.info(u'{} rows out of {} verified identical in source and destination table {}'.format(verified_rows, len(source_rows), source_mapper))

    return retval

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='SQL database pipe.')
    parser.add_argument('-s', '--sourcedb', action='store', required=True)
    parser.add_argument('-d', '--destdb', action='store', required=True)
    parser.add_argument('--skip-dest-create', action='store_true', help='Do not automatically create a database in the destination DB.')
    parser.add_argument('-e', '--encoding', action='store', help='Specify a character encoding for dest database, eg. utf8, latin1, etc.')
    parser.add_argument('-v', '--verbose', action='count', help='Increase verbosity, use twice for debug output.')
    parser.add_argument('-c', '--check-only', action='store_true', help='Do not copy anything, only perform checking that source and dest db are identical.')
    args = parser.parse_args()

    global log
    log = set_loglevel(args)
    log.info(u'Operating at log level: {}'.format(log.getEffectiveLevel()))

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
        if not (args.skip_dest_create or args.check_only):
            log.error(
                u'You have specified a destination DB connections string which includes a database ' \
                'name. If you have already created the destination database use --skip-dest-create, ' \
                'otherwise do not specify a database name in your connection string and it ' \
                'will be created.')
            sys.exit(1)
    else:
        if not args.check_only:
            # determine the db name of the source db and create it in the destination db
            dbname = get_db_name(source_engine)
            dest_engine = create_dest_db(dest_engine, dbname)
        else:
            log.error(
                u'You have specified --check-only option but the --destdb connection string does not ' \
                'include a database name. Please specify a destination database name and try again.'
            )
            sys.exit(1)

    # get rid of this sqlite specific thing we wont need to export
    if 'sqlite_sequence' in Base.metadata.tables:
        Base.metadata.remove(Base.metadata.tables.get('sqlite_sequence'))

    source_session = Session(source_engine)
    dest_session = Session(dest_engine)

    # copy ze data!
    if not args.check_only:
        log.info(u'Creating tables in dest database.')
        Base.metadata.create_all(bind=dest_engine)
        copy(source_session, dest_session, Base)

    verification = verify(source_session, dest_session, dest_engine, Base)
    
    completion_messages = {
        VERIFICATION_SUCCESS: u'Verification successful, every table, column, and row present in the ' \
                'source db has been verified present in the destination db.',
        VERIFICATION_FATAL: u'Verification fatal, a table or column was not created in the destination ' \
                'db, probably a SQL error has been produced as well. If the error cannot be corrected ' \
                'please file a bug at https://github.com/rsalmond/databond and include this log.',
        VERIFICATION_DIFF: u'Verification found differences between the data in the source and ' \
                'destination databases. Differences are documented in the log above, where present ' \
                'an ID column is also logged. These differences may be expected eg. due to data type ' \
                'differences between source and destination dbs or encoding differences. Please ' \
                'closely examine these differences as adjustments may be necessary.'
    }

    log.info(completion_messages[verification])
