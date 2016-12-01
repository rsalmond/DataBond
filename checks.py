from errors import InvalidDB

def check_sqlite_indexes(base):
    indexes = []
    for table_name, table_obj in base.metadata.tables.items():
        for index in table_obj.indexes:
            if index.name not in indexes:
                indexes.append(index.name)
            else:
                raise InvalidDB('Source db contains more than one index named {} and destination db type SQLite does not support that.'.format(index.name))
                
def check_create(base):
    """ test that the reflected tables and indexes can be created in the target database """
    check_sqlite_indexes(base)
