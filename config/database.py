from configparser import ConfigParser
import psycopg2

#database configuration
def config(filename='database.ini', section='postgresql'):
    """
    credits to: https://www.postgresqltutorial.com/postgresql-python/connect/
    """
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db

def connect():
    """credits to: https://www.postgresqltutorial.com/postgresql-python/connect/"""
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()
        print(params)

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        return conn

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
