from configparser import ConfigParser
from sqlalchemy import create_engine


def config(filename, section):
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


def connection_geo(filename='database.ini', section='postgresql geo'):
    conn = config(filename, section)
    return create_engine(f'postgresql://{conn["user"]}:{conn["password"]}@{conn["host"]}:{conn["port"]}/{conn["database"]}')

    