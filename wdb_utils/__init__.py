import oracledb
import pandas as pd
import yaml
import getpass
import os
from pathlib import Path


default_config = os.path.join(Path.home(), 'configs', 'config-default.yml')
oracledb.defaults.fetch_lobs = False


def __read_config_file(config_file=default_config, dbtype='oracle'):
    if dbtype != 'oracle':
        raise(Exception('Database type not supported'))
    with open(config_file, 'r') as ymlfile:
        cfg = yaml.load(ymlfile, yaml.FullLoader)

    user = cfg['database']['username']
    host = cfg['database']['host']
    if 'password' in cfg['database']:
        pwd = cfg['database']['password']
    else:
        pwd = getpass.getpass('Database password: ')
    return user, pwd, host


def run_query(query, config_file=default_config, arraysize=120000, fetch_ct=1000000):
    user, pwd, host = __read_config_file(config_file)
    results = []
    with oracledb.connect(user=user, password=pwd, dsn=host) as connection:
        cursor = connection.cursor()
        cursor.arraysize = arraysize
        cursor.execute(query)
        while True:
            rows = cursor.fetchmany(fetch_ct)
            if not rows:
                break
            else:
                results = results + rows
        cols = [n[0] for n in cursor.description]
    return pd.DataFrame(results, columns=cols)


def run_command(c, config_file=default_config):
    user, pwd, host = __read_config_file(config_file)

    connection = oracledb.connect(user=user, password=pwd, dsn=host)
    cursor = connection.cursor()
    try:
        cursor.execute(c)
    except Exception as e:
        print(e)

    connection.commit()
    connection.close()


def load_data(data, tablename, config_file=default_config):
    user, pwd, host = __read_config_file(config_file)

    connection = oracledb.connect(user=user,
                                  password=pwd,
                                  dsn=host)
    cursor = connection.cursor()
    inserted_data = [
        [None if pd.isnull(value) else value for value in sublist]
        for sublist in data.values.tolist()]
    sql = '''
     insert into {} ({}
     ) values ({})
  '''.format(tablename,
             ', '.join(["{}".format(colname) for colname in data.columns]),
             ', '.join([":{}".format(x) for x in range(1, len(data.columns)+1)])
             )
    try:
        cursor.executemany(sql, inserted_data)
        connection.commit()
        connection.close()

    except oracledb.DatabaseError as e:
        print(e)
        connection.rollback()
        connection.close()
        raise e
