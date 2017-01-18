import os
try:
    import configparser
except ImportError:
    import ConfigParser as configparser


BASE_DIR = os.path.abspath(os.path.dirname(__file__))

cf = configparser.ConfigParser()
cf.read(os.path.join(
    BASE_DIR, 'static', 'config.ini'
))

NC_FILE = os.path.join(BASE_DIR, 'data', 'netcdf', _filename)

MONGO = dict(
    local=True,
    user=cf.get('user', 'username'),
    password=cf.get('user', 'password'),
    domain=cf.get('server', 'domain'),
    database=cf.get('server', 'database'),
    port=int(cf.get('server', 'port')),
)

ELASTICSEARCH = dict(
    meta_index=cf.get('elasticsearch', 'meta_index'),
    meta_type=cf.get('elasticsearch', 'meta_type'),
)
