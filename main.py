import asyncio
import aiohttp
import csv
import logging
import os
import sqlalchemy

from aiopg.sa import create_engine
from contextlib import closing
from flask import Flask, request
from flask_sqlalchemy import SQLAlchemy
from io import TextIOWrapper
from itertools import chain
from zipfile import ZipFile


app = Flask(__name__)

#DSN = 'postgresql://postgres:qwertypark@localhost:5433/addr_db'
DSN = 'mysql+pymysql://postgres:qwertypark@/addr_db?unix_socket=/cloudsql/ukrpost2-224713:europe-west1:addresses'

CSV_FILENAME = 'houses.csv'
CHUNK_SIZE = 1024 * 100
ZIP_FILENAME = '/tmp/houses.zip'
REMOTE_ZIP_URL = 'http://services.ukrposhta.com/postindex_new/upload/houses.zip'


# db = sqlalchemy.create_engine(
#     # Equivalent URL:
#     # mysql+pymysql://<db_user>:<db_pass>@/<db_name>?unix_socket=/cloudsql/<cloud_sql_instance_name>
#     # 'postgresql://postgres:qwertypark@localhost:5433/addr_db'
#     sqlalchemy.engine.url.URL(
#         drivername='mysql+pymysql',
#         username='postgres',
#         password='qwertypark',
#         database='addr_db',
#         query={
#             'unix_socket': '/cloudsql/{}'.format('ukrpost2-224713:europe-west1:addresses')
#         }
#     ),
# )

db = sqlalchemy.create_engine(
    # Equivalent URL:
    # postgres+pg8000://<db_user>:<db_pass>@/<db_name>?unix_socket=/cloudsql/<cloud_sql_instance_name>
    sqlalchemy.engine.url.URL(
        drivername='postgres+pg8000',
        username='postgres',
        password='qwertypark',
        database='addr_db',
        query={
            'unix_sock': '/cloudsql/{}'.format('ukrpost2-224713:europe-west1:addresses')
        }
    ),
    # ... Specify additional properties here.
    # [START_EXCLUDE]

    # [START cloud_sql_postgres_limit_connections]
    # Pool size is the maximum number of permanent connections to keep.
    pool_size=5,
    # Temporarily exceeds the set pool_size if no connections are available.
    max_overflow=2,
    # The total number of concurrent connections for your application will be
    # a total of pool_size and max_overflow.
    # [END cloud_sql_postgres_limit_connections]

    # [START cloud_sql_postgres_connection_backoff]
    # SQLAlchemy automatically uses delays between failed connection attempts,
    # but provides no arguments for configuration.
    # [END cloud_sql_postgres_connection_backoff]

    # [START cloud_sql_postgres_connection_timeout]
    # 'pool_timeout' is the maximum number of seconds to wait when retrieving a
    # new connection from the pool. After the specified amount of time, an
    # exception will be thrown.
    pool_timeout=30,  # 30 seconds
    # [END cloud_sql_postgres_connection_timeout]

    # [START cloud_sql_postgres_connection_lifetime]
    # 'pool_recycle' is the maximum number of seconds a connection can persist.
    # Connections that live longer than the specified amount of time will be
    # reestablished
    pool_recycle=1800,  # 30 minutes
    # [END cloud_sql_postgres_connection_lifetime]

    # [END_EXCLUDE]
)
# [END cloud_sql_postgres_connection_pool]

async def make_request(dsn, sql):
    async with create_engine(dsn) as engine:
        async with engine.acquire() as conn:
            result = await conn.execute(sql)
            for r in result:
                print(r)


async def create_tables(conn):
    sql = """
        CREATE TABLE IF NOT EXISTS region (
            id SERIAL PRIMARY KEY,
            name character varying(255) UNIQUE
            );
        CREATE TABLE IF NOT EXISTS district (
            id SERIAL PRIMARY KEY,
            region_id integer NOT NULL references region (id),
            name character varying(255),
            constraint district_constraint unique (region_id, name)
            );
        CREATE TABLE IF NOT EXISTS city (
            id SERIAL PRIMARY KEY,
            district_id integer NOT NULL references district (id),
            name character varying(255),
            constraint city_constraint unique (district_id, name)
            );
        CREATE TABLE IF NOT EXISTS street (
            id SERIAL PRIMARY KEY,
            city_id integer NOT NULL references city (id),
            name character varying(255),
            constraint street_constraint unique (city_id, name)
            );
        CREATE TABLE IF NOT EXISTS house (
            id SERIAL PRIMARY KEY,
            street_id integer NOT NULL references street (id),
            number character varying(255),
            zip_code integer,
            constraint house_constraint unique (number, street_id, zip_code)
            );
        """
    await conn.execute(sql)


def grouped(iterator, batch_size):
    batch = []
    try:
        while True:
            batch = []
            for i in range(batch_size):
                batch.append(next(iterator))
            yield batch
    except StopIteration:
        yield batch


async def download_zip(url, zip_filename, loop, batch_size=1024*100):
    async with aiohttp.ClientSession(loop=loop) as session:
        async with session.get(url) as response:
            with open(zip_filename, 'wb') as f:
                while True:
                    chunk = await response.content.read(batch_size)
                    if not chunk:
                        break
                    f.write(chunk)


@app.route('/')
def index():
    """Return a friendly HTTP greeting."""
    a = 9
    return f'Hello World!, a={a}'


@app.route('/upload')
def upload():
    asyncio.set_event_loop(asyncio.new_event_loop())
    with closing(asyncio.get_event_loop()) as loop:
        # sql = '''SELECT * FROM region'''
        # loop.run_until_complete(make_request(DSN, sql))

        print('download zip')
        loop.run_until_complete(download_zip(REMOTE_ZIP_URL, ZIP_FILENAME, loop))
        # print('update tb')
        # loop.run_until_complete(update_db(DSN, ZIP_FILENAME, CSV_FILENAME))
    return 'Done!', 200, {'Content-Type': 'text/plain; charset=utf-8'}


@app.route('/update')
def update():
    asyncio.set_event_loop(asyncio.new_event_loop())
    with closing(asyncio.get_event_loop()) as loop:
        print('update tb')
        loop.run_until_complete(make_request(DSN, 'SELECT * FROM region'))
    return 'Done!', 200, {'Content-Type': 'text/plain; charset=utf-8'}

@app.route('/update2')
def update2():
    with db.connect() as conn:
        conn.execute(
            "SELECT * FROM region"
        )

@app.errorhandler(500)
def server_error(e):
    logging.exception('An error occurred during a request.')
    return """
    An internal error occurred: <pre>{}</pre>
    See logs for full stacktrace.
    """.format(e), 500


if __name__ == '__main__':
    # This is used when running locally. Gunicorn is used to run the
    # application on Google App Engine. See entrypoint in app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)