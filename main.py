import asyncio
import aiohttp
import csv
import logging
import os

from aiopg.sa import create_engine
from contextlib import closing
from flask import Flask, request
from flask_sqlalchemy import SQLAlchemy
from io import TextIOWrapper
from itertools import chain
from zipfile import ZipFile


app = Flask(__name__)

DSN = 'postgresql://postgres:qwertypark@localhost:5433/addr_db'
CSV_FILENAME = 'houses.csv'
CHUNK_SIZE = 1024 * 100
ZIP_FILENAME = 'houses.zip'
REMOTE_ZIP_URL = 'http://services.ukrposhta.com/postindex_new/upload/houses.zip'

os.environ['SQLALCHEMY_DATABASE_URI'] = DSN
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ['SQLALCHEMY_DATABASE_URI']
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)


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


async def update_db(dsn, zip_filename, csv_filename, batch_size=10000):
    async with create_engine(dsn) as engine:
        async with engine.acquire() as conn:
            with ZipFile(zip_filename) as z:
                with z.open(csv_filename, 'r') as f:
                    from datetime import datetime
                    fieldnames = ('region', 'district', 'city', 'zip_code', 'street', 'houses')
                    c = csv.DictReader(TextIOWrapper(f), fieldnames=fieldnames, delimiter=';')
                    headers = next(c)
                    await create_tables(conn)
                    for rows in grouped(c, batch_size):
                        now = datetime.now()

                        # region
                        table = 'region'
                        regions = set((row['region'] for row in rows))
                        sql = f'''INSERT INTO 
                           {table} (name) 
                           VALUES {",".join(["(%s)"] * len(regions))}
                           ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
                           RETURNING region.name, region.id'''
                        results = await conn.execute(sql, list(regions))
                        res = {row[0]: row[1] for row in results}
                        for row in rows:
                            row['region'] = res[row['region']]

                        # district
                        table = 'district'
                        districts = set((row['district'], row['region']) for row in rows)
                        sql = f'''INSERT INTO
                           {table} (name, region_id)
                           VALUES {",".join(["(%s,%s)"] * len(districts))}
                           ON CONFLICT (name, region_id) DO UPDATE SET name = EXCLUDED.name
                           RETURNING {table}.region_id, {table}.name, {table}.id'''
                        results = await conn.execute(sql, list(chain(*districts)))
                        res = {(row[0], row[1]): row[2] for row in results}
                        for row in rows:
                            row['district'] = res[row['region'], row['district']]

                        # city
                        table = 'city'
                        cities = set((row['city'], row['district']) for row in rows)
                        sql = f"""
                            INSERT INTO
                            {table} (name, district_id)
                            VALUES {",".join(["(%s,%s)"] * len(cities))}
                            ON CONFLICT (name, district_id) DO UPDATE SET name = EXCLUDED.name
                            RETURNING {table}.district_id, {table}.name, {table}.id
                            """
                        results = await conn.execute(sql, list(chain(*cities)))
                        res = {(row[0], row[1]): row[2] for row in results}
                        for row in rows:
                            row['city'] = res[row['district'], row['city']]

                        # street
                        table = 'street'
                        streets = set((row['street'], row['city']) for row in rows)
                        sql = f"""
                            INSERT INTO
                            {table} (name, city_id)
                            VALUES {",".join(["(%s,%s)"] * len(streets))}
                            ON CONFLICT (name, city_id) DO UPDATE SET name = EXCLUDED.name
                            RETURNING {table}.city_id, {table}.name, {table}.id
                            """
                        results = await conn.execute(sql, list(chain(*streets)))
                        res = {(row[0], row[1]): row[2] for row in results}
                        for row in rows:
                            row['street'] = res[row['city'], row['street']]
                        # house
                        table = 'house'
                        houses = set((row['zip_code'], house, row['street']) for row in rows for house in row['houses'].split(','))
                        sql = f"""
                            INSERT INTO
                            {table} (zip_code, number, street_id)
                            VALUES {",".join(["(%s,%s,%s)"] * len(houses))}
                            ON CONFLICT (zip_code, number, street_id) DO NOTHING
                            """
                        await conn.execute(sql, list(chain(*houses)))


@app.route('/')
def hello():
    """Return a friendly HTTP greeting."""
    a = 3
    return f'Hello World!, a={a}'


@app.route('/upload')
def index():
    asyncio.set_event_loop(asyncio.new_event_loop())
    with closing(asyncio.get_event_loop()) as loop:
        # sql = '''SELECT * FROM region'''
        # loop.run_until_complete(make_request(DSN, sql))

        print('download zip')
        loop.run_until_complete(download_zip(REMOTE_ZIP_URL, ZIP_FILENAME, loop))
        print('update tb')
        # loop.run_until_complete(update_db(DSN, ZIP_FILENAME, CSV_FILENAME))
    return 'Done!', 200, {'Content-Type': 'text/plain; charset=utf-8'}


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