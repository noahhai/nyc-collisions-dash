import sys
import os
import click
from functools import reduce
import mysql.connector as conn
import threading
import httpx
import requests
import asyncio


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


@click.command()
@click.option('-b', '--borough-name', help='The borough name to query for.')
@click.option('-a', '--action', help='The action to run (load_incidents|stat|load_stations).', required=True,
              type=click.Choice(['load_incidents', 'load_stations', 'stat']))
def per_borough(borough_name, action):
    """Simple script to query NYC bike crash data per borough."""

    if action == "load_incidents" and borough_name:
        print("error: cannot specify 'borough_name' and 'action=load_incidents'. load_incidents must be done for full data set.")
        sys.exit(1)

    if action == "load_incidents":
        init_db_incidents()
    elif action == "load_stations":
        init_db_stations()
        data = fetch_station_data()
        db = get_db()
        insert_db_stations(db, data)
        sys.exit(0)

    lock = threading.Lock()
    if action == 'stat':
        acc = {}
        handle_data_batch = lambda data_batch: stat_data(accumulator=acc, lock=lock, data=data_batch)
        data_callback = lambda: stat_data_print(acc, borough_name)
    else:
        db = get_db()
        handle_data_batch = lambda data_batch: insert_data_db(db=db, lock=lock, data=data_batch)
        data_callback = null_op

    loop = asyncio.get_event_loop()
    loop.run_until_complete(fetch_data(borough_name=borough_name, data_batch_callback=handle_data_batch, data_callback=data_callback))
    loop.close()



def get_db():
    return conn.connect(host=os.environ['DB_HOST'], port=os.environ['DB_PORT'],
                        user=os.environ['DB_USER'], passwd=os.environ['DB_PASSWORD'],
                        database=os.environ["DB_DATABASE"])


def insert_data_db(lock, db, data):
    # TODO : fork mysql-connector-python and make nullable columns optional on preparation of statement
    # then dont have to do this
    nullable_missing_fields = ['cross_street_name', 'on_street_name', 'off_street_name', 'cross_street_name',
                               'borough', 'zip_code', 'vehicle_type_code2', 'vehicle_type_code1',
                               'contributing_factor_vehicle_2', 'contributing_factor_vehicle_1',
                               'number_of_persons_injured', 'number_of_persons_killed']
    mandatory_missing_fields = ['longitude', 'latitude']
    cleaned = []
    skipped = 0
    for i, datum in enumerate(data):
        datum.pop('location', None)
        skip = False
        for k in mandatory_missing_fields:
            if not datum.get(k):
                skip = True
                continue
        if skip:
            skipped += 1
            continue
        for k in nullable_missing_fields:
            if not datum.get(k):
                datum[k] = None
        cleaned.append(datum)

    print(
        "INFO: inserted %i records" % int(len(data) - skipped))

    print(
        "WARN: %i records skipped due to missing mandatory data. " % skipped +
        "Please revisit to fill in mandatory longitude/latitude based on available address data")

    cursor = db.cursor()
    insert_query = """INSERT IGNORE INTO incident (
                            date, 
                            time, 
                            borough, 
                            zip_code, 
                            latitude, 
                            longitude, 
                            on_street_name, 
                            off_street_name, 
                            cross_street_name, 
                            number_of_persons_injured, 
                            number_of_persons_killed, 
                            number_of_pedestrians_injured, 
                            number_of_pedestrians_killed, 
                            number_of_cyclist_injured, 
                            number_of_cyclist_killed, 
                            number_of_motorist_injured, 
                            number_of_motorist_killed, 
                            contributing_factor_vehicle_1, 
                            contributing_factor_vehicle_2, 
                            unique_key, 
                            vehicle_type_code1, 
                            vehicle_type_code2
                        )
                        VALUES (
                            %(date)s, 
                            %(time)s, 
                            %(borough)s, 
                            %(zip_code)s, 
                            %(latitude)s, 
                            %(longitude)s, 
                            %(on_street_name)s, 
                            %(off_street_name)s, 
                            %(cross_street_name)s, 
                            %(number_of_persons_injured)s, 
                            %(number_of_persons_killed)s, 
                            %(number_of_pedestrians_injured)s, 
                            %(number_of_pedestrians_killed)s, 
                            %(number_of_cyclist_injured)s, 
                            %(number_of_cyclist_killed)s, 
                            %(number_of_motorist_injured)s, 
                            %(number_of_motorist_killed)s, 
                            %(contributing_factor_vehicle_1)s, 
                            %(contributing_factor_vehicle_2)s, 
                            %(unique_key)s, 
                            %(vehicle_type_code1)s, 
                            %(vehicle_type_code2)s
                        )"""
    lock.acquire()
    try:
        cursor.executemany(insert_query, cleaned)
        db.commit()
    except Exception as ex:
        print(ex)
        db.rollback()
        raise
    finally:
        lock.release()


def init_db_incidents():
    db = get_db()
    cursor = db.cursor()
    cursor.execute("DROP TABLE IF EXISTS incident")
    cursor.execute("""
        CREATE TABLE incident (
            date date not null,
            time timestamp,
            borough varchar(128) null,
            zip_code varchar(16) null,
            latitude float not null,
            longitude float not null,
            on_street_name varchar(255) null,
            off_street_name varchar(255) null,
            cross_street_name varchar(255) null,
            number_of_persons_injured smallint unsigned not null,
            number_of_persons_killed smallint unsigned not null,
            number_of_pedestrians_injured smallint unsigned not null,
            number_of_pedestrians_killed smallint unsigned not null,
            number_of_cyclist_injured smallint unsigned not null,
            number_of_cyclist_killed smallint unsigned not null,
            number_of_motorist_injured smallint unsigned not null,
            number_of_motorist_killed smallint unsigned not null,
            contributing_factor_vehicle_1 varchar(255),
            contributing_factor_vehicle_2 varchar(255),
            unique_key varchar(20),
            vehicle_type_code1 varchar(255),
            vehicle_type_code2 varchar(255)
        )
    """)
    db.commit()

def init_db_stations():
    db = get_db()
    cursor = db.cursor()
    cursor.execute("DROP TABLE IF EXISTS station")
    cursor.execute("""
        CREATE TABLE station (
            id int not null,
            stationName varchar(128),
            availableDocks int,
            totalDocks int,
            latitude float not null,
            longitude float not null,
            statusValue varchar(40) null,
            statusKey int null,
            availableBikes int,
            stAddress1 varchar(128),
            stAddress2 varchar(128),
            postalCode varchar(16) null            
        )
    """)
    db.commit()

def insert_db_stations(db, data):

    cursor = db.cursor()
    insert_query = """INSERT IGNORE INTO station (
                            id,
                            stationName,
                            availableDocks,
                            totalDocks,
                            latitude,
                            longitude,
                            statusValue,
                            statusKey,
                            availableBikes,
                            stAddress1,
                            stAddress2,
                            postalCode
                        )
                        VALUES (
                            %(id)s,
                            %(stationName)s,
                            %(availableDocks)s,
                            %(totalDocks)s,
                            %(latitude)s,
                            %(longitude)s,
                            %(statusValue)s,
                            %(statusKey)s,
                            %(availableBikes)s,
                            %(stAddress1)s,
                            %(stAddress2)s,
                            %(postalCode)s
                        )"""
    try:
        cursor.executemany(insert_query, data)
        db.commit()
    except Exception as ex:
        print(ex)
        db.rollback()
        raise


def stat_data_print(summarial_data, borough_name):
    print("Summary results:")
    print("total %i records for borough %s" % (summarial_data['n'], borough_name or 'all'))
    for k in stat_indexes:
        print("total %s: %i" % (k, summarial_data[k]))


def stat_data(lock, accumulator, data):
    lock.acquire()
    try:
        # initialize accumulator if empty
        if not accumulator.get('n', None):
            accumulator['n'] = 0
            for k in stat_indexes:
                accumulator[k] = 0

        kill_iter = ({k: int(datum.get(k,0)) for k in stat_indexes} for datum in data)
        summarial = reduce(lambda x, y: {k: x[k] + y[k] for k in stat_indexes}, kill_iter)
        n_acc = accumulator['n']
        n_batch = len(data)
        n_total = n_acc + n_batch
        for k in stat_indexes:
            accumulator[k] = accumulator[k] + summarial[k]
        accumulator['n'] = n_total
    except:
        raise
    finally:
        lock.release()


stat_indexes = [
    'number_of_persons_killed',
    'number_of_persons_injured',
    'number_of_pedestrians_killed',
    'number_of_pedestrians_injured',
    'number_of_cyclist_killed',
    'number_of_cyclist_injured',
    'number_of_motorist_killed',
    'number_of_motorist_injured',
]


async def fetch_data(borough_name=None, data_batch_callback=lambda: None, data_callback=lambda: None):
    # if os.environ.get('NYC_OPEN_DATA_TOKEN') and os.environ.get('NYC_OPEN_DATA_USER') and \
    #         os.environ.get('NYC_OPEN_DATA_PASS'):
    #     client = Socrata('data.cityofnewyork.us', os.environ['NYC_OPEN_DATA_TOKEN'],
    #                      username=os.environ['NYC_OPEN_DATA_USER'],
    #                      password=os.environ['NYC_OPEN_DATA_PASS'])
    # else:
    #     client = Socrata('data.cityofnewyork.us', None)

    http_client = httpx.AsyncClient()
    if os.environ.get('NYC_OPEN_DATA_TOKEN'):
        headers = {"X-App-token": os.environ.get('NYC_OPEN_DATA_TOKEN')}
    else:
        headers = {}
    fetch = get_http_query(http_client=http_client, socrata_client=None, borough_name=borough_name, headers=headers)

    fetch_tracker = {
        "curr_offset": 0,
        "batch_size": 5000,
    }
    lock = threading.Lock()

    # number processors should be good even though we're io-bound
    max_workers = os.cpu_count()
    await asyncio.wait([get_loop_fetch(fetch, data_batch_callback, "worker_%i" % i)(fetch_tracker, lock) for i in range(max_workers)])
    data_callback()

def get_loop_fetch(fetch, data_batch_callback, worker_name):
    async def loop_fetch(tracker, lock):
        while True:
            lock.acquire()
            offset = tracker["curr_offset"]
            tracker["curr_offset"] += tracker["batch_size"]
            lock.release()
            res_count = await fetch_batch(fetch, offset, tracker["batch_size"], data_batch_callback, worker_name)
            if res_count == 0:
                break
    return loop_fetch

async def fetch_batch(fetch, offset, batch_size, data_batch_callback, worker_name):
    res = await fetch(offset, batch_size)
    status = "%s: fetched batch with offset %i." % (worker_name, offset)
    if len(res) > 0:
        status += " Results from key %s to key %s." % (res[0]['unique_key'], res[len(res) - 1]['unique_key'])
    else:
        status += " No results."
    print(status)
    if len(res) > 0:
        data_batch_callback(res)
    return len(res)

def get_http_query(http_client, socrata_client, borough_name, headers):
    async def http_query(offset, batchsize):
        try:
            # if borough_name:
            #     return socrata_client.get("h9gi-nx95", borough=borough_name, limit=batch_size,
            #            offset=offset, order='unique_key')
            # else:
            #     return socrata_client.get("h9gi-nx95", limit=batch_size, offset=offset, order='unique_key')

            url = "https://data.cityofnewyork.us/resource/h9gi-nx95.json?$offset=%i&$limit=%i" % (offset, batchsize)
            if borough_name:
                url += "&borough=%s" % borough_name
            r = await http_client.get(url, headers=headers)
            return r.json()
        except requests.exceptions.RequestException as e:
            eprint(e)
    return http_query


def fetch_station_data():
    try:
        url = "https://feeds.citibikenyc.com/stations/stations.json"
        resp = requests.get(url)
        result = resp.json()
        return result["stationBeanList"]
    except requests.exceptions.RequestException as e:
        eprint(e)
        raise

def null_op(data=None):
    pass


if __name__ == '__main__':
    per_borough()
