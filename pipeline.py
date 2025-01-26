# pip install gtfs-realtime-bindings pandas requests
import time
import datetime
import warnings
import pandas as pd
from requests import get
from datetime import datetime
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
from db_conn import connect_db, fetch_db
warnings.filterwarnings('ignore')

def request_api_rapidkl(category, watermark):

    URL = f'https://api.data.gov.my/gtfs-realtime/vehicle-position/prasarana?category={category}'

    # Parse the GTFS Realtime feed
    feed = gtfs_realtime_pb2.FeedMessage()
    response = get(URL)
    feed.ParseFromString(response.content)

    # Extract and print vehicle position information
    vehicle_positions = [MessageToDict(entity.vehicle) for entity in feed.entity]
    df = pd.json_normalize(vehicle_positions)
    df['watermark'] = watermark

    if df.empty:
        print(f'ERROR: Dataframe is empty - {watermark}')
    else:
        print(f'STATUS: Dataframe created - {watermark}')

    return df


def generate_rapidkl_data(category, requests_amt):
    from datetime import datetime
    dfs = []
    for _ in range(requests_amt):
        df_output = request_api_rapidkl(category, datetime.now())
        dfs.append(df_output)
        time.sleep(30)

    if all([x.empty for x in dfs]):
        print('ERROR: All dataframe(s) is empty. Failed to generate dataset')
    else:   
        df_concat = pd.concat(dfs)
        return df_concat
    
# df_fetch = generate_rapidkl_data('rapid-bus-kl', 5)

def rename_col(df):
    return df.rename({
        'trip.tripId': 'trip_id',
        'trip.startTime': 'start_time',
        'trip.startDate': 'start_date',
        'trip.routeId': 'route_id',
        'position.latitude': 'latitude',
        'position.longitude': 'longitude',
        'position.bearing': 'bearing',
        'position.speed': 'speed',
        'vehicle.id': 'vehicle_id',
        'vehicle.licensePlate': 'license_plate'
        }, axis=1)

def convert_unixtime_to_standard(unixtime):
    return datetime.fromtimestamp(int(unixtime))


def create_dim_drivers():
    engine = connect_db()
    df_trip = fetch_db('SELECT * FROM dev.fact_daily_trip')
    bus_plates = df_trip['license_plate'].unique()
    driver_names = [f'driver_{str(x+1).zfill(5)}' for x in range(len(bus_plates))]
    df_drivers = pd.DataFrame({'driver_id':[x+1 for x in range(len(bus_plates))], 'driver_name':driver_names})
    df_drivers.to_sql('dim_drivers', con=engine, schema='dev', if_exists='replace', index=False)

def create_dim_busses():
    engine = connect_db()
    df_trip = fetch_db('SELECT * FROM dev.fact_daily_trip')
    bus_plates = sorted(df_trip['license_plate'].unique())
    bus_id = [x+1 for x in range(len(bus_plates))]
    df_bus = pd.DataFrame({'bus_id':bus_id, 'bus_plates': bus_plates})
    df_bus.to_sql('dim_busses', con=engine, schema='dev', if_exists='replace', index=False)

