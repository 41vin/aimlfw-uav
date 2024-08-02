# ==================================================================================
#       Copyright (c) 2020 AT&T Intellectual Property.
#       Copyright (c) 2020 HCL Technologies Limited.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#          http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
# ==================================================================================
"""

This module is temporary which aims to populate cell data into influxDB. This will be depreciated once KPIMON push cell info. into influxDB.

"""
import pandas as pd
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
import datetime


# -------------------------------------------------------------------------
# --------------------------------- Config --------------------------------
# -------------------------------------------------------------------------
DATASET_PATH = '/path/to/dataset.csv'
INFLUX_IP = 'localhost'
INFLUX_TOKEN = 'VJpoNpqeVnjzvhpPm8jZ'

# -------------------------------------------------------------------------
# -------------------------------- Constant  ------------------------------
# -------------------------------------------------------------------------
BUCKET_NAME = 'UAVData'
INFLUX_PORT = 8086


class INSERTDATA:
    def __init__(self):
        self.client = InfluxDBClient(
            url=f"http://{INFLUX_IP}:{INFLUX_PORT}", token=INFLUX_TOKEN)


def explode(df):
    for col in df.columns:
        if isinstance(df.iloc[0][col], list):
            df = df.explode(col)
        d = df[col].apply(pd.Series)
        df[d.columns] = d
        df = df.drop(col, axis=1)
    return df


def jsonToTable(df):
    df.index = range(len(df))
    cols = [col for col in df.columns if isinstance(
        df.iloc[0][col], dict) or isinstance(df.iloc[0][col], list)]
    if len(cols) == 0:
        return df
    for col in cols:
        d = explode(pd.DataFrame(df[col], columns=[col]))
        d = d.dropna(axis=1, how='all')
        df = pd.concat([df, d], axis=1)
        df = df.drop(col, axis=1).dropna()
    return jsonToTable(df)


def data_normalization(df):
    import numpy as np
    df.drop('time', axis=1, inplace=True)
    # Normalization
    std = []
    mean = []

    # Iterate over columns
    for col in df.columns:
        std_val = df[col].std()
        mean_val = df[col].mean()
        std.append(std_val)
        mean.append(mean_val)
        df[col] = (df[col] - mean_val) / std_val
    return df


def time(df):
    df.index = pd.date_range(
        start=datetime.datetime.now(), freq='10ms', periods=len(df))
    df['time'] = df['time'].apply(lambda x: str(x))
    return df


def populatedb():

    df = pd.read_csv(DATASET_PATH)
    df = jsonToTable(df)
    df = time(df)
    df = data_normalization(df)
    print(df)
    db = INSERTDATA()
    write_api = db.client.write_api(write_options=SYNCHRONOUS)
    write_api.write(bucket=BUCKET_NAME, record=df,
                    data_frame_measurement_name="liveCell", org="primary")


populatedb()
