# -*- coding: utf-8 -*-
"""
om geneshaya namaha
siriQ - " Brief description of the script"
@author: Praveen Ayyasola (www.siriQ.ai)
@created: 16th Feb 2024
@version: 1.0

"""

# Imports
import datetime as dt
import ray
import pandas as pd
import os
import logging
import pytz

from pathlib import Path
from brokers.zerodha.z_utilities import instrument_token, get_kite_interval
from typing import Optional
from dotenv import load_dotenv

# Global path setup
BASE_DIR = Path(__file__).resolve().parent

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

"""Securely fetches and returns a configured KiteConnect instance."""
load_dotenv()
api_key = os.getenv('zerodha_api_key')
api_secret = os.getenv("zerodha_api_secret")
zerodha_id = os.getenv("zerodha_id")
zerodha_password = os.getenv("zerodha_password")
totp_key = os.getenv("zerodha_totp_key")

import datetime as dt
import pandas as pd
import logging
from typing import Optional

@ray.remote(num_gpus=1, max_calls=1)
def fetch_ohlcv(kite, symbol: str, from_date: dt.datetime, to_date: dt.datetime, interval: dt.timedelta, exchange: str) -> Optional[pd.DataFrame]:
    data_chunks = []

    try:
        kite_interval = get_kite_interval(interval)

        instrument_list = pd.DataFrame(kite.instruments(exchange))
        int_token = instrument_token(instrument_list, symbol)
        if int_token is None:
            raise ValueError(f"Could not find instrument token for symbol {symbol}")

        current_from_date = from_date
        while current_from_date < to_date:
            current_to_date = min(current_from_date + dt.timedelta(days=59), to_date)
            data_chunk = pd.DataFrame(kite.historical_data(int_token, current_from_date, current_to_date, interval=kite_interval))
            data_chunks.append(data_chunk)
            current_from_date = current_to_date + dt.timedelta(days=1)

        historical_data = pd.concat(data_chunks, ignore_index=True)
        historical_data['date'] = pd.to_datetime(historical_data['date'], utc=True).dt.tz_convert('Asia/Kolkata')
        historical_data.set_index('date', inplace=True)

        print("Sample of converted data:")
        print(historical_data.head())  # Debugging: Check the first few rows

        # Save to CSV
        csv_filename = f"data/inputs/{symbol}_historical_data.csv"
        historical_data.to_csv(csv_filename)
        print(f"Data for {symbol} saved to {csv_filename}")  # Confirm file saving

        return historical_data
    except Exception as e:
        logging.error(f"Error fetching historical data for {symbol}: {e}")
        return None





