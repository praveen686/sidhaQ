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

from brokers.zerodha.z_utilities import instrument_token, get_kite_interval
from typing import Optional
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

"""Securely fetches and returns a configured KiteConnect instance."""
load_dotenv()
api_key = os.getenv('zerodha_api_key')
api_secret = os.getenv("zerodha_api_secret")
zerodha_id = os.getenv("zerodha_id")
zerodha_password = os.getenv("zerodha_password")
totp_key = os.getenv("zerodha_totp_key")




@ray.remote(num_gpus=1, max_calls=1)
def fetch_ohlcv(kite, symbol: str, from_date: dt.datetime, to_date: dt.datetime, interval: dt.timedelta, exchange: str) -> Optional[pd.DataFrame]:
    """Fetches historical OHLCV data for a symbol from Zerodha's Kite Connect API using the provided KiteConnect
    instance."""
    data_chunks = []  # Collect data chunks here for efficiency

    try:
        kite_interval = get_kite_interval(interval)

        instrument_list = pd.DataFrame(kite.instruments(exchange))
        int_token = instrument_token(instrument_list, symbol)
        if int_token is None:
            raise ValueError(f"Could not find instrument token for symbol {symbol}")

        current_from_date = from_date
        while current_from_date < to_date:
            current_to_date = min(current_from_date + dt.timedelta(days=59), to_date)
            data_chunk = pd.DataFrame(
                kite.historical_data(int_token, current_from_date, current_to_date, interval=kite_interval))
            data_chunks.append(data_chunk)
            current_from_date = current_to_date + dt.timedelta(days=1)  # Adjust based on rate limits

        historical_data = pd.concat(data_chunks, ignore_index=True)
        historical_data['date'] = pd.to_datetime(historical_data['date'])
        historical_data.set_index('date', inplace=True)

        return historical_data
    except Exception as e:
        logging.error(f"Error fetching historical data for {symbol}: {e}")
        return None
