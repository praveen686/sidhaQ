# # -*- coding: utf-8 -*-
# """
# om geneshaya namaha
# sidhaQ - " Brief description of the script"
# @author: Praveen Ayyasola (www.sidhaQ.ai)
# @created: 16th Feb 2024
# @version: 1.0

# """

# Imports
import logging
import datetime as dt
import ray
import os

from brokers.zerodha.z_fetchOHLCV import fetch_ohlcv
from pathlib import Path
from dotenv import load_dotenv
from kiteconnect import KiteConnect

# Global path setup
BASE_DIR = Path(__file__).resolve().parent
print(BASE_DIR)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
dotenv_path = '/home/isoula/PycharmProjects/sidhaQ/brokers/zerodha/.env'
load_dotenv(dotenv_path=dotenv_path)

# Retrieve credentials and TOTP key from environment variables
api_key = os.getenv('ZERODHA_API_KEY')


# generate trading session
access_token_file_path = '/home/isoula/PycharmProjects/sidhaQ/z_access_token.txt'
access_token = open(access_token_file_path,'r').read()
kite = KiteConnect(api_key)
kite.set_access_token(access_token)
kite.access_token

# Fetch data in chunks
ray.init()

symbols = ['NIFTY BANK', 'NIFTY 50', 'NIFTY FIN SERVICE']  
from_date = dt.datetime(2023, 1, 1)  # Example start date
to_date = dt.datetime(2023, 2, 1)    # Example end date
interval = dt.timedelta(minutes=1)      # Example interval
exchange = 'NSE'                     # Example exchange

index = [fetch_ohlcv.remote(kite,symbol, from_date, to_date, interval, exchange) for symbol in symbols]
results = ray.get(index)

# Process or display your results
for symbol, result in zip(symbols, results):
    if result is not None:
        print(f"Data for {symbol}:", result.head())
    else:
        print(f"Failed to fetch data for {symbol}")      
ray.shutdown()    
