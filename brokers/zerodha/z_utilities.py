# -*- coding: utf-8 -*-
"""
om ganeshaya namaha
siriQ - "Brief description of the script"
@author: Praveen Ayyasola (www.siriQ.ai)
@created: 9th Feb 2024
@version: 1.0
"""

# Imports
import pandas as pd
import datetime as dt
import logging

# Configure logging at the module level. You might want to configure it more broadly for your application.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def instrument_token(data: pd.DataFrame, symbol: str) -> int:
    """
    Extracts instrument token values from Zerodha for NSE exchange.
    """
    try:
        token = data[data.tradingsymbol == symbol].instrument_token.values
        if token.size > 0:
            logger.info(f"Instrument token for '{symbol}' found: {token[0]}")
            return token[0]
        else:
            logger.error(f"Instrument token for symbol '{symbol}' not found.")
            raise ValueError(f"Instrument token for symbol '{symbol}' not found.")
    except Exception as e:
        logger.exception(f"Error fetching instrument token for '{symbol}': {e}")
        raise


def get_kite_interval(interval: dt.timedelta) -> str:
    """
    Converts a datetime.timedelta object to a Kite Connect compatible interval string.

    :param interval: The timedelta interval to convert.
    :return: A string representing the interval in Kite Connect API format.
    """
    # Define a mapping of timedelta intervals to Kite Connect interval strings
    interval_mapping = {
        dt.timedelta(days=1): "day",
        dt.timedelta(minutes=1): "minute",
        dt.timedelta(minutes=3): "3minute",
        dt.timedelta(minutes=5): "5minute",
        dt.timedelta(minutes=10): "10minute",
        dt.timedelta(minutes=15): "15minute",
        dt.timedelta(minutes=30): "30minute",
        dt.timedelta(hours=1): "60minute",
    }

    if interval in interval_mapping:
        logger.info(f"Interval {interval} converted to Kite Connect format: {interval_mapping[interval]}")
        return interval_mapping.get(interval, "day")  # Default to "day" if not found
    else:
        logger.warning(f"Unsupported interval: {interval}")
        raise ValueError(f"Unsupported interval: {interval}")


