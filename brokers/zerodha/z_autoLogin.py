# -*- coding: utf-8 -*-
"""
om geneshaya namaha
siriQ - " Brief description of the script"
@author: Praveen Ayyasola (www.siriQ.ai)
@created: 16th Feb 2024
@version: 1.0

"""

# Imports
import pyotp
import os
import requests
import logging

from pathlib import Path
from kiteconnect import KiteConnect
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Global path setup
BASE_DIR = Path(__file__).resolve().parent

"""Securely fetches and returns a configured KiteConnect instance."""
dotenv_path = '/home/isoula/PycharmProjects/siriQ/.env'
load_dotenv(dotenv_path=dotenv_path)
api_key = os.getenv('ZERODHA_API_KEY')
api_secret = os.getenv("ZERODHA_API_SECRET")
zerodha_id = os.getenv("ZERODHA_ID")
zerodha_password = os.getenv("ZERODHA_PASSWORD")
totp_key = os.getenv("ZERODHA_TOTP_KEY")

def autologin(api_key, api_secret, zerodha_id, zerodha_password, totp_key) -> KiteConnect:
    """
    Logs into Kite and returns a KiteConnect instance with improved error handling.
    """
    try:
        totp = pyotp.TOTP(totp_key)
        twofa = totp.now()

        with requests.Session() as req_session:
            login_url = "https://kite.zerodha.com/api/login"
            twofa_url = "https://kite.zerodha.com/api/twofa"
            login_response = req_session.post(login_url, data={"user_id": zerodha_id, "password": zerodha_password,
                                                               "twofa_value": twofa})
            login_response.raise_for_status()  # Raises an error for bad responses

            login_data = login_response.json()
            if 'data' not in login_data or 'request_id' not in login_data['data']:
                logger.error("Invalid login response structure.")
                raise ValueError("Invalid login response structure.")

            request_id = login_data['data']['request_id']
            twofa_response = req_session.post(twofa_url, data={"user_id": zerodha_id, "request_id": request_id,
                                                               "twofa_value": twofa})
            # twofa_response.raise_for_status()  # Raises an error for bad responses

            kite_login_url = f"https://kite.trade/connect/login?api_key={api_key}"
            zerodha_api_ssn = req_session.get(kite_login_url)
            request_token = zerodha_api_ssn.url.split("request_token=")[1].split("&")[0]

            kite = KiteConnect(api_key=api_key)
            gen_ssn = kite.generate_session(request_token, api_secret)
            kite.set_access_token(access_token=gen_ssn["access_token"])

            logger.info("KiteConnect session successfully established.")
            return kite

    except requests.exceptions.RequestException as e:
        logger.error(f"Network error occurred: {e}")
    except ValueError as e:
        logger.error(f"Error processing login response: {e}")
    return None


# To be run once only after 6AM the next day,
kite = autologin(api_key, api_secret, zerodha_id, zerodha_password, totp_key)

# Generating and storing access token - valid till 6 am the next day
access_token_file = BASE_DIR / 'z_access_token.txt'
with open(access_token_file, 'w') as file:
    file.write(kite.access_token)
