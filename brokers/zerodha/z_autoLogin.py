# # -*- coding: utf-8 -*-
# """
# om geneshaya namaha
# sidhaQ - " Brief description of the script"
# @author: Praveen Ayyasola (www.sidhaQ.ai)
# @created: 16th Feb 2024
# @version: 1.0

# """

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

def autologin(api_key, api_secret, zerodha_id, zerodha_password, totp_key) -> KiteConnect:
    try:
        totp = pyotp.TOTP(totp_key)
        twofa = totp.now()

        with requests.Session() as req_session:
            login_url = "https://kite.zerodha.com/api/login"
            twofa_url = "https://kite.zerodha.com/api/twofa"
            login_response = req_session.post(login_url, data={"user_id": zerodha_id, "password": zerodha_password,
                                                               "twofa_value": twofa})
            login_response.raise_for_status()

            login_data = login_response.json()
            if 'data' not in login_data or 'request_id' not in login_data['data']:
                logger.error("Invalid login response structure.")
                raise ValueError("Invalid login response structure.")

            request_id = login_data['data']['request_id']
            twofa_response = req_session.post(twofa_url, data={"user_id": zerodha_id, "request_id": request_id,
                                                               "twofa_value": twofa})
            twofa_response.raise_for_status()

            kite_login_url = f"https://kite.trade/connect/login?api_key={api_key}"
            zerodha_api_ssn = req_session.get(kite_login_url)
            request_token = zerodha_api_ssn.url.split("request_token=")[1].split("&")[0]

            kite = KiteConnect(api_key=api_key)
            gen_ssn = kite.generate_session(request_token, api_secret)
            kite.set_access_token(access_token=gen_ssn["access_token"])

            logger.info("KiteConnect session successfully established.")
            return kite

    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error occurred: {e.response.status_code} - {e.response.text}")
    except requests.exceptions.ConnectionError:
        logger.error("Connection error occurred. Please check your internet connection.")
    except requests.exceptions.Timeout:
        logger.error("Request timed out. The server might be too slow or down.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Unexpected network error occurred: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    return None


# Load environment variables
dotenv_path = '/home/isoula/PycharmProjects/sidhaQ/brokers/zerodha/.env'
load_dotenv(dotenv_path=dotenv_path)

# Retrieve credentials and TOTP key from environment variables
api_key = os.getenv('ZERODHA_API_KEY')
api_secret = os.getenv("ZERODHA_API_SECRET")
zerodha_id = os.getenv("ZERODHA_ID")
zerodha_password = os.getenv("ZERODHA_PASSWORD")
totp_key = os.getenv("ZERODHA_TOTP_KEY")

# Attempt to login and establish a KiteConnect session
kite = autologin(api_key, api_secret, zerodha_id, zerodha_password, totp_key)

if kite is not None:
    # Successfully established KiteConnect session
    access_token_file = 'z_access_token.txt'
    with open(access_token_file, 'w') as file:
        file.write(kite.access_token)
    logger.info("Access token stored successfully.")
else:
    # Failed to establish KiteConnect session
    logger.error("Failed to establish KiteConnect session, check the logs for details.")

from datetime import datetime, time

# Current time check function
def is_time_after_6_05_am() -> bool:
    now = datetime.now()
    target_time = time(6, 5)  # 6:05 AM
    return now.time() > target_time

def get_or_refresh_access_token() -> str:
    access_token_file_path = '/home/isoula/PycharmProjects/sidhaQ/z_access_token.txt'
    if is_time_after_6_05_am():
        # It's after 6:05 AM, try to login and refresh the token
        kite = autologin(api_key, api_secret, zerodha_id, zerodha_password, totp_key)
        if kite is not None:
            # Successfully established KiteConnect session, store new access token
            with open(access_token_file_path, 'w') as file:
                file.write(kite.access_token)
            logger.info("New access token stored successfully.")
            return kite.access_token
        else:
            # Failed to establish KiteConnect session, check the logs for details.
            logger.error("Failed to establish KiteConnect session, check the logs for details.")
            # Attempt to read the existing token from file as fallback
            if access_token_file_path.exists():
                with open(access_token_file_path, 'r') as file:
                    return file.read().strip()
            return None
    else:
        # It's before 6:05 AM, try to read the existing token from file
        if access_token_file_path.exists():
            with open(access_token_file_path, 'r') as file:
                return file.read().strip()
        else:
            logger.error("Access token file does not exist. A new session needs to be established.")
            return None

# Use the function to get or refresh the access token
access_token = get_or_refresh_access_token()

if access_token is not None:
    # Use the access token for further operations
    pass
else:
    logger.error("Access token could not be retrieved or generated.")