from BgxClient import BingXClient,BingXWebSocketClient
import logging
import os
from Constant import *
from Utils import *
from mybroker import MyBroker,Brokers
from datetime import datetime, timedelta
 
from fastapi import FastAPI, Depends, HTTPException, Request, status, BackgroundTasks, Query, Header
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend
from fastapi_cache.decorator import cache
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, EmailStr, Field
from typing import Optional
from contextlib import asynccontextmanager
from fastapi import Request, HTTPException, status
from jose import jwt, JWTError
import logging
from datetime import datetime
import jwt
from jwt.exceptions import InvalidTokenError, DecodeError, ExpiredSignatureError
import motor.motor_asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId
from passlib.context import CryptContext
import sys
import os
import time
import string
import random
import json
import asyncio
import logging
from cryptography.fernet import Fernet
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formataddr
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
import base64
from mybroker import MyBroker,Brokers
from BgxClient import BingXClient
import os
from bson import ObjectId
from fastapi.responses import JSONResponse
from fastapi import FastAPI
import json
from bson import ObjectId
from fastapi.responses import JSONResponse
import json
from bson import ObjectId
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend



JWT_SECRET = "WFuFIpMpWx2kdYG0fFBb15GqwHlghQMpjI16i8BpP8W6WhC1LWU9TObowQ6F4gSTGygHFFSyfGTzCnX2CbDc3A=="
JWT_ALGORITHM = "HS256"


fernet_key = base64.urlsafe_b64encode(JWT_SECRET[:32].encode().ljust(32, b'0'))
fernet = Fernet(fernet_key)



# Security
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


#Helpers
def encrypt_api_credentials(api_key: str, secret_key: str):
    return {
        "api_key": base64.urlsafe_b64encode(fernet.encrypt(api_key.encode())).decode('utf-8'),
        "secret_key": base64.urlsafe_b64encode(fernet.encrypt(secret_key.encode())).decode('utf-8')
    }

def decrypt_api_credentials(encrypted_key_b64: str, encrypted_secret_b64: str):
    return {
        "api_key": fernet.decrypt(base64.urlsafe_b64decode(encrypted_key_b64.encode('utf-8'))).decode('utf-8'),
        "secret_key": fernet.decrypt(base64.urlsafe_b64decode(encrypted_secret_b64.encode('utf-8'))).decode('utf-8')
    }




if __name__ == "__main__":

    TOTP = "T2QOR2FTEEOLURCTWEBJGZKI5N4CF6KR"


    # API = "TsYIfUmV2aKpaa5XWJfvSrkF5kGvtCaQjONVLKl6JN5ELEa93g8JTjV2ThP6s9ewIMqKdTCmUCM1O2E2jyA"
    # SECRET = "a8hWnhCbQ8EMtlOTwN6zXuKGeMjOSJqhaR4y9y8rhLqpkYaJplxLcYtkflxdBmlsz7YIA4KuQr3Ey5PKjw"

    # Create logs directory if it doesn't exist
    logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
    
    try:

        # Setup a separate logger for the main application with print capturing enabled
        main_logger = setup_logger(
            "BingX_Main",
            log_level=logging.INFO,
            log_to_console=True,
            log_to_file=True,
            log_file=os.path.join(logs_dir, "bingx_main.log"),
            capture_print=True,  
            capture_other_loggers=True  
        )
        

        # credentils = {
        #     "api_key": API,
        #     "secret_key": SECRET,
        # }

        abc = {'userId': 'vipinpal7060@gmail.com', 'broker_name': 'BingX', 'credentials': {'app_name': 'Vipin_app', 'api_key': 'Z0FBQUFBQm9MY3ZyOHFmNS1RRTZQYnRvVFpZNXVhNnBCMGE1bTUwSlZ5UEl6RnB1VHlKd2stdWxxMHB4LXVzN05CWUZ0VTNHRkZyRmxMcjlOVk5HbkdNZkQ0dUFRQ2V1c0dxZWtQYnJQclNTTk4yc0VxX0VySUlLQkYxSGJuU3EtOTlGVE9wYjFXcVN2V2JyYkZNWlNlb081a2ZmR1docXotYTBDU0cwS2h3RkhlWWdFNHpOZ0pfMjhlei1xSEFsdnd5Z1U5X2REQVRk', 'secret_key': 'Z0FBQUFBQm9MY3ZyRW5lb2FqendvckZVOVowdVYxdU9wYWJYQ3YyNUJad2hweEJfQ1cwc0VKOFh2Vm1tMkRZTkRSMmVFZlFhb0F0QThoZHZ4NVB4SW81VUdLU3I1em93RFVKeEtDYjZYbEhJTk85cjZLQkduTDlJVWgzWXpKOG9vc2RjbDI5ZFQ4N1dkWHJhMm1xNUp6b3E2eUM0R082OW9yalZkT3FmamZTSEhON1RkS050cE0zQUR4SGFhbFl1NXZVd0h5X3c4QWpL'}}

        cd = decrypt_api_credentials(abc['credentials']['api_key'],abc['credentials']['secret_key'])


        client = BingXClient(cd['api_key'],cd['secret_key'])

        # ph = client.get_position_history(symbol="ETH-USDT")

        print(client.get_position_history(symbol="SOL-USDT"))
        print("======================================================")
        print(client.get_position_history(symbol="ETH-USDT"))
        print("======================================================")
        print(client.get_position_history(symbol="BTC-USDT"))
        print("======================================================")


        # ph = client.get_position_history(symbol="SOL-USDT")
        # ph = my_broker.client.get_orderbook(symbol="SOL-USDT")
        # print( "history",ph)
        # print(st)
        # print(et)


        
        # subscription
        # subscription = {
        #     "id": "e745cd6d-d0f6-4a70-8d5a-043e4c741b40",
        #     "reqType": "sub",
        #     "dataType": "BTC-USDT@depth5@500ms"
        # }

        # candle_subscription = create_kline_channel("BTC-USDT", "1m")
        

    
        # Create WebSocket client with custom logging configuration
        # ws = BingXWebSocketClient(
        #     channels=[candle_subscription],
        #     log_level=logging.DEBUG,  
        #     log_to_console=False,      
        #     log_to_file=True,         
        #     log_file=os.path.join(logs_dir, "bingx_ws.log"), 
        #     max_file_size=100*1024*1024, 
        #     backup_count=5            
        # )
    

        # main_logger.info("Application started - all print statements will be captured in log file")

        # spot_bal = user.get_account_balance(account_type=AccountType.SPOT)
        # print("Account Spot Balance:", spot_bal)

        # quote_symbols = user.get_quote_symbols()
        # print("Quote Symbols:", quote_symbols)

        # market_data = user.get_market_data(symbol='BTC-USDT')
        # print("Market Data:", market_data)

        # quote_sym = user.get_quote_symbols(symbol='BTC-USDT')
        # print("Quote Symbols:", quote_sym)

        # server_time = user.get_server_time()
        # print("Server Time:", server_time)

        # test_order = user.test_order(
        #     symbol='BTC-USDT',
        #     side='BUY',
        #     order_type='MARKET',
        #     position_direction='LONG',
        #     quantity=0.001,
        # )
        # print("Test Order:", test_order)

        # orderbook = user.get_orderbook(symbol='BTC-USDT', limit=100)
        # print("Orderbook:", orderbook)



        # fut_bal = user.get_account_balance(account_type=AccountType.FUTURE)
        # print("Account Future Balance:", fut_bal)

        # main_logger.info("Starting BingX WebSocket client")
        # ws.start()
        
        # Keep main thread alive
        # main_logger.info("WebSocket client running. Press Ctrl+C to stop.")
        
        # Example of changing log levels dynamically during runtime
        # Start with INFO level
        # time.sleep(5)
        # main_logger.info("Changing WebSocket client log level to DEBUG for more detailed logging")
        # ws.set_log_level(logging.DEBUG)
        
        # After some time, change back to INFO level
        # time.sleep(10)
        # main_logger.info("Changing WebSocket client log level back to INFO")
        # ws.set_log_level(logging.INFO)
        
        # Continue running
        # while True:
        #     time.sleep(1)

    except KeyboardInterrupt:
        main_logger.info("Received keyboard interrupt, shutting down...")
        # ws.stop()
        
        # Restore original stdout/stderr before exiting
        restore_stdout_stderr(main_logger)
        main_logger.info("Application shutdown complete")



