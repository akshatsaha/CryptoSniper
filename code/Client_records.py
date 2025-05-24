import logging
import os
from Constant import *
from Utils import *
from mybroker import MyBroker,Brokers
from datetime import datetime, timedelta
import pymongo
import threading
import traceback
import time
import copy
import os
from os import path
from datetime import datetime
import pymongo
from Constant import *
from Utils import *
from typing import Dict, List, Callable
from mybroker import MyBroker, Brokers
import pydantic
from sys import argv
from cryptography.fernet import Fernet
from passlib.context import CryptContext
import base64





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



def get_broker(broker_name: str, credentials=dict) -> MyBroker:
    if broker_name in [Brokers.BingX, Brokers.DeltaEx, Brokers.BitMEX, Brokers.ByBit]:

        cred = decrypt_api_credentials(encrypted_key_b64=credentials['api_key'],encrypted_secret_b64=credentials['secret_key']) # type: ignore
        
        return MyBroker(broker_name, cred)
    else:
        raise ValueError(f"Unknown broker: {broker_name}")

def get_users_credentials(user:list):
    ## TODO: get users from db based on strategyName and they are running this strategy
    response = []
    for x in user:
        email = x['email']
        brokerData = brokerCollection.find_one({"userId":email,"is_active":True},{"_id":0,"userId":1,"broker_name":1,"credentials":1})
        if brokerData is None:
            continue

        response.append(brokerData)
    

    return response



if __name__ == "__main__":

    link = "mongodb+srv://vipinpal7060:lRKAbH2D7W18LMZd@cluster0.fg30pmw.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    mongo_client = pymongo.MongoClient(link)
    UserCollection = mongo_client['cryptosnipers']['users']
    brokerCollection = mongo_client['cryptosnipers']['broker_connections']
    PositionCollection = mongo_client['clientPositions']

    TOTP = "T2QOR2FTEEOLURCTWEBJGZKI5N4CF6KR"

    # API_KEY = "B88vBgorx9TwUxuGm5iHqyU9B0CcFJWStuUaS3IV6Ce0bswVl6hvnOi9DghwE1FDxVyONjZ4UAtiGdg"
    # SECRET_KEY = "U5Zu6tb2qeCYhaK2HsTTwXwORLkTlXZIWNfShkfbfmdz2Qy7ydvGo5498w2zLOFoCpub87F4NYaroya9PW7w"


    # API = "TsYIfUmV2aKpaa5XWJfvSrkF5kGvtCaQjONVLKl6JN5ELEa93g8JTjV2ThP6s9ewIMqKdTCmUCM1O2E2jyA"
    # SECRET = "a8hWnhCbQ8EMtlOTwN6zXuKGeMjOSJqhaR4y9y8rhLqpkYaJplxLcYtkflxdBmlsz7YIA4KuQr3Ey5PKjw"



    # Create logs directory if it doesn't exist
    logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)

    # Setup a separate logger for the main application with print capturing enabled
    main_logger = setup_logger(
        "BingX_Main",
        log_level=logging.INFO,
        log_to_console=True,
        log_to_file=True,
        log_file=os.path.join(logs_dir, "clientRecords.log"),
        capture_print=True,  
        capture_other_loggers=True  
    )

    while True:
        next_time = datetime.now().replace(second=0, microsecond=0)
        try:
            odate = datetime.now()
            print(odate)
            dt = int(odate.minute)

            Users = list(UserCollection.find({"status":"approved"},{'_id':0,"name": 1, "email": 1}))
            user_credentails = get_users_credentials(Users)
            print(user_credentails)

            for user in user_credentails:
                userid = user['userId']
                UserBroker = get_broker(user['broker_name'], user['credentials'])
                print(userid)
                print(UserBroker)

                # positionHistory
                st = (datetime.now() - timedelta(days=5)).timestamp()
                et = datetime.now().timestamp() 
                if dt == 20:
                    next_time = odate.replace(hour=odate.hour+1,minute=59,second=0)
                    for sym in ['BTC-USDT','ETH-USDT']:
                        print(sym)
                        positions = UserBroker.client.get_position_history(symbol=sym)
                        pos_data = positions['data']['positionHistory']
                        print(pos_data)

                        for x in pos_data:
                            PositionCollection[userid].update_one({"positionId":x['positionId']},{"$set":x},upsert=True)
                        time.sleep(1)

                    # positions = UserBroker.client.get_position_history(symbol="SOL-USDT",startTime=int(st*1000),endTime=int(et*1000))
                    # pos_data = positions['data']['positionHistory']

                    # for x in pos_data:
                    #     PositionCollection[userid].update_one({"positionId":x['positionId']},{"$set":x},upsert=True)

                    time.sleep(1)

            print(user)
            # pause.until(next_time)

        except Exception as e:
            import traceback
            traceback.print_exc()
            print(e)
            break

