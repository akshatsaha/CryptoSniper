from datetime import datetime,timedelta
from BgxClient import *
from Constant import *
from Utils import *
from websocket import WebSocketApp
import pymongo
from mybroker import MyBroker
import pandas as pd

def file_path_locator():

    wd = path.abspath(path.dirname(__file__))
    folder = wd + sep + pardir  # pardir Goes Back 1 Folder

    return folder


if __name__ == "__main__":

    link = "mongodb+srv://vipinpal7060:lRKAbH2D7W18LMZd@cluster0.fg30pmw.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    mongo_client = pymongo.MongoClient(link)
    
    folder = file_path_locator()
    logs_dir = path.join(path.normpath(folder), "logs")
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)


    try:
        # API = "TsYIfUmV2aKpaa5XWJfvSrkF5kGvtCaQjONVLKl6JN5ELEa93g8JTjV2ThP6s9ewIMqKdTCmUCM1O2E2jyA"
        # SECRET = "a8hWnhCbQ8EMtlOTwN6zXuKGeMjOSJqhaR4y9y8rhLqpkYaJplxLcYtkflxdBmlsz7YIA4KuQr3Ey5PKjw"
        # credentils = {
        #     "api_key": API,
        #     "secret_key": SECRET,
        # }
        # broker = MyBroker("BingX",credentils)

        
        BTC_CHANNEL = create_kline_channel("BTC-USDT", "1m")
        ETH_CHANNEL = create_kline_channel("ETH-USDT", "1m")
        SOL_CHANNEL = create_kline_channel("SOL-USDT", "1m")

        # Create WebSocket client with custom logging configuration
        ws = BingXWebSocketClient(
            channels=[BTC_CHANNEL,ETH_CHANNEL,SOL_CHANNEL],
            log_level=logging.DEBUG,  
            log_to_console=False,      
            log_to_file=True,         
            log_file=os.path.join(logs_dir, "bingx_ws.log"), 
            max_file_size=100*1024*1024, 
            backup_count=5,
            database=mongo_client
        )

        ws.start()

        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("Exiting...")
        ws.stop()
        
    except Exception as e:
        import traceback
        print(f"Error setting up logger: {e}")
        print(traceback.format_exc())
        ws.stop()
        exit(1)


