import hmac
import hashlib
import time
import requests
import urllib.parse
from collections import OrderedDict
import json
import gzip
import io
import pytz
import logging
from logging.handlers import RotatingFileHandler
from threading import Thread
from websocket import WebSocketApp
from Utils import * 
from Constant import *
from pymongo import MongoClient
from datetime import datetime, timezone,timedelta
import os



# Base exception class for BingX errors
class BingXError(Exception):
    """Base exception class for BingX API errors"""
    pass

class BingXRequestError(BingXError):
    """Exception raised for network-related errors"""
    def __init__(self, message):
        super().__init__(message)


class BingXAPIError(BingXError):
    """Exception raised for API response errors"""
    def __init__(self, response_data):
        self.code = response_data.get('code', -1)
        self.msg = response_data.get('msg', 'Unknown error')
        super().__init__(f"API Error (code {self.code}): {self.msg}")

class BingXWebSocketClient:
    
    def __init__(self, url="wss://open-api-swap.bingx.com/swap-market", channels=None, 
                 log_level=logging.INFO, log_to_console=False, log_to_file=True, 
                 log_file="bingx_ws.log", max_file_size=5*1024*1024, backup_count=3,database:MongoClient=None):
        self.url = url
        self.channels = channels or []
        self.ws = None
        self.thread = None
        self.active = False
        self.database = database
        self.reconnect_interval = 5  # seconds

        
        
        # Setup logger with custom configuration
        self.logger = setup_logger(
            "BingX_WS", 
            log_level=log_level,
            log_to_console=log_to_console,
            log_to_file=log_to_file,
            log_file=log_file,
            max_file_size=max_file_size,
            backup_count=backup_count,
            capture_other_loggers=False  # Ensure WebSocket logs don't propagate to parent loggers
        )
        self.logger.debug(f"Initializing WebSocket client with URL: {url}")
        self.logger.debug(f"Configured channels: {channels}")

    def _decompress(self, data):
        """Handle GZIP decompression with error checking"""
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(data)) as f:
                return f.read().decode('utf-8')
        except Exception as e:
            self.logger.error(f"Decompression error: {e}")
            return None

    def _send_pong(self):
        """Send pong response to keep connection alive"""
        if self.ws and self.ws.sock and self.ws.sock.connected:
            self.ws.send("Pong")
            self.logger.debug("Sent Pong response")

    def _handle_message(self, message):
        """Process raw WebSocket messages"""
        if isinstance(message, bytes):
            decompressed = self._decompress(message)
            if decompressed == "Ping":
                self._send_pong()
                return
            try:
                data = json.loads(decompressed)
                # self.logger.info(f"Market Data Update: {data}")
                if self.database is not None:
                    sym = data.get('s')
                    coll = sym.replace("-","")
                    cand_db = self.database['CandleData']
                    ohlc_db = self.database['Ticks']
                    collection = cand_db[coll]
                    live = ohlc_db[coll]
                    candleData = data.get('data')[0]
                    if candleData is not None:
                        timestamp = int(candleData.get('T'))
                        utc_dt = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)

                        ohlc = {
                            "timestamp": int(candleData.get('T')),
                            "open": float(candleData.get('o')),
                            "high": float(candleData.get('h')),
                            "low": float(candleData.get('l')),
                            "close": float(candleData.get('c')),
                            "volume": float(candleData.get('v')),
                            "date":utc_dt
                        }

                        # ohlc111  = OrderedDict([
                        #     ("timestamp", ohlc['timestamp']),
                        #     ("open", ohlc['open']),
                        #     ("high", ohlc['high']),
                        #     ("low", ohlc['low']),
                        #     ("close", ohlc['close']),
                        #     ("volume", ohlc['volume']),
                        #     ("date",utc_dt)
                        # ])
                        
                        collection.update_one({"timestamp": ohlc["timestamp"]},{"$set":ohlc}, upsert=True)
                        live.update_one({"ID":0},{"$set":ohlc}, upsert=True)

                    else:
                        self.logger.warning("candleData is None")


                # Add your trading logic here
            except json.JSONDecodeError:
                self.logger.warning("Received non-JSON message")
        else:
            self.logger.warning(f"Unexpected message format: {type(message)}")

    def on_open(self, ws):
        """Connection open handler"""
        self.logger.info("WebSocket connection established")
        for channel in self.channels:
            sub_msg = json.dumps(channel)
            ws.send(sub_msg)
            self.logger.info(f"Subscribed to: {sub_msg}")

    def on_message(self, ws, message):
        """Incoming message handler"""
        self._handle_message(message)

    def on_error(self, ws, error):
        """Error handler"""
        self.logger.error(f"WebSocket error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        """Connection close handler"""
        self.logger.warning(f"Connection closed (code: {close_status_code}, reason: {close_msg})")
        if self.active:
            self.logger.info(f"Attempting reconnect in {self.reconnect_interval}s...")
            Thread(target=self._reconnect).start()

    def _reconnect(self):
        """Handle reconnection logic"""
        time.sleep(self.reconnect_interval)
        self.start()

    def start(self):
        """Start WebSocket connection"""
        self.active = True
        self.logger.info(f"Starting WebSocket connection to {self.url}")
        self.ws = WebSocketApp(
            self.url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        self.thread = Thread(target=self.ws.run_forever)
        self.thread.start()
        self.logger.debug("WebSocket thread started")

    def stop(self):
        """Graceful shutdown"""
        self.active = False
        self.logger.info("Stopping WebSocket client...")
        if self.ws:
            self.ws.close()
        if self.thread and self.thread.is_alive():
            self.thread.join()
        self.logger.info("WebSocket client stopped successfully")
        
    def set_log_level(self, level):
        """Change the log level of the WebSocket client logger
        
        Args:
            level: The new log level (e.g., logging.DEBUG, logging.INFO)
        """
        self.logger = set_log_level(self.logger, level)
        self.logger.debug(f"WebSocket client log level changed to {logging.getLevelName(level)}")


class BingXClient:

    _endpoints={
        ## Public Endpoints
        "get_server_time": "openApi/swap/v2/server/time",
        "get_quote_symbols": "/openApi/swap/v2/quote/contracts",
        "get_quote_depth": "/openApi/swap/v2/quote/depth",
        "get_quote_ticker": "/openApi/swap/v2/quote/ticker",
        "get_quote_kline":  "/openApi/swap/v3/quote/klines",
        "get_quote_trades": "/openApi/swap/v2/quote/trades",
        "get_quote_book_ticker": "/openApi/swap/v2/quote/bookTicker",
        "get_market_data": "/openApi/spot/v1/common/symbols",
        "get_historical_trades": "/openApi/swap/v1/market/historicalTrades",
        "get_market_price_canlde":"/openApi/swap/v1/market/markPriceKlines",
        "get_price_ticker":"/openApi/swap/v1/ticker/price",

        ## Account Endpoints
        "get_asset_balance": "/openApi/spot/v1/account/balance",
        "get_user_balance": "/openApi/swap/v3/user/balance",
        "get_spot_account_balance": "/openApi/spot/v1/account/balance",
        "get_future_account_balance": "/openApi/swap/v3/user/balance",
        "get_positions": "/openApi/swap/v2/user/positions",
        "get_account_profit_loss":"/openApi/swap/v2/user/income",
        "get_commission_rate": "/openApi/swap/v2/user/commissionRate",
        "get_position_risk": "/openApi/swap/v3/user/positionRisk",

        ## Trade Endpoints
        "post_test_order": "/openApi/swap/v2/trade/order/test",
        "post_place_order": "/openApi/swap/v2/trade/order",
        "post_batch_orders": "/openApi/swap/v2/trade/batchOrders",
        "post_close_position": "/openApi/swap/v2/trade/closeAllPositions",
        "delete_cancel_order": "/openApi/swap/v2/trade/order",
        "delete_cancel_multiple_orders": "/openApi/swap/v2/trade/batchOrders",
        "delete_open_all_orders": "/openApi/swap/v2/trade/allOpenOrders",
        "get_all_open_orders": "/openApi/swap/v2/trade/openOrders",
        "get_leverage": "/openApi/swap/v2/trade/leverage",
        "post_set_leverage": "/openApi/swap/v2/trade/leverage",
        "get_orderbook": "/openApi/swap/v2/trade/allOrders",
        "get_historical_orders": "/openApi/swap/v2/trade/allFillOrders",
        "post_position_mode_dual": "/openApi/swap/v1/positionSide/dual",
        "get_position_mode_dual": "/openApi/swap/v1/positionSide/dual",
        "post_close_position_by_id": "/openApi/swap/v1/trade/closePosition",
        "get_all_orders": "/openApi/swap/v1/trade/fullOrder",
        "get_historical_orders_details": "/openApi/swap/v2/trade/fillHistory",
        "get_position_history": "/openApi/swap/v1/trade/positionHistory",
        "get_account_assets": "/openApi/swap/v3/user/accountAssets",
        "get_account_assets_detail": "/openApi/swap/v3/user/accountAssetsDetail",
        "get_account_assets_detail": "/openApi/swap/v3/user/accountAssetsDetail",
        "get_recent_trades": "/openApi/spot/v1/market/trades",
        "get_order_details": "/openApi/swap/v2/trade/order",
        "get_open_orders": "/openApi/spot/v1/order/openOrders",
        "get_all_orders": "/openApi/spot/v1/order/historyOrders",
        "get_trade_fee": "/openApi/spot/v1/account/tradeFee",
        "get_asset_details": "/openApi/spot/v1/account/assetDetail",
        "get_margin_type":"/openApi/swap/v2/trade/marginType",
        "set_margin_type":"/openApi/swap/v2/trade/marginType",
    }

    def __init__(self, api_key, secret_key):
        self.api_key = api_key
        self.secret_key = secret_key
        self.base_url = 'https://open-api.bingx.com'
        self.logs_dir="logs"
        self.log_level=logging.INFO
        self.log_to_console=False 
        self.log_to_file=True
        self.log_file= "/home/ubuntu/CRYPTOCODE/code/logs/test.log" #os.path.join(self.logs_dir, "BingxClient.log") 
        self.max_file_size=100*1024*1024
        self.backup_count=5

        # os.makedirs(self.logs_dir, exist_ok=True)
        # print() /home/ubuntu/CRYPTOCODE

        self.logger = setup_logger(
            "BingX_Client", 
            log_level=self.log_level,
            log_to_console=self.log_to_console,
            log_to_file=self.log_to_file,
            log_file=self.log_file,
            max_file_size=self.max_file_size,
            backup_count=self.backup_count,
            capture_other_loggers=False,
            capture_print=False
        )

    def _generate_signature(self, params, method):
        # Always sort parameters alphabetically by key
        sorted_params = sorted(params.items(), key=lambda x: x[0])
        
        # Build query string
        query_string = "&".join([f"{k}={v}" for k, v in sorted_params])
        
        # Generate HMAC SHA256 signature
        signature = hmac.new(
            self.secret_key.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return signature, query_string

    def send_request(self, method, endpoint, params=None, recv_window=5000):
        url = f"{self.base_url}{endpoint}"
        headers = {'X-BX-APIKEY': self.api_key}
        
        # Initialize parameters
        request_params = {}
        if params:
            request_params.update(params)
        
        # Add timestamp and recvWindow
        request_params['timestamp'] = int(time.time() * 1000)
        if recv_window is not None:
            request_params['recvWindow'] = recv_window
        
        try:
            # Generate signature and get query string
            signature, query_string = self._generate_signature(request_params, method)
            
            if method in ['GET', 'DELETE']:
                # For GET/DELETE, append signature to query string
                url = f"{url}?{query_string}&signature={signature}"
                response = requests.request(
                    method,
                    url,
                    headers=headers
                )
            else:
                # For POST/PUT, add signature to params and send as JSON
                request_params['signature'] = signature
                headers['Content-Type'] = 'application/json'
                response = requests.request(
                    method,
                    url,
                    json=request_params,
                    headers=headers
                )
            
            # Handle response
            response.raise_for_status()
            data = response.json()
            
            if 'code' in data and data['code'] != 0:
                raise BingXAPIError(data)
                
            return data
            
        except requests.exceptions.RequestException as e:
            raise BingXRequestError(f"Request failed: {str(e)}") from e
        except ValueError as e:
            raise BingXAPIError(f"Failed to parse response: {str(e)}") from e


    def get_server_time(self):
        endpoint = self._endpoints.get('get_server_time')
        if not endpoint:
            raise ValueError("Invalid endpoint for get_server_time")
            
        response = self.send_request('GET', endpoint)
        
        if 'data' not in response or 'serverTime' not in response['data']:
            raise ValueError("Invalid response structure")
            
        return response['data']['serverTime']


    def get_quote_symbols(self,symbol:str):
        if symbol is None or "-" not in symbol :
            raise ValueError("Invalid symbol format like BTC-USDT")

        url = self._endpoints.get('get_quote_symbols')
        if not url:
            raise ValueError("Invalid endpoint for get_quote_symbols")

        if symbol is None:
            return self.send_request('GET', url, params={"recvWindow": 5000})
        

        # Implement logic to fetch quote symbols
        return self.send_request('GET', url, params={"symbol": symbol,"recvWindow": 5000})


    def get_user_balance(self):
        url = self._endpoints.get('get_user_balance')
        if not url:
            raise ValueError("Invalid endpoint for get_user_balance")

        return self.send_request('GET', url, params={'recvWindow': 5000})

    def get_assets_balance(self):
        url = self._endpoints.get('get_asset_balance')
        if not url:
            raise ValueError("Invalid endpoint for get_asset_balance")

        return self.send_request('GET', url, params={'recvWindow': 5000})

    def get_account_balance(self, account_type=AccountType.SPOT):

        if account_type == AccountType.SPOT:
            url = self._endpoints.get('get_spot_account_balance')
            if not url:
                raise ValueError('Invalid endpoint for get_account_balance')


        if account_type == AccountType.FUTURE:
            url = self._endpoints.get('get_future_account_balance')
            if not url:
                raise ValueError('Invalid endpoint for get_account_balance')

        return self.send_request('GET', url, params={'recvWindow': 5000})

    def get_market_data(self, symbol):
        if not symbol or "-" not in symbol:
            raise ValueError("Invalid symbol format like BTC-USDT")
            
        url = self._endpoints.get('get_market_data')
        if not url:
            raise ValueError("Invalid endpoint for get_market_data")

        return self.send_request('GET', url, params={'symbol': symbol})

        
    def get_positions(self, symbol=None):
        if symbol and "-" not in symbol:
            raise ValueError("Invalid symbol format! like BTC-USDT")
        url = self._endpoints.get('get_positions')
        if not url:
            raise ValueError('Invalid endpoint for get_positions')

        params = {}
        if symbol:
            params['symbol'] = symbol
            
        return self.send_request('GET', url, params=params)
        
    def get_account_profit_loss(self, symbol=None, incomeType=None, startTime=None, endTime=None, limit=500):
        if symbol and "-" not in symbol:
            raise ValueError("Invalid symbol format! like BTC-USDT")
        if incomeType and incomeType not in ["TRANSFER","REALIZED_PNL","FUNDING_FEE","TRADING_FEE","INSURANCE_CLEAR","TRIAL_FUND","ADL","SYSTEM_DEDUCTION","GTD_PRICE"]:
            raise ValueError("Invalid incomeType! Use Of these types: TRANSFER,REALIZED_PNL,FUNDING_FEE,TRADING_FEE,INSURANCE_CLEAR,TRIAL_FUND,ADL,SYSTEM_DEDUCTION,GTD_PRICE")

        url = self._endpoints.get('get_account_profit_loss')
        if not url:
            raise ValueError('Invalid endpoint for get_account_profit_loss')

        params = {
            'limit': limit
        }
        if symbol:
            params['symbol'] = symbol
        if incomeType:
            params['incomeType'] = incomeType
        if startTime:
            params['startTime'] = startTime
        if endTime:
            params['endTime'] = endTime
            
        return self.send_request('GET', url, params=params)
        
    def get_commission_rate(self, symbol=None):
        
        url = self._endpoints.get('get_commission_rate')
        if not url:
            raise ValueError('Invalid endpoint for get_commission_rate')

        params = {}
        if symbol:
            params['symbol'] = symbol
            
        return self.send_request('GET', url, params=params)
        

    ## Trade Endpoints

    def test_order(self, symbol, side, order_type, position_direction, quantity, price=None, time_in_force=None):
        if symbol is None or "-" not in symbol :
            raise ValueError('Symbol is required for test_order and must contain "-"')

        if side is None or side not in [OrderSide.BUY, OrderSide.SELL]:
            raise ValueError('Side is required for test_order and must be "BUY" or "SELL"')

        if order_type is None or order_type not in [OrderType.LIMIT, OrderType.MARKET,OrderType.STOP,OrderType.STOP_MARKET,OrderType.STOP_LOSS_LIMIT,OrderType.TAKE_PROFIT,OrderType.TAKE_PROFIT_LIMIT]:
            raise ValueError('Order type is required for test_order and must be "LIMIT" or "MARKET"')

        if order_type == OrderType.LIMIT and price is None and quantity is None:
            raise ValueError('Price & Quantity is required for LIMIT order type')

        if order_type == OrderType.MARKET and price is not None:
            raise ValueError('Price should not be provided for MARKET order type')

        if position_direction is None or position_direction not in [PositionType.LONG, PositionType.SHORT]:
            raise ValueError('Position direction is required for test_order and must be "LONG" or "SHORT"')

        if time_in_force is not None and time_in_force not in ['GTC', 'IOC', 'FOK']:
            raise ValueError('Invalid timeInForce value. Must be "GTC", "IOC", or "FOK"')

        url = self._endpoints.get('post_test_order')
        if not url:
            raise ValueError('Invalid endpoint for test_order')


        params = {
            'symbol': symbol,
            'side': side,
            'type': order_type,
            'positionSide': position_direction,
            'quantity': quantity
        }

        if price is not None:
            params['price'] = price

        if time_in_force is not None:
            params['timeInForce'] = time_in_force

        self.logger.info(f"Request params: {params}, URL: {url}")

        return self.send_request('POST', url, params=params)


    def place_order(self, symbol, side, order_type, position_direction, quantity, price=None, time_in_force=None, clientOrderId:str=None, stopPrice=None, workingType=None):
        print("Inside place_order ")
        if symbol is None or "-" not in symbol :
            raise ValueError('Symbol is required for test_order and must contain "-"')

        if side is None or side not in [OrderSide.BUY, OrderSide.SELL]:
            raise ValueError('Side is required for test_order and must be "BUY" or "SELL"')

        if order_type is None or order_type not in [OrderType.LIMIT, OrderType.MARKET,OrderType.STOP,OrderType.STOP_MARKET,OrderType.STOP_LOSS_LIMIT,OrderType.TAKE_PROFIT,OrderType.TAKE_PROFIT_LIMIT]:
            raise ValueError('Order type is required for test_order and must be "LIMIT" or "MARKET", STOP, STOP_LOSS, STOP_LOSS_LIMIT, TAKE_PROFIT, TAKE_PROFIT_LIMIT')

        if order_type == OrderType.LIMIT and price is None and quantity is None:
            raise ValueError('Price & Quantity is required for LIMIT order type')

        if order_type == OrderType.MARKET and price is not None:
            raise ValueError('Price should not be provided for MARKET order type')

        if position_direction is None or position_direction not in [PositionType.LONG, PositionType.SHORT]:
            raise ValueError('Position direction is required for test_order and must be "LONG" or "SHORT"')

        url = self._endpoints.get('post_place_order')
        if not url:
            raise ValueError('Invalid endpoint for place_order')

        params = {
            'symbol': symbol,
            'side': side,
            'type': order_type,
            'positionSide': position_direction,
            'quantity': quantity
        }

        if price is not None:
            params['price'] = price

        if time_in_force is not None:
            params['timeInForce'] = time_in_force

        if clientOrderId is not None:
            params['clientOrderId'] = clientOrderId

        if stopPrice is not None:
            params['stopPrice'] = stopPrice
            params['workingType'] = workingType or "CONTRACT_PRICE"
            # Removed stopGuaranteed parameter as it's not available for all accounts


        self.logger.info(f"Request params: {params}, URL: {url}")
        

        return self.send_request('POST', url, params=params)


        
    def close_all_position(self, symbol=None):
        if symbol and "-" not in symbol:
            raise ValueError('Invalid symbol format! like BTC-USDT')

        url = self._endpoints.get('post_close_position')
        if not url:
            raise ValueError('Invalid endpoint for post_close_position')

        params = {}
        if symbol:
            params['symbol'] = symbol
            
        return self.send_request('POST', url, params=params)
        

    def cancel_order(self, symbol:str, order_id:int=None):
        if symbol is None or "-" not in symbol :
            raise ValueError('Symbol is required for cancel_order and must contain "-" like BTC-USDT')

        if order_id is None:
            raise ValueError('Order ID is required for cancel_order')

        url = self._endpoints.get('delete_cancel_order')
        if not url:
            raise ValueError('Invalid endpoint for cancel_order')

        params = {
           'symbol': symbol,
           'orderId': order_id 
        }
        return self.send_request('DELETE', url, params=params)


    def cancel_all_orders(self, symbol):
        if symbol and "-" not in symbol:
            raise ValueError('Invalid symbol format! like BTC-USDT')
        
        url = self._endpoints.get('delete_cancel_multiple_orders')
        if not url:
            raise ValueError('Invalid endpoint for cancel_all_orders')

        params = {
          'symbol': symbol
        }
        return self.send_request('DELETE', url, params=params)

        
    def get_all_open_orders(self, symbol=None):
        if symbol and "-" not in symbol:
            raise ValueError('Invalid symbol format! like BTC-USDT')
        
        url = self._endpoints.get('get_all_open_orders')
        if not url:
            raise ValueError('Invalid endpoint for get_all_open_orders')

        params = {}
        if symbol:
            params['symbol'] = symbol
            
        return self.send_request('GET', url, params=params)
     


    def get_margin_type(self,symbol:str=None):
        if symbol and "-" not in symbol:
            raise ValueError('Invalid symbol format! like BTC-USDT')
        
        url = self._endpoints.get('get_margin_type')
        if not url:
            raise ValueError('Invalid endpoint for cancel_all_orders')

        params = {
          'symbol': symbol
        }
        return self.send_request('GET', url, params=params)



    def set_margin_type(self,symbol:str=None,margin_type:str=None):
        if symbol and "-" not in symbol:
            raise ValueError('Invalid symbol format! like BTC-USDT')
        
        if margin_type not in ["ISOLATED","CROSSED","SEPARATE_ISOLATED"]:
            raise ValueError('Wrong Margin Type! use ISOLATED , CROSSED, SEPARATE_ISOLATED ')
        
        url = self._endpoints.get('set_margin_type')
        if not url:
            raise ValueError('Invalid endpoint for cancel_all_orders')

        params = {
          'symbol': symbol
        }
        return self.send_request('GET', url, params=params)



    def get_open_orders(self, symbol):
        url = self._endpoints.get('get_open_orders')
        if not url:
            raise ValueError('Invalid endpoint for get_open_orders')

        params = {
          'symbol': symbol
        }
        return self.send_request('GET', url, params=params)


    def get_all_orders(self, symbol):
        url = self._endpoints.get('get_all_orders')
        if not url:
            raise ValueError('Invalid endpoint for get_all_orders')

        params = {
         'symbol': symbol
        }
        return self.send_request('GET', url, params=params)


    def get_order_details(self, symbol:str, order_id:int):

        url = self._endpoints.get('get_order_details')
        if not url:
            raise ValueError('Invalid endpoint for get_order_details')

        params = {
         'symbol': symbol,
         'orderId': order_id
        }
        return self.send_request('GET', url, params=params)


    def get_quote_depth(self, symbol, limit=100):
        url = self._endpoints.get('get_quote_depth')
        if not url:
            raise ValueError('Invalid endpoint for get_quote_depth')

        params = {
        'symbol': symbol,
        'limit': limit 
        }
        return self.send_request('GET', url, params=params)


    def get_quote_ticker(self, symbol):
        url = self._endpoints.get('get_quote_ticker')
        if not url:
            raise ValueError('Invalid endpoint for get_quote_ticker')

        params = {
       'symbol': symbol
        }
        return self.send_request('GET', url, params=params)


    def get_quote_trades(self, symbol, limit=100):
        url = self._endpoints.get('get_quote_trades')
        if not url:
            raise ValueError('Invalid endpoint for get_quote_trades')

        params = {
        'symbol': symbol,
        'limit': limit
        }
        return self.send_request('GET', url, params=params)


    def get_orderbook(self, symbol, limit=100):
        if symbol is None or "-" not in symbol :
            raise ValueError('Symbol is required for get_orderbook and must contain "-"')
        
        url = self._endpoints.get('get_orderbook')
        if not url:
            raise ValueError('Invalid endpoint for get_orderbook')

        params = {
            "symbol": symbol,
            'limit': limit
        }
        return self.send_request('GET', url, params=params)
        
    def get_quote_book_ticker(self, symbol):
        """Get best price/qty on the order book for a symbol"""
        url = self._endpoints.get('get_quote_book_ticker')
        if not url:
            raise ValueError('Invalid endpoint for get_quote_book_ticker')

        params = {
            'symbol': symbol
        }
        return self.send_request('GET', url, params=params)
        
    def get_historical_trades(self, symbol, limit=500, fromId=None):
        """Get historical trades for a specific symbol"""
        url = self._endpoints.get('get_historical_trades')
        if not url:
            raise ValueError('Invalid endpoint for get_historical_trades')

        params = {
            'symbol': symbol,
            'limit': limit
        }
        if fromId:
            params['fromId'] = fromId
            
        return self.send_request('GET', url, params=params)
        
    def get_market_price_candle(self, symbol, interval, startTime=None, endTime=None, limit=500):
        """Get mark price klines/candlestick data"""
        url = self._endpoints.get('get_market_price_canlde')
        if not url:
            raise ValueError('Invalid endpoint for get_market_price_candle')

        params = {
            'symbol': symbol,
            'interval': interval,
            'limit': limit
        }
        if startTime:
            params['startTime'] = startTime
        if endTime:
            params['endTime'] = endTime
            
        return self.send_request('GET', url, params=params)
        
    def get_price_ticker(self, symbol=None):
        """Get latest price for a symbol or all symbols"""
        url = self._endpoints.get('get_price_ticker')
        if not url:
            raise ValueError('Invalid endpoint for get_price_ticker')

        params = {}
        if symbol:
            params['symbol'] = symbol
            
        return self.send_request('GET', url, params=params)
        

    def get_kline_data(self, symbol, interval, startTime=None, endTime=None, limit=500):
        """Get kline/candlestick data for spot markets"""
        if symbol and "-" not in symbol:
            raise ValueError('Invalid symbol format! like BTC-USDT')
            
        url = self._endpoints.get('get_quote_kline')
        if not url:
            raise ValueError('Invalid endpoint for get_quote_kline')

        params = {
            'symbol': symbol,
            'interval': interval,
            'limit': limit
        }
        if startTime:
            params['startTime'] = startTime
        if endTime:
            params['endTime'] = endTime
            
        return self.send_request('GET', url, params=params)





    def get_position_risk(self, symbol=None):
        """Get position risk information"""
        url = self._endpoints.get('get_position_risk')
        if not url:
            raise ValueError('Invalid endpoint for get_position_risk')

        params = {}
        if symbol:
            params['symbol'] = symbol
            
        return self.send_request('GET', url, params=params)
        
    def post_batch_orders(self, orders):
        """Place multiple orders in a single request"""
        if not isinstance(orders, list) or len(orders) == 0:
            raise ValueError('Orders must be a non-empty list of order parameters')
            
        url = self._endpoints.get('post_batch_orders')
        if not url:
            raise ValueError('Invalid endpoint for post_batch_orders')

        # Each order in the list should have the required parameters
        for order in orders:
            if 'symbol' not in order or '-' not in order['symbol']:
                raise ValueError('Symbol is required for each order and must contain "-"')
            if 'side' not in order or order['side'] not in [OrderSide.BUY, OrderSide.SELL]:
                raise ValueError('Side is required for each order and must be "BUY" or "SELL"')
            if 'type' not in order or order['type'] not in [OrderType.LIMIT, OrderType.MARKET]:
                raise ValueError('Order type is required for each order and must be "LIMIT" or "MARKET"')
            if 'positionSide' not in order or order['positionSide'] not in [PositionType.LONG, PositionType.SHORT]:
                raise ValueError('Position direction is required for each order and must be "LONG" or "SHORT"')
            if 'quantity' not in order:
                raise ValueError('Quantity is required for each order')
            if order['type'] == OrderType.LIMIT and 'price' not in order:
                raise ValueError('Price is required for LIMIT order type')

        return self.send_request('POST', url, params={'batchOrders': orders})

    def delete_cancel_multiple_orders(self, symbol, orderIdList=None):
        """Cancel multiple orders for a symbol"""
        if symbol is None or "-" not in symbol:
            raise ValueError('Symbol is required for delete_cancel_multiple_orders and must contain "-"')
            
        url = self._endpoints.get('delete_cancel_multiple_orders')
        if not url:
            raise ValueError('Invalid endpoint for delete_cancel_multiple_orders')

        params = {
            'symbol': symbol
        }
        if orderIdList:
            if not isinstance(orderIdList, list):
                raise ValueError('orderIdList must be a list of order IDs')
            params['orderIdList'] = orderIdList
            
        return self.send_request('DELETE', url, params=params)
        
    def delete_open_all_orders(self, symbol):
        """Cancel all open orders for a symbol"""
        if symbol is None or "-" not in symbol:
            raise ValueError('Symbol is required for delete_open_all_orders and must contain "-"')
            
        url = self._endpoints.get('delete_open_all_orders')
        if not url:
            raise ValueError('Invalid endpoint for delete_open_all_orders')

        params = {
            'symbol': symbol
        }
        return self.send_request('DELETE', url, params=params)
   
    def get_leverage(self, symbol):
        """Get current leverage setting"""
        if symbol is None or "-" not in symbol:
            raise ValueError('Symbol is required for get_leverage and must contain "-"')
            
        url = self._endpoints.get('get_leverage')
        if not url:
            raise ValueError('Invalid endpoint for get_leverage')

        params = {
            'symbol': symbol
        }
        return self.send_request('GET', url, params=params)
        
    def set_leverage(self, symbol, leverage, positionSide=str):
        """Change leverage for a symbol"""
        if symbol is None or "-" not in symbol:
            raise ValueError('Symbol is required for set_leverage and must contain "-"')
        if leverage is None or not isinstance(leverage, (int, float)):
            raise ValueError('Leverage must be a number')
            
        url = self._endpoints.get('post_set_leverage')
        if not url:
            raise ValueError('Invalid endpoint for post_set_leverage')

        params = {
            'symbol': symbol,
            'leverage': leverage,
        }
        if positionSide:
            if positionSide not in [PositionType.LONG, PositionType.SHORT]:
                raise ValueError('Position side must be "LONG" or "SHORT"')
            params['side'] = positionSide
            
        return self.send_request('POST', url, params=params)
        
    def get_historical_orders(self, symbol:str=None, orderId=None, startTime=None, endTime=None, limit=500):
        """Get all filled orders history"""
        if symbol and "-" not in symbol:
            raise ValueError('Symbol is required for get_historical_orders and must contain "-"')
            
        url = self._endpoints.get('get_historical_orders')
        if not url:
            raise ValueError('Invalid endpoint for get_historical_orders')

        params = {
            'symbol': symbol,
            'limit': limit
        }
        if orderId:
            params['orderId'] = orderId
        if startTime:
            params['startTime'] = startTime
        else:
            params['startTime'] = int(time.time() * 1000) -  7 * 24 * 60 * 60 * 1000
        if endTime:
            params['endTime'] = endTime
        else:
            params['endTime'] = int(time.time() * 1000)
            
        return self.send_request('GET', url, params=params)
        
    def post_position_mode_dual(self, dualSidePosition):
        """Change position mode (Hedge Mode or One-way Mode)"""
        if dualSidePosition is None:
            raise ValueError('dualSidePosition is required for post_position_mode_dual')
            
        url = self._endpoints.get('post_position_mode_dual')
        if not url:
            raise ValueError('Invalid endpoint for post_position_mode_dual')

        params = {
            'dualSidePosition': dualSidePosition
        }
        return self.send_request('POST', url, params=params)
        
    def get_position_mode_dual(self):
        """Get current position mode (Hedge Mode or One-way Mode)"""
        url = self._endpoints.get('get_position_mode_dual')
        if not url:
            raise ValueError('Invalid endpoint for get_position_mode_dual')

        return self.send_request('GET', url)
        
    def post_close_position_by_id(self, symbol, positionId):
        """Close a specific position by ID"""
        if symbol is None or "-" not in symbol:
            raise ValueError('Symbol is required for post_close_position_by_id and must contain "-"')
        if positionId is None:
            raise ValueError('Position ID is required for post_close_position_by_id')
            
        url = self._endpoints.get('post_close_position_by_id')
        if not url:
            raise ValueError('Invalid endpoint for post_close_position_by_id')

        params = {
            'symbol': symbol,
            'positionId': positionId
        }
        return self.send_request('POST', url, params=params)
        
    def get_historical_orders_details(self, symbol, orderId=None, startTime=None, endTime=None, limit=500):
        """Get detailed history of filled orders"""
        if symbol is None or "-" not in symbol:
            raise ValueError('Symbol is required for get_historical_orders_details and must contain "-"')
            
        url = self._endpoints.get('get_historical_orders_details')
        if not url:
            raise ValueError('Invalid endpoint for get_historical_orders_details')

        params = {
            'symbol': symbol,
            'limit': limit
        }
        if orderId:
            params['orderId'] = orderId
        if startTime:
            params['startTime'] = startTime
        if endTime:
            params['endTime'] = endTime
            
        return self.send_request('GET', url, params=params)
        
    def get_position_history(self, symbol=None, startTime:int=0, endTime:int=0, limit=500):
        """Get position history"""
        url = self._endpoints.get('get_position_history')
        if not url:
            raise ValueError('Invalid endpoint for get_position_history')

        st = (datetime.now() - timedelta(days=30)).timestamp()
        et = datetime.now().timestamp() 
        params = {
            'limit': limit
        }
        if symbol:
            params['symbol'] = symbol
        if startTime == 0:
            params['startTs'] = int(st*1000)
        else:
            params['startTs'] = startTime
        if endTime == 0:
            params['endTs'] = int(et*1000)
        else:
            params['endTs'] = endTime            
        
        return self.send_request('GET', url, params=params)
        
        
    def get_account_assets(self):
        """Get account assets information"""
        url = self._endpoints.get('get_account_assets')
        if not url:
            raise ValueError('Invalid endpoint for get_account_assets')

        return self.send_request('GET', url)
        
    def get_account_assets_detail(self, asset=None):
        """Get detailed account assets information"""
        url = self._endpoints.get('get_account_assets_detail')
        if not url:
            raise ValueError('Invalid endpoint for get_account_assets_detail')

        params = {}
        if asset:
            params['asset'] = asset
            
        return self.send_request('GET', url, params=params)
        
    def get_recent_trades(self, symbol, limit=500):
        """Get recent trades for a symbol"""
        if symbol is None:
            raise ValueError('Symbol is required for get_recent_trades')
            
        url = self._endpoints.get('get_recent_trades')
        if not url:
            raise ValueError('Invalid endpoint for get_recent_trades')

        params = {
            'symbol': symbol,
            'limit': limit
        }
        return self.send_request('GET', url, params=params)
        
    def get_trade_fee(self, symbol=None):
        """Get trade fee information"""
        url = self._endpoints.get('get_trade_fee')
        if not url:
            raise ValueError('Invalid endpoint for get_trade_fee')

        params = {}
        if symbol:
            params['symbol'] = symbol
            
        return self.send_request('GET', url, params=params)
        
    def get_asset_details(self, asset=None):
        """Get asset details"""
        url = self._endpoints.get('get_asset_details')
        if not url:
            raise ValueError('Invalid endpoint for get_asset_details')

        params = {}
        if asset:
            params['asset'] = asset
            
        return self.send_request('GET', url, params=params)


