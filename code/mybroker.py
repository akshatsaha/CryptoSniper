from typing import Optional
from BgxClient import BingXClient, BingXAPIError, BingXWebSocketClient


class Brokers:
    BingX = "BingX"
    DeltaEx = "DeltaEx"
    BitMEX = "BitMEX"
    ByBit = "ByBit"

    ## credentials required for each broker
    required_keys = {
        "BingX": ["api_key", "secret_key"],
        "DeltaEx": ["api_key", "secret_key"],
        "BitMEX": ["api_key", "secret_key"],
        "ByBit": ["api_key", "secret_key"]
    }



class MyBroker:
    
    def __init__(self, BrokerName: str, credentials: dict):
        # Check if broker name is valid
        broker_values = [getattr(Brokers, attr) for attr in dir(Brokers) 
                        if not attr.startswith('__') and not callable(getattr(Brokers, attr)) 
                        and not isinstance(getattr(Brokers, attr), dict)]
        
        if BrokerName not in broker_values:
            raise ValueError(f"Invalid BrokerName: {BrokerName}. Available brokers: {broker_values}")
        
        # Find the broker attribute name from its value
        broker_attr = next(attr for attr in dir(Brokers) 
                          if not attr.startswith('__') and not callable(getattr(Brokers, attr)) 
                          and getattr(Brokers, attr) == BrokerName)
        
        # Get required keys for this broker
        required_keys = Brokers.required_keys.get(broker_attr, [])
        
        # Check if all required keys are present in credentials
        if not all(key in credentials for key in required_keys):
            raise ValueError(f"Missing required keys in credentials for {BrokerName}: {required_keys}")

        self.credentials = credentials
        self.client = BingXClient(credentials["api_key"], credentials["secret_key"])
        self.ws_client = BingXWebSocketClient(credentials["api_key"], credentials["secret_key"])
        

    def get_balance(self, account_type:str):
        try:
            balance = self.client.get_account_balance(account_type)
            return balance
        except BingXAPIError as e:
            print(f"Error getting balance: {e}")
            return None

    def get_user_balance(self):
        try:
            user_balance = self.client.get_user_balance()
            return user_balance
        except BingXAPIError as e:
            print(f"Error getting user balance: {e}")
            return None


    def get_assets_balance(self):
        try:
            assets_balance = self.client.get_assets_balance()
            return assets_balance
        except BingXAPIError as e:
            print(f"Error getting assets balance: {e}")
            return None

    def get_positions(self, symbol:str):
        try:
            positions = self.client.get_positions(symbol)
            return positions
        except BingXAPIError as e:
            print(f"Error getting positions: {e}")
            return None

    def get_account_profit_loss(self, symbol:str):
        try:
            account_profit_loss = self.client.get_account_profit_loss(symbol)
            return account_profit_loss
        except BingXAPIError as e:
            print(f"Error getting account profit loss: {e}")
            return None

    def get_commission_rate(self, symbol:Optional[str] = None):
        try:
            if symbol is None:
                commission_rate = self.client.get_commission_rate()
            else:   
                commission_rate = self.client.get_commission_rate(symbol=symbol)
            return commission_rate
        except BingXAPIError as e:
            print(f"Error getting commission rate: {e}")
            return None



    def place_order(self, symbol:str, side:str, order_type:str, position_direction:str, quantity:float, price:float = None, clientOrderId:str=None, stopPrice=0, workingType=None):
        try:
            if order_type == "LIMIT":
                order = self.client.place_order(
                    symbol=symbol,
                    side=side,
                    order_type=order_type,
                    position_direction=position_direction,
                    quantity=quantity,
                    price=price,
                    clientOrderId=clientOrderId
                )
            elif order_type == "STOP":
                order = self.client.place_order(
                    symbol=symbol,
                    side=side,
                    order_type=order_type,
                    position_direction=position_direction,
                    quantity=quantity,
                    price=price,
                    stopPrice=stopPrice,
                    clientOrderId=clientOrderId
                )
                print(f"Order Inside my broker",order)
            elif order_type == "STOP_MARKET":
                # Add specific handling for STOP_MARKET orders
                order = self.client.place_order(
                    symbol=symbol,
                    side=side,
                    order_type=order_type,
                    position_direction=position_direction,
                    quantity=quantity,
                    price=price,
                    stopPrice=stopPrice,
                    workingType=workingType or "CONTRACT_PRICE",  # Use provided workingType or default to CONTRACT_PRICE
                    clientOrderId=clientOrderId
                )
                print(f"STOP_MARKET order placed: {order}")
            else:
                order = self.client.place_order(
                    symbol=symbol,
                    side=side,
                    order_type=order_type,
                    position_direction=position_direction,
                    quantity=quantity,
                    clientOrderId=clientOrderId
                )
                
            return order
        except BingXAPIError as e:
            print(f"Error placing order: {e}")
            return None


    def close_all_position(self,symbol:str=None):
        try:
            if symbol is None:
                close_all_position = self.client.close_all_position()
            else:
                close_all_position = self.client.close_all_position(symbol=symbol)
            return close_all_position
        except BingXAPIError as e:
            print(f"Error closing all position: {e}")
            return None



    def get_order_details(self,symbol:str, order_id:int):
        try:
            order_details = self.client.get_order_details(symbol,order_id)
            return order_details
        except BingXAPIError as e:
            print(f"Error getting order details: {e}")
            return None


    def get_all_open_orders(self,symbol:str):
        try:
            all_open_orders = self.client.get_all_open_orders(symbol=symbol)
            return all_open_orders
        except BingXAPIError as e:
            print(f"Error getting all open orders: {e}")
            return None


    def cancel_order(self,symbol:str,order_id:int):
        try:
            cancel_order = self.client.cancel_order(symbol=symbol,order_id=order_id)
            return cancel_order
        except BingXAPIError as e:
            print(f"Error canceling order: {e}")
            return None

    def get_leverage(self,symbol:str):
        try:
            leverage = self.client.get_leverage(symbol=symbol)
            return leverage
        except BingXAPIError as e:
            print(f"Error getting leverage: {e}")
            return None

    def set_leverage(self,symbol:str,leverage:int,positionSide:str):
        try:    
            set_leverage = self.client.set_leverage(symbol=symbol,leverage=leverage,positionSide=positionSide)
            return set_leverage
        except BingXAPIError as e:
            print(f"Error setting leverage: {e}")
            return None

    def get_historical_orders(self,symbol:str=None,startTime:int=None,endTime:int=None):
        try:
            historical_orders = self.client.get_historical_orders(symbol=symbol,startTime=startTime,endTime=endTime)
            return historical_orders
        except BingXAPIError as e:
            print(f"Error getting historical orders: {e}")
            return None




    def get_orderbook(self,symbol:str,limit:int=100):
        try:
            orderbook = self.client.get_orderbook(symbol=symbol, limit=limit)
            return orderbook
        except BingXAPIError as e:
            print(f"Error getting orderbook: {e}")
            return None

    def get_market_data(self,symbol:str):
        try:
            market_data = self.client.get_market_data(symbol=symbol)
            return market_data
        except BingXAPIError as e:
            print(f"Error getting market data: {e}")
            return None

    def get_quote_symbols(self,symbol:str):
        try:
            quote_symbols = self.client.get_quote_symbols(symbol=symbol)    
            return quote_symbols
        except BingXAPIError as e:
            print(f"Error getting quote symbols: {e}")
            return None

    def get_server_time(self):
        try:
            server_time = self.client.get_server_time()
            return server_time
        except BingXAPIError as e:
            print(f"Error getting server time: {e}")
            return None

    def test_order(self,symbol:str,side:str,order_type:str,quantity:float):
        try:
            test_order = self.client.test_order(
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=quantity 
            )
            return test_order
        except BingXAPIError as e:
            print(f"Error testing order: {e}")
            return None
            
    def get_broker_client(self):
        """Returns the broker client instance"""
        return self.client
        
    def get_broker_ws_client(self):
        """Returns the broker websocket client instance"""
        return self.ws_client

