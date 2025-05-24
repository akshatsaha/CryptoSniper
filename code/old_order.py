import threading
import traceback
import time
import copy
import os
from os import path
from datetime import datetime, timedelta, UTC
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
import decimal

# Encryption setup
SECRET_KEY = "hiu3#$*&64785#@$#$hniu5466ubgu5"
ALGORITHM = "HS256"
fernet_key = base64.urlsafe_b64encode(SECRET_KEY[:32].encode().ljust(32, b'0'))
fernet = Fernet(fernet_key)
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


# Collections will be initialized in main based on strategy

class User(pydantic.BaseModel):
    broker: str
    credentials: Dict[str, str]

class OrderData(pydantic.BaseModel):
    ID: int
    Symbol: str
    Side: str
    OrderType: str
    PositionType: str
    Quantity: float
    Price: float
    StrategyId: str
    Entry: bool = False
    Exit: bool = False
    StopLoss: bool = False
    stop_loss: bool = False  # For backward compatibility
    stop_price: float = 1.0  # Default value to avoid validation errors
    TriggerPrice: float = 1.0  # Default value to avoid validation errors

class MinQty:
    BTCUSDT = 0.0001
    ETHUSDT = 0.01
    SOLUSDT = 1
    


def decrypt_api_credentials(encrypted_key: str, encrypted_secret: str):
    """Decrypt API credentials using Fernet encryption"""
    
    return {
        "api_key": fernet.decrypt(encrypted_key).decode(),
        "secret_key": fernet.decrypt(encrypted_secret).decode()
    }

def get_broker(broker_name: str, credentials=dict) -> MyBroker:
    """Initialize and return a broker instance based on broker name"""
    if broker_name in [Brokers.BingX, Brokers.DeltaEx, Brokers.BitMEX, Brokers.ByBit]:
        # For production, uncomment the following to use encrypted credentials
        # cred = decrypt_api_credentials(encrypted_key=credentials['api_key'], encrypted_secret=credentials['secret_key'])
        
        # For testing, using hardcoded credentials (replace with actual credentials in production)
        API = "TsYIfUmV2aKpaa5XWJfvSrkF5kGvtCaQjONVLKl6JN5ELEa93g8JTjV2ThP6s9ewIMqKdTCmUCM1O2E2jyA"
        SECRET = "a8hWnhCbQ8EMtlOTwN6zXuKGeMjOSJqhaR4y9y8rhLqpkYaJplxLcYtkflxdBmlsz7YIA4KuQr3Ey5PKjw"

        cd = {
            "api_key": API,
            "secret_key": SECRET
        }
        return MyBroker(broker_name, cd)
    else:
        raise ValueError(f"Unknown broker: {broker_name}")

def get_users_credentials(user_list: list):
    """Get broker credentials for users from database"""
    response = []
    for user in user_list:
        email = user['email']
        broker_data = brokerCollection.find_one(
            {"userId": email, "is_active": True},
            {"_id": 0, "userId": 1, "broker_name": 1, "credentials": 1}
        )

        if broker_data is None:
            continue

        response.append(broker_data)

    return response

def calculate_quantity(symbol: str, execution_capital: float, leverage: float, price: float):
    """Calculate order quantity based on capital, leverage and price"""
    # Get minimum quantity for the symbol
    min_qty = getattr(MinQty, symbol, 0.001)
    
    # Calculate quantity
    quantity = execution_capital * leverage / price
    
    # Round to appropriate decimal places based on symbol
    if symbol == "BTCUSDT":
        quantity = round(decimal.Decimal(quantity), 4)
    elif symbol == "ETHUSDT":
        quantity = round(decimal.Decimal(quantity), 2)
    else:
        quantity = round(decimal.Decimal(quantity), 0)
    
    # Ensure minimum quantity
    if quantity < min_qty:
        quantity = min_qty
    
    return quantity

def place_order(user_broker: MyBroker, order_data: OrderData, client_order_id=None):
    """Place an order with the broker based on order data"""
    logger.info(f"Placing order: {order_data}")
    
    try:
        # Determine order parameters based on order type
        if order_data.StopLoss or order_data.stop_loss:
            # Stop loss order
            stop_price = order_data.stop_price or order_data.TriggerPrice
            if stop_price is None or stop_price <= 0:
                stop_price = 1.0  # Fallback default
                
            logger.info(f"Placing StopLoss order for {order_data.Symbol} at price {order_data.Price} with stop price {stop_price}")
            
            # For stop loss orders, add workingType parameter and ensure proper side/positionSide relationship
            order = user_broker.place_order(
                symbol=order_data.Symbol,
                side=order_data.Side,
                order_type="STOP_MARKET",  # Explicitly set to STOP_MARKET for stop loss
                position_direction=order_data.PositionType,
                quantity=order_data.Quantity,
                price=order_data.Price,
                stopPrice=stop_price,
                workingType="CONTRACT_PRICE",  # Added workingType parameter as per working example
                clientOrderId=client_order_id or f"SL_{order_data.ID}"
            )
        else:
            # Regular order (entry or exit)
            order = user_broker.place_order(
                symbol=order_data.Symbol,
                side=order_data.Side,
                order_type=order_data.OrderType,
                position_direction=order_data.PositionType,
                quantity=order_data.Quantity,
                price=order_data.Price,
                clientOrderId=client_order_id
            )
        
        if order:
            order_type = "Entry" if order_data.Entry else "Exit" if order_data.Exit else "StopLoss"
            logger.info(f"====== {order_type} Order ========")
            logger.info(order)
            order_id = order['data']['order']['orderId']
            logger.info(f"====== {order_type} Order END ========")
            return order_id, order
        
        return None, None
    except Exception as e:
        logger.error(f"Error placing order: {e}")
        logger.error(traceback.format_exc())
        
        # Log error to database
        errorCollection.insert_one({
            "timestamp": datetime.now(UTC),
            "order_data": order_data.dict(),
            "error": str(e),
            "traceback": traceback.format_exc(),
            "function": "place_order"
        })
        return None, None

def order_confirmation(user, user_broker, order_data, order_id):
    """Check order status and update database if filled"""
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            order = user_broker.get_order_details(
                symbol=order_data.Symbol,
                order_id=order_id,
            )
            
            if order and order['data']['order']:
                logger.info("====== Order Confirmation ========")
                logger.info(order)
                logger.info("====== Order Confirmation END ========")
                
                # If order is filled and it's an entry order, record trade details
                if order['data']['order']['status'] == 'FILLED' and order_data.Entry:
                    details = order['data']['order']
                    
                    # Insert trade details into client trades collection
                    try:
                        # Insert the trade details
                        clientTradeCollecion[user].insert_one({
                            "ID": order_data.ID,
                            "StrategyId": order_data.StrategyId,
                            'symbol': details['symbol'],
                            'orderId': details['orderId'],
                            'side': details['side'],
                            'positionSide': details['positionSide'],
                            'type': details['type'],
                            'origQty': details['origQty'],
                            'price': details['price'],
                            'executedQty': details['executedQty'],
                            'avgPrice': details['avgPrice'],
                            'cumQuote': details['cumQuote'],
                            'stopPrice': details['stopPrice'],
                            'profit': details['profit'],
                            'timestamp': datetime.now(UTC),
                            'status': 'FILLED'
                        })
                        logger.info(f"Trade details inserted into collection {user}")
                    except Exception as e:
                        logger.error(f"Error in order confirmation: '{user}'")
                        logger.error(traceback.format_exc())
                    
                    # Return order details
                    return order['data']['order']
                
                # If order is still pending, wait and retry
                elif order['data']['order']['status'] in ['NEW', 'PARTIALLY_FILLED']:
                    retry_count += 1
                    time.sleep(5)  # Wait 2 seconds before retrying
                    continue
                
                # If order has another status, return it
                return order['data']['order']
            
            # If no order data, retry
            retry_count += 1
            time.sleep(5)
            
        except Exception as e:
            logger.error(f"Error in order confirmation: {e}")
            logger.error(traceback.format_exc())
            
            # Log error to database
            errorCollection.insert_one({
                "timestamp": datetime.now(UTC),
                "order_data": order_data.dict(),
                "order_id": order_id,
                "error": str(e),
                "traceback": traceback.format_exc(),
                "function": "order_confirmation"
            })
            
            retry_count += 1
            time.sleep(2)
    
    # If we've exhausted retries, return None
    return None

def order_checker(user, user_broker, order_data, order_id, retry=0, max_retry=3):
    """Check order status and handle accordingly"""
    if retry >= max_retry:
        logger.warning(f"Max retries reached for order {order_id}")
        
        # Cancel the pending order
        cancel_result = user_broker.cancel_order(
            symbol=order_data.Symbol,
            order_id=order_id
        )
        
        if cancel_result and cancel_result['data']['order']['status'] == "CANCELLED":
            # Place market order instead
            market_order_data = copy.deepcopy(order_data)
            market_order_data.OrderType = "MARKET"
            
            new_order_id, new_order = place_order(user_broker, market_order_data)
            if new_order_id:
                return order_checker(user, user_broker, market_order_data, new_order_id, 0, max_retry)
                    
        return None
    

    try:
        order = user_broker.get_order_details(
            symbol=order_data.Symbol,
            order_id=order_id,
        )
        
        if not order or not order['data']['order']:
            # Retry if no order data
            time.sleep(5)
            return order_checker(user, user_broker, order_data, order_id, retry + 1, max_retry)
        
        status = order['data']['order']['status']
        
        # Handle different order statuses
        if status == 'FILLED':
            logger.info(f"Order {order_id} is filled")
            details = order['data']['order']
            try:
                # Insert the trade details
                clientTradeCollecion[user].insert_one({
                    "ID": order_data.ID,
                    "StrategyId": order_data.StrategyId,
                    'symbol': details['symbol'],
                    'orderId': details['orderId'],
                    'side': details['side'],
                    'positionSide': details['positionSide'],
                    'type': details['type'],
                    'origQty': details['origQty'],
                    'price': details['price'],
                    'executedQty': details['executedQty'],
                    'avgPrice': details['avgPrice'],
                    'cumQuote': details['cumQuote'],
                    'stopPrice': details['stopPrice'],
                    'profit': details['profit'],
                    'timestamp': datetime.now(UTC),
                    'status': 'FILLED'
                })
                logger.info(f"Trade details inserted into collection {user}")
            except Exception as e:
                logger.error(f"Error in order confirmation: '{user}'")
                logger.error(traceback.format_exc())


            ## update user in tradeColl
            ID = order_data.ID

            doc = {
                    "order_id": order_id,
                    "price": details['avgPrice'],
                    "status": "PLACED"
                }
            

            TradeCollection.update_one(
                {"ID": ID},
                {
                    "$set": {
                        "executed": True,
                        "execution_time": datetime.now(UTC),
                        f"Users.{user}": doc,
                        "Placed": True,
                        "Last_Checked": "All Placed"
                    }
                },
                upsert=True
            )

            return order['data']['order']
        
        elif status == 'PARTIALLY_FILLED':
            logger.info(f"Order {order_id} is partially filled")
            # Wait and check again
            time.sleep(5)
            return order_checker(user, user_broker, order_data, order_id, retry+1, max_retry)
        
        elif status == 'NEW':
            # For entry orders, check if it's been pending too long and convert to market if needed
            if order_data.Entry and (datetime.now(UTC) - datetime.fromtimestamp(order['data']['order']['time']/1000)).seconds > 10:
                logger.info(f"Order {order_id} pending too long, cancelling and placing market order")
                
                # Cancel the pending order
                cancel_result = user_broker.cancel_order(
                    symbol=order_data.Symbol,
                    order_id=order_id
                )
                
                if cancel_result and cancel_result['data']['order']['status'] == "CANCELLED":
                    # Place market order instead
                    market_order_data = copy.deepcopy(order_data)
                    market_order_data.OrderType = "MARKET"
                    
                    new_order_id, new_order = place_order(user_broker, market_order_data)
                    if new_order_id:
                        return order_checker(user, user_broker, market_order_data, new_order_id, 0, max_retry)
            
            # Otherwise, wait and check again
            time.sleep(5)
            return order_checker(user, user_broker, order_data, order_id, retry, max_retry)
        
        elif status in ['CANCELED', 'REJECTED', 'EXPIRED']:
            logger.warning(f"Order {order_id} has status {status}")
            return order['data']['order']
        
        else:
            logger.info(f"Order {order_id} has status {status}")
            time.sleep(5)
            return order_checker(user, user_broker, order_data, order_id, retry+1, max_retry)
    
    except Exception as e:
        logger.error(f"Error checking order {order_id}: {e}")
        logger.error(traceback.format_exc())
        
        # Log error to database
        errorCollection.insert_one({
            "timestamp": datetime.now(UTC),
            "order_data": order_data.dict(),
            "order_id": order_id,
            "error": str(e),
            "traceback": traceback.format_exc(),
            "function": "order_checker"
        })
        
        # Retry
        time.sleep(2)
        return order_checker(user, user_broker, order_data, order_id, retry + 1, max_retry)

def execute_trade(trade_dict: dict):
    """Main function to execute a trade based on the provided trade dictionary"""
    try:
        # Extract trade details from FRC_SPT_STRA.py format
        symbol = trade_dict.get('Symbol', trade_dict.get('symbol'))
        side = trade_dict.get('Side', trade_dict.get('side'))
        position_type = 'LONG' if side == 'BUY' else 'SHORT'  # Derive from side if not provided
        order_type = trade_dict.get('OrderType', trade_dict.get('order_type', 'LIMIT'))
        price = trade_dict.get('Price', trade_dict.get('price', trade_dict.get('EntryPrice')))
        strategy_id = trade_dict.get('StrategyId', trade_dict.get('strategy_id', STRATEGY))
        trade_id = trade_dict.get('ID', trade_dict.get('trade_id', 0))
        quantity = trade_dict.get('Qty', trade_dict.get('quantity', 0))
        
        # Determine order type
        is_entry = trade_dict.get('Entry', trade_dict.get('is_entry', False))
        is_exit = trade_dict.get('Exit', trade_dict.get('is_exit', False))
        is_stop_loss = trade_dict.get('StopLoss', trade_dict.get('is_stop_loss', False))
        stop_price = trade_dict.get('Price')
        
        # If this is a stop loss order but no stop price is provided, calculate it
        if is_stop_loss and not stop_price:
            # Try to get stop price from StopLoss field if it's a number
            if isinstance(trade_dict.get('StopLoss'), (int, float)):
                stop_price = trade_dict.get('StopLoss')
            else:
                # Default to 1% below/above the entry price as a fallback 171.2 
                direction_multiplier = -1 if side == 'BUY' else 1
                stop_price = price + round((0.05 +  0.01 * direction_multiplier),2)
        
        

        # Validate required fields
        if not all([symbol, side, price, trade_id]):
            print(f"Missing required trade parameters: {trade_dict}")
            return {"success": False, "error": "Missing required trade parameters"}
        
        # Get user credentials
        user_credentials = get_users_credentials(Users)
        if not user_credentials:
            print(f"No valid broker credentials found for strategy {strategy_id}")
            return {"success": False, "error": "No valid broker credentials found"}
        
        # Track successful orders
        successful_orders = []
        failed_orders = []
        
        # Create threads for parallel order execution
        threads = []
        
        # Execute trade for each user
        for user_cred in user_credentials:
            try:
                # Get user ID
                user_id = user_cred['userId']
                
                # Get broker
                broker_name = user_cred['broker_name']
                user_broker = get_broker(broker_name, user_cred['credentials'])
                
                # If quantity is not provided, calculate it based on user's config
                if quantity <= 0:
                    # Get user's trading config from the Users list
                    user_config = next((u for u in Users if u.get('google_id') == user_id), None)
                    if user_config and USTRA in user_config.get('strategies', {}):
                        execution_capital = user_config['strategies'][USTRA].get('executionCapital', 100)
                        leverage = user_config['strategies'][USTRA].get('leverage', 10)
                        quantity = calculate_quantity(symbol, execution_capital, leverage, price)
                    else:
                        # Default values if user config not found
                        quantity = calculate_quantity(symbol, 100, 10, price)
                
                # Ensure stop_price and TriggerPrice are valid numbers

                if is_stop_loss or is_exit:
                    # For stop loss orders, calculate a reasonable stop price if not provided
                    direction_multiplier = -1 if side == 'BUY' else 1
                    stop_price = price + round((0.05 +  0.01 * direction_multiplier),2)
                    stop_price_t = stop_price
                    position_type = 'LONG' if side == 'SELL' else 'SHORT'

                else:
                    direction_multiplier = -1 if side == 'BUY' else 1
                    stop_price_t = stop_price + round((0.05 +  0.01 * direction_multiplier),2)
                    

                # if is_exit:
                #     position_type = 'LONG' if side == 'SELL' else 'SHORT'


                # Create order data
                order_data = OrderData(
                    ID=trade_id,
                    Symbol=symbol,
                    Side=side,
                    OrderType=order_type,
                    PositionType=position_type,
                    Quantity=quantity,
                    Price=price,
                    StrategyId=strategy_id,
                    Entry=is_entry,
                    Exit=is_exit,
                    StopLoss=is_stop_loss,
                    stop_price=stop_price,
                    TriggerPrice=stop_price_t
                )
                
                # Create a thread for this user's order
                thread = threading.Thread(
                    target=place_order_for_user,
                    args=(user_id, user_broker, order_data, successful_orders, failed_orders),
                    name=f"OrderExecution_{user_id}_{trade_id}"
                )
                thread.daemon = True
                threads.append(thread)
            
            except Exception as e:
                print(f"Error preparing trade for user {user_cred.get('userId')}: {e}")
                print(traceback.format_exc())
                
                failed_orders.append({
                    "user_id": user_cred.get('userId'),
                    "error": str(e)
                })
        
        # Start all threads
        for thread in threads:
            thread.start()
            # Small delay to prevent API rate limits
            time.sleep(0.1)
        
        # Wait for all threads to complete (optional - can make this async)
        for thread in threads:
            thread.join(timeout=10)  # Wait max 10 seconds per thread
        
        # Update trade record in database with user results order_details"price": details['avgPrice'],
        users_dict = {}
        for order in successful_orders:
            try:
                users_dict[order['user_id']] = {"order_id": order['order_id'],"price":order['price'], "status": order['status']}
            except Exception as e:
                print("order",order)
                users_dict[order['user_id']] = {"order_id": order['order_id'],"price":price, "status": order['status']}

        TradeCollection.update_one(
            {"ID": trade_id},
            {
                "$set": {
                    "executed": True,
                    "execution_time": datetime.now(UTC),
                    "Users": users_dict,
                    "Placed": True,
                    "Last_Checked": "All Placed" if len(successful_orders) == len(user_credentials) else "Partially Placed"
                }
            },
            upsert=True
        )
        
        return {
            "success": True,
            "successful_orders": len(successful_orders),
            "failed_orders": len(failed_orders)
        }
    
    except Exception as e:
        print(f"Error in execute_trade: {e}")
        print(traceback.format_exc())
        
        # Log error to database
        errorCollection.insert_one({
            "timestamp": datetime.now(UTC),
            "trade_dict": trade_dict,
            "error": str(e),
            "traceback": traceback.format_exc(),
            "function": "execute_trade"
        })
        
        return {"success": False, "error": str(e)}

def place_order_for_user(user_id, user_broker, order_data, successful_orders, failed_orders):
    """Execute an order for a specific user (to be run in a thread)"""
    try:
        # Place the order
        order_id, order = place_order(user_broker, order_data)
        
        if order_id:
            # Order placed successfully
            print(f"Order placed successfully for user {user_id}: {order_id}")
            
            # If it's an entry order, check for confirmation
            if order_data.Entry:
                order_details = order_confirmation(user_id, user_broker, order_data, order_id)
                if order_details and order_details.get('status') == 'FILLED':
                    successful_orders.append({
                        "user_id": user_id,
                        "order_id": order_id,
                        "price": order_details['avgPrice'],
                        "status": "FILLED"
                    })
                else:
                    # Start a thread to check order status
                    thread = threading.Thread(
                        target=order_checker,
                        args=(user_id, user_broker, order_data, order_id),
                        name=f"OrderChecker_{order_id}"
                    )
                    thread.daemon = True
                    thread.start()
                    
                    successful_orders.append({
                        "user_id": user_id,
                        "order_id": order_id,
                        "status": "PENDING"
                    })
            else:
                # For exit and stop loss orders, just record the order
                successful_orders.append({
                    "user_id": user_id,
                    "order_id": order_id,
                    "status": "PLACED"
                })
        else:
            # Order placement failed
            print(f"Failed to place order for user {user_id}")
            failed_orders.append({
                "user_id": user_id,
                "error": "Failed to place order"
            })
    
    except Exception as e:
        print(f"Error executing trade for user {user_id}: {e}")
        print(traceback.format_exc())
        
        failed_orders.append({
            "user_id": user_id,
            "error": str(e)
        })
        
    # Update the global client trade collection for this user if not already initialized
    try:
        if user_id not in clientTradeCollecion.list_collection_names():
            # Create the collection if it doesn't exist
            logger.info(f"Creating collection {user_id} in ClientTrades database")
        # No need to store in a dictionary anymore
        logger.info(f"Using collection {user_id} for user {user_id}")
    except Exception as e:
        logger.error(f"Error initializing client trade collection for user {user_id}: {e}")
        logger.error(traceback.format_exc())

def cancel_order(order_data):
    """Cancel an existing order"""
    try:
        # Extract order details
        symbol = order_data.get('symbol')
        order_id = order_data.get('order_id')
        user_id = order_data.get('user_id')
        
        # Validate required fields
        if not all([symbol, order_id, user_id]):
            logger.error(f"Missing required parameters for cancellation: {order_data}")
            return {"success": False, "error": "Missing required parameters"}
        
        # Get user credentials
        user_cred = brokerCollection.find_one({"userId": user_id, "is_active": True})
        if not user_cred:
            logger.warning(f"No valid broker credentials found for user {user_id}")
            return {"success": False, "error": "No valid broker credentials found"}
        
        # Get broker
        broker_name = user_cred['broker_name']
        user_broker = get_broker(broker_name, user_cred['credentials'])
        
        # Cancel the order
        result = user_broker.cancel_order(
            symbol=symbol,
            order_id=order_id
        )
        
        if result and result['data']['success']:
            logger.info(f"Order {order_id} cancelled successfully")
            
            # Update order status in database
            clientTradeCollecion[user_id].update_one(
                {"orderId": order_id},
                {"$set": {"status": "CANCELED", "cancel_time": datetime.now(UTC)}}
            )
            
            return {"success": True}
        else:
            logger.error(f"Failed to cancel order {order_id}: {result}")
            return {"success": False, "error": "Failed to cancel order"}
    
    except Exception as e:
        logger.error(f"Error in cancel_order: {e}")
        logger.error(traceback.format_exc())
        
        # Log error to database
        errorCollection.insert_one({
            "timestamp": datetime.now(UTC),
            "order_data": order_data,
            "error": str(e),
            "traceback": traceback.format_exc(),
            "function": "cancel_order"
        })
        
        return {"success": False, "error": str(e)}

def execute_market_order(order_data):
    """Execute a market order immediately"""
    try:
        # Extract order details
        symbol = order_data.get('symbol')
        side = order_data.get('side')
        position_type = order_data.get('position_type')
        quantity = order_data.get('quantity')
        strategy_id = order_data.get('strategy_id')
        trade_id = order_data.get('trade_id')
        is_entry = order_data.get('is_entry', False)
        is_exit = order_data.get('is_exit', False)
        user_id = order_data.get('user_id')
        
        # Validate required fields
        if not all([symbol, side, position_type, quantity, strategy_id, trade_id, user_id]):
            logger.error(f"Missing required parameters for market order: {order_data}")
            return {"success": False, "error": "Missing required parameters"}
        
        # Get user credentials
        user_cred = brokerCollection.find_one({"userId": user_id, "is_active": True})
        if not user_cred:
            logger.warning(f"No valid broker credentials found for user {user_id}")
            return {"success": False, "error": "No valid broker credentials found"}
        
        # Get broker
        broker_name = user_cred['broker_name']
        user_broker = get_broker(broker_name, user_cred['credentials'])
        
        # Create order data
        order_data_obj = OrderData(
            ID=trade_id,
            Symbol=symbol,
            Side=side,
            OrderType="MARKET",
            PositionType=position_type,
            Quantity=quantity,
            Price=0,  # Not used for market orders
            StrategyId=strategy_id,
            Entry=is_entry,
            Exit=is_exit,
            StopLoss=False
        )
        
        # Place the market order
        order_id, order = place_order(user_broker, order_data_obj)
        
        if order_id:
            logger.info(f"Market order placed successfully: {order_id}")
            
            # Update trade record in database
            TradeCollection.update_one(
                {"ID": trade_id},
                {
                    "$set": {
                        "executed": True,
                        "execution_time": datetime.now(UTC),
                        "order_id": order_id,
                        "order_type": "MARKET"
                    }
                },
                upsert=True
            )
            
            return {"success": True, "order_id": order_id}
        else:
            logger.error(f"Failed to place market order")
            return {"success": False, "error": "Failed to place market order"}
    
    except Exception as e:
        logger.error(f"Error in execute_market_order: {e}")
        logger.error(traceback.format_exc())
        
        # Log error to database
        errorCollection.insert_one({
            "timestamp": datetime.now(UTC),
            "order_data": order_data,
            "error": str(e),
            "traceback": traceback.format_exc(),
            "function": "execute_market_order"
        })
        
        return {"success": False, "error": str(e)}

# Define function lists for different operations
cancel_functions = [cancel_order]
execution_functions = [execute_market_order]

def function_roulette(document: dict, functions: List[Callable]):
    """Execute a list of functions on a document"""
    for func in functions:
        try:
            result = func(document)
            if result and result.get('success'):
                return result
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}")
            logger.error(traceback.format_exc())
    
    return {"success": False, "error": "All functions failed"}

def order_runner(document: dict):
    """Process an order document and execute appropriate functions"""
    try:
        
        if document.get('Entry', False) is True:
            print(f"Processing entry order: {document}")
            return execute_trade(document)
            
        elif document.get('StopLoss', False) is True:
            print(f"Processing stop loss order: {document}")
            document['is_stop_loss'] = True
            return execute_trade(document)
            
        elif document.get('Exit', False) is True:
            print(f"Processing exit order: {document}")
            document['is_exit'] = True
            return execute_trade(document)
            
        # elif document.get('type') == 'cancel':
        #     return function_roulette(document, cancel_functions)
            
        # elif document.get('type') == 'execute':
        #     return function_roulette(document, execution_functions)
            
        # elif document.get('type') == 'trade':
        #     return execute_trade(document)
            
        else:
            print(f"Processing as generic trade document: {document}")
            return execute_trade(document)
    
    except Exception as e:
        print(f"Error in order_runner: {e}")
        print(traceback.format_exc())
        
        # Log error to database
        errorCollection.insert_one({
            "timestamp": datetime.now(UTC),
            "document": document,
            "error": str(e),
            "traceback": traceback.format_exc(),
            "function": "order_runner"
        })
        
        return {"success": False, "error": str(e)}

# Main execution code
if __name__ == "__main__":
    print("Starting order execution system")
    StrId = {
    "1001":"FRC_SPT_BTC",
    "1002":"FRC_SPT_ETH",
    "1003":"FRC_SPT_SOL",
    "2001":"EMA_CROSS_BTC",
    "2002":"EMA_CROSS_ETH",
    "2003":"EMA_CROSS_SOL"
    }
    
    STRATEGY = str(argv[1])

    if  "BTC"  in STRATEGY or "Bit" in STRATEGY:
        SYMBOL = 'BTC-USDT'
        perTradLoss = 0.01
    elif "ETH" in STRATEGY:
        SYMBOL = 'ETH-USDT'
        perTradLoss = 0.1
    elif "SOL" in STRATEGY:
        SYMBOL = 'SOL-USDT'
        perTradLoss = 1
    else:
        print("Symbol not allow" )

    
    # Initialize collections based on strategy  Crypto Scalper
    
    USTRA = f"{STRATEGY.replace("_"," ")}"
    print(USTRA)

    # Set up database collections
    link = "mongodb+srv://vipinpal7060:lRKAbH2D7W18LMZd@cluster0.fg30pmw.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    mongo_client = pymongo.MongoClient(link)
    TradeCollection = mongo_client[STRATEGY]["Trades"]
    PositionCollection = mongo_client[STRATEGY]["Position"]
    LiveCollection = mongo_client[STRATEGY]["LiveUpdate"]
    UserCollection = mongo_client['cryptosnipers']['users']
    brokerCollection = mongo_client['cryptosnipers']['broker_connections']
    errorCollection = mongo_client['cryptosnipers']['errors']
    strategiesCollection = mongo_client['cryptosnipers']['Strategies']
    candleDb = mongo_client["Candles"]
    COLL = SYMBOL.replace("-","")
    candles = candleDb[COLL]
    clientTradeCollecion = mongo_client['ClientTrades']
    # Setup logging
    current_file = str(os.path.basename(__file__)).replace('.py','')
    folder = file_path_locator()
    logs_dir = path.join(path.normpath(folder), "logs")
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)

    LOG_file = F"{logs_dir}/{STRATEGY}_order.log"
    
    print("USTRA",USTRA)


    logger = setup_logger(name=current_file,log_to_file=True,log_to_console=True,log_file=LOG_file,capture_print=True,capture_other_loggers=True)

    print(LOG_file)

    
    # Get users for this strategy
    # Users = list(UserCollection.find({"status":"approved",f"strategies.{USTRA}.active":True},{'_id':0,"name": 1, "email": 1, f"strategies.{USTRA}": 1}))
    
    Users = list(UserCollection.find({"status": "approved","strategies": USTRA,"is_active": True},
    {
        '_id': 0,
        "name": 1,
        "email": 1,
        "strategies": 1
    }
    ))

    print("Users", Users)
    user_credentails = get_users_credentials(Users)

    print("user_credentails", user_credentails)
    

    # Start watching for trade documents in MongoDB
    while True:
        try:
            with TradeCollection.watch(full_document="updateLookup") as stream:
                for change in stream:
                    if change['operationType'] == 'insert':
                        print("========================")
                        print("INSIDE INSERT CONDITION")
                        print(change)
                        print("========================")
                        
                        # Process the new trade document
                        order_runner(change['fullDocument'])
                        
                        # Update status in database
                        TradeCollection.update_one({"_id":change['fullDocument']['_id']}, {"$set": {'Placed':"Order_Checker"}})
                    
                    if change['operationType'] == 'update':
                        # Handle updates if needed
                        print("========================")
                        print("INSIDE UPDATE CONDITION")
                        print(change)
                        print("========================")
                        
                        # Check if this is a cancellation or execution request
                        if change.get('fullDocument', {}).get('type') == 'cancel':
                            cancel_order(change['fullDocument'])
                        elif change.get('fullDocument', {}).get('type') == 'execute':
                            execute_market_order(change['fullDocument'])
        
        except Exception as e:
            print(f"Error in MongoDB change stream: {e}")
            print(traceback.format_exc())
            time.sleep(5)  # Wait before reconnecting