import os
import traceback
import warnings
import time
from datetime import datetime, timedelta, timezone
import sys
import pytz
from Utils import *
from dotenv import load_dotenv
import numpy as np
import pandas as pd
import pymongo
from sys import argv
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.filterwarnings("ignore")



def liveUpdate():
    up = LiveCollection.find_one({'ID':0})
    if up is None:
        logger.info("No live update found, fetching historical data")
        setup_dict = {'ID':0,'PREV FRACTAL': 0}
        LiveCollection.insert_one(setup_dict)
        return setup_dict if up is None else up

    return up


def ATR(df, atr_period):
    hl = pd.Series(df["high"] - df["low"]).abs()
    hc = pd.Series(df["high"] - df["close"].shift()).abs()
    cl = pd.Series(df["close"].shift() - df["low"]).abs()
    hcl= pd.concat([hl,hc,cl], axis = 1)
    tr = hcl.max(axis = 1)
    
    # Calculate and return the ATR values
    return tr.ewm(alpha=1/atr_period, min_periods=atr_period).mean().round(2)


def detect_fractals(df, consecutive=5):

    """Detect fractal patterns in price data"""
    highs = df['high'].values
    lows = df['low'].values
    date = df['date'].values

    lookback = (consecutive - 1) // 2
    n = (consecutive // 2) + 1
    fractal_top = np.full(len(highs), np.nan)
    fractal_bottom = np.full(len(highs), np.nan)

    # Initialize with explicit datetime64[ns] dtype
    fractal_time_top = np.full(len(highs), np.datetime64('NaT'), dtype='datetime64[ns]')
    fractal_time_bottom = np.full(len(highs), np.datetime64('NaT'), dtype='datetime64[ns]')

    for i in range(lookback, len(highs) - lookback):
        if (i + n) >= len(date):
            continue

        is_fractal_top = True
        is_fractal_bottom = True
        for j in range(1, lookback + 1):
            if highs[i] <= highs[i - j] or highs[i] <= highs[i + j]:
                is_fractal_top = False
            if lows[i] >= lows[i - j] or lows[i] >= lows[i + j]:
                is_fractal_bottom = False

        if is_fractal_top:
            fractal_top[i] = highs[i]
            # Convert to numpy datetime64 explicitly
            fractal_time_top[i] = pd.to_datetime(date[i + n]).to_datetime64()
        if is_fractal_bottom:
            fractal_bottom[i] = lows[i]
            fractal_time_bottom[i] = pd.to_datetime(date[i + n]).to_datetime64()

    df['fractal_top'] = fractal_top
    df['fractal_bottom'] = fractal_bottom
    df['fractal_time_top'] = fractal_time_top
    df['fractal_time_bottom'] = fractal_time_bottom

    df['fractal_top'] = df['fractal_top'].shift(2)
    df['fractal_bottom'] = df['fractal_bottom'].shift(2)
    df['fractal_time_top'] = df['fractal_time_top'].shift(2)
    df['fractal_time_bottom'] = df['fractal_time_bottom'].shift(2)

    df['fractal_bottom'].ffill(inplace=True)
    df['fractal_top'].ffill(inplace=True)
    df['fractal_time_top'].ffill(inplace=True)
    df['fractal_time_bottom'].ffill(inplace=True)


    return df

def fetch_historical_data(timeframe): 
    try:
        # Get last complete candle time
        now = datetime.now(tz=pytz.utc)
        rounded = now.replace(second=0, microsecond=0, minute=(now.minute // TF) * TF)
        last_complete = rounded - timedelta(minutes=TF)
        candleData = list(mycandle.find({}, {"_id": 0}).sort("timestamp", pymongo.DESCENDING).limit(40000))
        
        candleDf = pd.DataFrame(candleData)
        candleDf = candleDf[['timestamp', 'date', 'open', 'high', 'low', 'close', 'volume']]
        candleDf = candleDf.astype({
            'timestamp': 'int64',
            'open': 'float64',
            'high': 'float64',
            'low': 'float64',
            'close': 'float64',
            'volume': 'float64',
        })

        candleDf['date'] = pd.to_datetime(candleDf['date'], errors='coerce').dt.tz_localize('UTC')
        df = cand_conv(timeframe,candleDf)
        df.reset_index(inplace=True)

        # Filter only completed candles
        df = df[df['date'] <= last_complete]

        return df
    

    except Exception as e:
        logger.error(f"Error fetching historical data: {str(e)}")
        logger.error(traceback.format_exc())
        return None


def cand_conv2(timeframe, df, z=False):
    if df.empty:
        return df
        
    last_date = df.loc[df.index[-1], 'date']
    total_minutes = last_date.hour * 60 + last_date.minute
    delete_row = False
    
    # Convert timeframe to integer if it's a string
    original_timeframe = timeframe
    if isinstance(timeframe, str):
        try:
            timeframe = int(timeframe.replace('min', ''))
        except ValueError:
            logger.error(f"Invalid timeframe format: {timeframe}")
            raise
    
    if z:
        if total_minutes % timeframe == 0:
            df = df[:-1]
            delete_row = False
        elif (total_minutes + 1) % timeframe == 0:
            delete_row = False
        elif total_minutes + timeframe > 1440:
            delete_row = False
        else:
            delete_row = True

    ohlc_dict = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }
    
    # Convert to pandas datetime if not already
    if not pd.api.types.is_datetime64_any_dtype(df['date']):
        df['date'] = pd.to_datetime(df['date'])
    
    # Set date as index for resampling
    df = df.set_index('date')
    
    # Use timeframe for resampling (already converted to int if it was a string)
    try:
        df = df.resample(f'{timeframe}T').apply(ohlc_dict)
    except Exception as e:
        logger.error(f"Error in resampling: {str(e)}")
        logger.error(f"timeframe value: {timeframe}, type: {type(timeframe)}")
        raise
    
    # Reset index to make date a column again
    df = df.reset_index()
    
    if delete_row and z:
        df = df[:-1]
    
    return df


def cand_conv(timeframe, df):
    if df.empty:
        return df
        
    # Ensure date is in datetime format
    if not pd.api.types.is_datetime64_any_dtype(df['date']):
        df['date'] = pd.to_datetime(df['date'])
    
    # Sort by date to ensure proper processing
    df = df.sort_values('date').reset_index(drop=True)
    
    # Find all midnight timestamps
    c = list(df.loc[(df['date'].dt.hour == 0) & (df['date'].dt.minute == 0)].index)
    cd = pd.DataFrame()
    
    # Handle different cases based on where the day boundaries are
    if not c or (len(c) == 1 and c[0] == 0):
        x = cand_conv2(timeframe, df, z=True)
        if not x.empty:
            cd = pd.concat([cd, x], ignore_index=True)
            if not cd.empty:
                cd = cd.iloc[1:].reset_index(drop=True)
        return cd
    
    elif len(c) == 1 and c[0] != 0:
        x1 = cand_conv2(timeframe, df[:c[0]])
        x2 = cand_conv2(timeframe, df[c[0]:], z=True)
        cd = pd.concat([x1[1:], x2], ignore_index=True)
        return cd
    
    elif c[0] == 0:
        c = c[1:]
    
    # Process data between day boundaries
    for i in range(len(c)):
        if i == 0:
            x = cand_conv2(timeframe, df[:c[i]])
        elif i == len(c) - 1:
            x1 = cand_conv2(timeframe, df[c[i-1]:c[i]])
            x2 = cand_conv2(timeframe, df[c[i]:], z=True)
            cd = pd.concat([cd, x1, x2], ignore_index=True)
            continue
        else:
            x = cand_conv2(timeframe, df[c[i-1]:c[i]])
        
        if not x.empty:
            cd = pd.concat([cd, x], ignore_index=True)
    
    # Clean up the result
    if not cd.empty:
        cd = cd.iloc[1:].reset_index(drop=True)
    
    return cd


def SuperTrend(df, atr_period, st_factor) :

    y = df[['high', 'low', 'close']].copy()

    hl = pd.Series(y["high"] - y["low"]).abs()
    hc = pd.Series(y["high"] - y["close"].shift()).abs()
    cl = pd.Series(y["close"].shift() - y["low"]).abs()
    hcl = pd.concat([hl, hc, cl], axis=1)
    tr = hcl.max(axis=1)

    y['hl2'] = (y['high'] + y['low']) / 2
    y['atr'] = tr.ewm(alpha=1 / atr_period, min_periods=atr_period).mean().round(7).fillna(0)
    y['UB'] = y['hl2'] + st_factor * y['atr']
    y['LB'] = y['hl2'] - st_factor * y['atr']

    y['direction'] = 0
    y['supertrend'] = 0.0

    np_dtype = np.dtype({'names': y.dtypes.keys(), 'formats': y.dtypes.values})
    x = np.ones(len(y), dtype=np_dtype)

    for i in y.dtypes.keys():
        x[i] = y[i].to_numpy()

    prevsupertrend = x['UB'][0]
    prevlowerband = x['LB'][0]
    prevupperband = x['UB'][0]

    for i in range(1,len(x)):
        if x['LB'][i] > prevlowerband or x['close'][i - 1] < prevlowerband:
            pass
        else:
            x['LB'][i] = prevlowerband
        if x['UB'][i] < prevupperband or x['close'][i - 1] > prevupperband:
            pass
        else:
            x['UB'][i] = prevupperband
        if x['atr'][i - 1] == 0:
            x['direction'][i] = 1
        elif prevsupertrend == prevupperband:
            if x['close'][i] > x['UB'][i]:
                x['direction'][i] = -1
            else:
                x['direction'][i] = 1
        else:
            if x['close'][i] < x['LB'][i]:
                x['direction'][i] = 1
            else:
                x['direction'][i] = -1
        if x['direction'][i] == -1:
            prevsupertrend = x['LB'][i]
        else:
            prevsupertrend = x['UB'][i]
        prevlowerband = x['LB'][i]
        prevupperband = x['UB'][i]

    df['Direction'] = x['direction']

def analyze_market_data(df, ATR_PERIOD=14, FRACTAL_PERIOD=5, ST_FACTOR=3):
    df = df.copy()
    
    df['atr'] = ATR(df, ATR_PERIOD)
    
    df = detect_fractals(df, consecutive=FRACTAL_PERIOD)
    
    SuperTrend(df, ATR_PERIOD, ST_FACTOR)
    
    fractal_cols = ['fractal_top', 'fractal_bottom', 
                   'fractal_time_top', 'fractal_time_bottom']
    df[fractal_cols] = df[fractal_cols].ffill()
    
    return df

def check_for_entry_signals(df,trade_dict):
    
    open_positions = list(PositionCollection.find({"Status": "Open"}))
    if len(open_positions) > 0:
        logger.info("Open positions exist, skipping entry signal generation")
        return None

    latest = df.iloc[-1]
    prev = df.iloc[-2]
    prev_frac = trade_dict['PREV FRACTAL']

    ## 1min candle
    ticker = ohlc.find_one({"ID":0})
    current_price = ticker['close']

    # Check for buy signal (price breaks above fractal top)
    buy_signal = False
    sell_signal = False
    
    # Find the most recent valid fractal top and bottom
    recent_top = None
    recent_bottom = None
    
    # Look for the most recent fractal top
    for i in range(len(df)-1, 0, -1):
        if not np.isnan(df.iloc[i]['fractal_top']):
            recent_top = df.iloc[i]
            break
    
    # Look for the most recent fractal bottom
    for i in range(len(df)-1, 0, -1):
        if not np.isnan(df.iloc[i]['fractal_bottom']):
            recent_bottom = df.iloc[i]
            break
    
    # No signals if we don't have recent fractals
    if recent_top is None or recent_bottom is None:
        return None
    

    direction = latest.get('Direction', None)
    if direction is None:
        logger.warning("SuperTrend direction not found in dataframe, cannot determine trend direction")
        return None
    
    fractal_top = recent_top['fractal_top']
    fractal_bottom = recent_bottom['fractal_bottom']
    latest_time = pd.to_datetime(ticker['date'], utc=True)
    fractal_time_top = pd.to_datetime(recent_top['fractal_time_top'], utc=True)    
    fractal_time_bottom = pd.to_datetime(recent_bottom['fractal_time_bottom'], utc=True) 

    # Check for buy signal - price breaks above recent fractal top and SuperTrend direction is -1 (bullish)
    if (ticker['high'] > recent_top['fractal_top'] and 
        latest_time > fractal_time_top and 
        prev_frac != recent_top['fractal_top'] and 
        direction == -1):
        
        logger.info(f"Latest candle: {latest.to_dict()}")
        logger.info(f"Previous candle: {prev.to_dict()}")
        logger.info(f"Ticker: {ticker}")    
        
        entry_price = ticker['close']
        atr_sl = entry_price - (SL_FACTOR * latest['atr'])
        take_profit = entry_price + (TP_FACTOR * latest['atr'])

                
        qty = (CAPITAL * LEVERAGE) / entry_price
        sell_bid = latest['low'] - .1*prev['atr']
        sell_ep = 1 / sell_bid
        MaxSL = entry_price - (1 / (sell_ep - perTradLoss / (qty * -CAPITAL))).round(2)

        stop_loss = max(atr_sl, MaxSL)
        
        return {
            'Signal': 'BUY',
            'EntryPrice': float(entry_price),
            "atr_sl":float(atr_sl),
            'StopLoss': float(stop_loss),
            "MaxSL":float(MaxSL),
            'Target': float(take_profit),
            'Atr': float(latest['atr']),
            'FractalPrice': float(recent_top['fractal_top']),
            'FractalTime': fractal_time_top,
            'EntryTime': latest_time,
            'Direction': int(direction)
        }
        
    # Check for sell signal - price breaks below recent fractal bottom and SuperTrend direction is 1 (bearish)
    if (ticker['low'] < recent_bottom['fractal_bottom'] and 
        latest_time > fractal_time_bottom and 
        prev_frac != recent_bottom['fractal_bottom'] and 
        direction == 1):

        logger.info(f"Latest candle: {latest.to_dict()}")
        logger.info(f"Previous candle: {prev.to_dict()}")
        logger.info(f"Ticker: {ticker}")
        
        entry_price = ticker['close']
        atr_sl = entry_price + (SL_FACTOR * latest['atr'])
        take_profit = entry_price - (TP_FACTOR * latest['atr'])
        
        qty = (CAPITAL * LEVERAGE) / entry_price
        sell_bid = latest['low'] - .1*prev['atr']
        sell_ep = 1 / sell_bid
        MaxSL = entry_price + (1 / (sell_ep - perTradLoss / (qty * -CAPITAL))).round(2)

        stop_loss = min(atr_sl, MaxSL)

        return {
            'Signal': 'SELL',
            'EntryPrice': float(entry_price),
            "atr_sl":float(atr_sl),
            'StopLoss': float(stop_loss),
            "MaxSL":float(MaxSL),
            'Target': float(take_profit),
            'Atr': float(latest['atr']),
            'FractalPrice': float(recent_top['fractal_bottom']),
            'FractalTime': fractal_time_bottom, ##FratalTime
            'EntryTime': latest_time, 
            'Direction': int(direction)
        }
    
    return None

def execute_trade(signal):
    try:
        # Calculate position size
        qty = (CAPITAL * LEVERAGE) / signal['EntryPrice']
        global POSITION_ID
        if "SOL" in SYMBOL:
            qty = int(qty)

        POSITION_ID = int(time.perf_counter_ns()) if POSITION_ID == 0 else POSITION_ID
        sl_id =  int(time.perf_counter_ns())
        # Convert numpy and pandas types to native Python types
        position_doc = {
            'ID': POSITION_ID,
            "SL_ID":sl_id,
            'Symbol': SYMBOL,
            'Side': str(signal['Signal']),
            "Condition":"Executed",
            'EntryPrice': float(signal['EntryPrice']),
            'Qty': round(qty,3),
            'StopLoss': round(float(signal['StopLoss']),2),
            'Target': round(float(signal['Target']),2),
            'Atr': round(float(signal['Atr']),2),
            'FractalPrice': round(float(signal['FractalPrice']),2),
            'FractalTime': pd.to_datetime(signal['FractalTime']).to_pydatetime(),
            'EntryTime': pd.to_datetime(signal['EntryTime']).to_pydatetime(),
            'Direction': int(signal['Direction']),
            'Status': 'Open',
            "UpdateTime":0,
            "Users":{}
        }
         
        PositionCollection.insert_one(position_doc)
        
        entry_doc = {
            'ID': POSITION_ID,
            "Entry":True,
            'Symbol': SYMBOL,
            'Side': str(signal['Signal']),
            "Condition":"Executed",
            'Price': float(signal['EntryPrice']),
            'OrderTime': pd.to_datetime(signal['EntryTime']).to_pydatetime(),
            "OrderType":"LIMIT",
            'Qty': round(qty,3),
            'Status': 'Open',
            "UpdateTime":0,
            "Users":{}
        }

        TradeCollection.insert_one(entry_doc)

        time.sleep(60)
        ## StopLoss Doc

        sl_doc = {
            'ID': sl_id,
            "StopLoss":True,
            'Symbol': SYMBOL,
            'Side': "SELL" if entry_doc['Side'] == "BUY" else "BUY",
            "Condition":"Open",
            'Price': round(float(signal['StopLoss']),2),
            'OrderTime': pd.to_datetime(signal['EntryTime']).to_pydatetime(),
            "OrderType":"STOP_MARKET",
            'Qty': round(qty,3),
            'Status': 'Open',
            "UpdateTime":0,
            "Users":{}
        }

        TradeCollection.insert_one(sl_doc)

        
        
        # Store position in position collection
        live_doc = {
            "ID":0,
            "PREV FRACTAL":signal['FractalPrice'],
            'EntryID': POSITION_ID,
            "SL_ID": sl_id,
            'Symbol': SYMBOL,
            'Side': signal['Signal'],
            'EntryTime': signal['EntryTime'],
            'EntryPrice': signal['EntryPrice'],
            'Qty': round(qty,3),
            'FractalPrice': signal['FractalPrice'],
            'FractalTime': signal['FractalTime'],
            'ATR': signal['Atr'],
            'Direction': round(signal['Direction'],2), 
            'StopLoss': round(signal['StopLoss'],2),
            'Target': round(float(signal['Target']),2),
            'Status': 'Open'
        }
        
        LiveCollection.update_one({"ID":0}, {'$set': live_doc},upsert=True)
        
        return POSITION_ID
    
    except Exception as e:
        logger.error(f"Error executing trade: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def check_open_positions():

    try:
        # Get current price
        ticker = ohlc.find_one({"ID":0})
        current_price = ticker['close']
        
        # Get open positions
        open_positions = list(PositionCollection.find({"Status": "Open"}))
        # logger.info(open_positions)
        for position in open_positions:
            exit_triggered = False
            exit_type = None
            exit_price = None
            
            # Check if stop loss hit
            if position['Side'] == "BUY":
                if current_price <= position['StopLoss']:
                    exit_triggered = True
                    exit_type = "StopLoss"
                    exit_price = position['StopLoss']

                    flt = {
                        "Condition":"Executed",
                        "UpdateTime": datetime.now(tz=pytz.utc)
                    }

                    TradeCollection.update_one({"ID":position['SL_ID']},{"$set":flt})
                elif current_price >= position['Target']:
                    exit_triggered = True
                    exit_type = "Target"
                    exit_price = position['Target']
                    flt = {
                        "Condition":"Cancel",
                        "UpdateTime": datetime.now(tz=pytz.utc)
                    }
                    TradeCollection.update_one({"ID":position['SL_ID']},{"$set":flt})
            else: 
                if current_price >= position['StopLoss']:
                    exit_triggered = True
                    exit_type = "StopLoss"
                    exit_price = position['StopLoss']
                    flt = {
                        "Condition":"Executed",
                        "UpdateTime": datetime.now(tz=pytz.utc)
                    }
                    TradeCollection.update_one({"ID":position['SL_ID']},{"$set":flt})

                elif current_price <= position['Target']:
                    exit_triggered = True
                    exit_type = "Target"
                    exit_price = position['Target']
                    flt = {
                        "Condition":"Cancel",
                        "UpdateTime": datetime.now(tz=pytz.utc)
                    }
                    TradeCollection.update_one({"ID":position['SL_ID']},{"$set":flt})
            
            if exit_triggered:
                # Calculate PNL
                if position['Side'] == "BUY":
                    side = "SELL"
                    pnl = (exit_price - position['EntryPrice']) * position['Qty']
                else:
                    side = "BUY"
                    pnl = (position['EntryPrice'] - exit_price) * position['Qty']
                
                # Update position status
                PositionCollection.update_one(
                    {"ID": position['ID']},
                    {"$set": {
                        "Status": "Closed",
                        "ExitPrice": exit_price,
                        "ExitTime": ticker['date'],
                        "ExitType": exit_type,
                        "PNL": pnl,
                    }}
                )
                
                # Update live collection
                LiveCollection.update_one(
                    {"ID": 0},
                    {"$set": {
                        "ExitPrice": exit_price,
                        "ExitTime": ticker['date'],
                        "ExitType": exit_type,
                        "PNL": pnl,
                        'Status': 'Completed'
                    }}
                )
                
                # Store completed trade in trade collection
                odate = datetime.now(timezone.utc)
                if exit_type == "Target":
                    trade_record = {
                        "ID": int(time.perf_counter_ns()),
                        "EntryId": position['ID'],
                        "Exit":True,
                        "Symbol": SYMBOL,
                        "Side": side,
                        "Price": exit_price,
                        "OrderTime": odate,
                        "OrderType":"MARKET",
                        "QTY": position['Qty'],
                        "Status": "Open"
                    }
                    
                    TradeCollection.insert_one(trade_record)
                    
                logger.info(f"Position closed: {position}")
                global POSITION_ID
                
                POSITION_ID = int(time.perf_counter_ns())
                
    
    except Exception as e:
        logger.error(f"Error checking positions: {str(e)}")
        logger.error(traceback.format_exc())

    
    except Exception as e:
        logger.error(f"Error updating OHLC database: {str(e)}")
        logger.error(traceback.format_exc())

def main():
    logger.info(f"Starting {STRATEGY}")
    
    dt = datetime.now()
    while True:
        try:
            # Fetch historical data
            df = fetch_historical_data(TIMEFRAME)
            if df is None or len(df) < (5):  # Need enough data for analysis
                logger.warning(f"Not enough data for analysis, waiting...{df.shape}")
                time.sleep(60)
                continue
            
            df = analyze_market_data(df)
            
            open_positions = list(PositionCollection.find({"Status": "Open"}))

            if len(open_positions) == 0:
                 
                trade_dict = liveUpdate()
                # Check for entry signals
                signal = check_for_entry_signals(df,trade_dict)

                # Execute trade if signal found
                if signal is not None:
                    trade_id = execute_trade(signal)
                    if trade_id:
                        logger.info(f"Trade executed with ID: {trade_id}")
                        time.sleep(60)
            else:

                check_open_positions()
                
        
        except Exception as e:
            logger.error(f"Error in main loop: {str(e)}")
            logger.error(traceback.format_exc())
            time.sleep(60)  # Wait before retrying


if __name__ == "__main__":
    # MongoDB connection
    load_dotenv()
    MONGO_LINK = os.getenv("STRATEGY_LINK")
    perTradLoss = 0
    
    STRATEGY = str(argv[1])

    if  "BTC" in STRATEGY:
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
        # sys.exit(1)
    
    # Setup logger
    current_file = str(os.path.basename(__file__)).replace('.py','')
    folder = file_path_locator()
    logs_dir = path.join(path.normpath(folder), "logs")
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)

    LOG_file = F"{logs_dir}/{STRATEGY}.log"
    

    logger = setup_logger(name=current_file,log_to_file=True,log_file=LOG_file,capture_print=True,log_to_console=True)

    TF = 5
    TIMEFRAME = f"{TF}min" 
    FRACTAL_PERIOD = 5
    ATR_PERIOD = 14  
    ST_FACTOR = 3.0 
    SL_FACTOR = 1.0  
    TP_FACTOR = 2.0
    LEVERAGE = 20
    CAPITAL = 150
    
   
    # Initialize MongoDB connections
    myclient = pymongo.MongoClient(MONGO_LINK)
    mydb = myclient[STRATEGY]
    LiveCollection = mydb["LiveUpdate"]
    PositionCollection = mydb["Position"]
    TradeCollection = mydb["Trades"]
    candles = myclient["CandleData"]
    COLL = SYMBOL.replace('-','')
    mycandle = candles[COLL]
    Ticks = myclient['Ticks']
    ohlc = Ticks[COLL]

    trade_dict = liveUpdate()
    POSITION_ID = trade_dict['ID']
    
    try:                
        import pause
        dt = datetime.now()
        minutes = dt.minute
        tt = minutes+1
        pause.until(dt.replace(minute=tt,second=0))

        main()

    except KeyboardInterrupt:
        logger.info("Strategy stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        logger.error(traceback.format_exc())


