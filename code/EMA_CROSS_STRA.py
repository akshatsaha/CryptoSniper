from __future__ import annotations
import time
import warnings
import pandas as pd
from datetime import datetime, timedelta
import pymongo
from sys import argv
import pytz
from Utils import *
from  dotenv import load_dotenv
import traceback
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)


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
        df = resample_data(candleDf, timeframe)
        df.reset_index(inplace=True)

        df = df[df['date'] <= last_complete]

        return df
    

    except Exception as e:
        logger.error(f"Error fetching historical data: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def resample_data(df, time_frame):
    try:
        df = df.copy()
        df.dropna(subset=['date'], inplace=True)
        df.set_index('date', inplace=True)

        resample_rule = {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
        }

        if 'volume' in df.columns:
            resample_rule['volume'] = 'sum'

        resampled = df.resample(time_frame).agg(resample_rule).dropna()
        resampled.reset_index(inplace=True)

        columns_order = ['date', 'open', 'high', 'low', 'close', 'volume']
        resampled = resampled[[col for col in columns_order if col in resampled.columns]]

        return resampled

    except Exception as e:
        logger.error(f"Error during resampling: {str(e)}")
        logger.error(traceback.format_exc())
        return pd.DataFrame()

def calculate_emas(df):
    df['EMA_10'] = df['close'].ewm(span=EMA_PERIOD_FAST, adjust=False).mean()
    df['EMA_30'] = df['close'].ewm(span=EMA_PERIOD_SLOW, adjust=False).mean()
    df['P_EMA_10'] = df['EMA_10'].shift(1)
    df['P_EMA_30'] = df['EMA_30'].shift(1)
    return df

def check_buy_signal(df):
    prev_row = df.iloc[-3]
    last_row = df.iloc[-2]
    return (
        prev_row['P_EMA_10'] < prev_row['P_EMA_30'] and   ## buy
        last_row['EMA_10'] > last_row['EMA_30'] and
        last_row['close'] > last_row['EMA_10'] and
        last_row['close'] > last_row['EMA_30']
        
        # prev_row['close'] > prev_row['EMA_10'] and
        # prev_row['close'] > prev_row['EMA_30']
    )

    # return (
    #     # prev_row['P_EMA_10'] > prev_row['P_EMA_30'] and
    #     # last_row['EMA_10'] < last_row['EMA_30'] and
    #     # last_row['close'] < last_row['EMA_10'] and
    #     last_row['close'] > last_row['EMA_30']
    # )

def liveUpdate():
    up = LiveCollection.find_one({'ID':0})
    if up is None:
        logger.info("No live update found, fetching historical data")
        setup_dict = {'ID':0}
        LiveCollection.insert_one(setup_dict)
        return setup_dict if up is None else up

    return up

def live_trading_loop():
    while True:
        try:
            
            open_positions = list(PositionCollection.find({"Status":"Open"}))

            if len(open_positions) == 0:
                df = fetch_historical_data(TIME_FRAME)
                if df is None or len(df) < 5:
                    logger.warning(f"Not enough data for analysis (df shape: {df.shape if df is not None else None}), waiting...")
                    time.sleep(60)
                    continue

                df = calculate_emas(df)
                if check_buy_signal(df) and len(open_positions) == 0:
                    print(f"[{datetime.utcnow()}] Buy signal detected!")
                    logger.info("[{}] Buy signal detected!".format(datetime.utcnow()))

                    POSITION_ID = int(time.perf_counter_ns())
                    sl_id = int(time.perf_counter_ns())

                    entry_price = df['close'].iloc[-1]
                    entry_time = df['date'].iloc[-1]

                    sl = entry_price - SL_POINT
                    target = entry_price + TG_POINT

                    CAPITAL = 100
                    LEVERAGE = 25   

                    qty = (CAPITAL * LEVERAGE) / entry_price
                    qty = max(QTY,round(qty, 3))


                    position_doc = {    
                        "ID": POSITION_ID,
                        "SL_ID": sl_id,
                        'Symbol': SYMBOL,
                        "Side": "BUY",
                        "Condition":"Executed",
                        "EntryPrice": entry_price,
                        "EntryTime": entry_time,
                        'QTY': qty,
                        "StopLoss": sl,
                        "Target": target,
                        'Status': 'Open',
                        "ExitTime": '',
                        "ExitPrice": '',
                        "ExitType": '',
                       "UpdateTime": datetime.utcnow(),
                       "PNL": 0
                    }

                    PositionCollection.insert_one(position_doc)
                    logger.info(f"Position {POSITION_ID} opened and logged in PositionCollection.")

                    entry_doc = {
                        "ID": POSITION_ID,
                        "Entry": True,
                        'Symbol': SYMBOL,
                        "Side": "BUY",
                        "Condition":"Executed",
                        "Price": entry_price,
                        "OrderTime": entry_time,
                        "OrderType":"LIMIT",
                        'QTY': qty,
                        'Status': 'Open',
                        "UpdateTime": 0,
                        "Users":{}
                    }

                    result = TradeCollection.insert_one(entry_doc)
                    logger.info(f"Entry trade executed successfully: {result.inserted_id} for position {POSITION_ID}")
            
                    time.sleep(60)
                    ## StopLoss Doc

                    sl_doc = {
                        "ID": sl_id,
                        "StopLoss":True,
                        'Symbol': SYMBOL,
                        "Side": "SELL",
                        "Condition":"Open",
                        "Price": sl,
                        "OrderTime": entry_time,
                        "OrderType":"STOP_MARKET",
                        'QTY': qty,
                        'Status': 'Open',
                        "UpdateTime": 0,
                        "Users":{}
                    }
                    res = TradeCollection.insert_one(sl_doc)
                    logger.info(f"SL trade executed successfully: {res.inserted_id} for position {POSITION_ID}")
                    
                    live_status_update_data = {
                        "ID": 0,
                        "EntryID": POSITION_ID,
                        "SL_ID": sl_id,
                        "Symbol": SYMBOL,
                        "Side": "BUY",
                        "EntryTime": entry_time,
                        "EntryPrice": entry_price,
                        "QTY": qty,
                        "Status": 'Open',
                        "StopLoss": sl,
                        "Target": target,
                        'Status': 'Open'
                    }

                    LiveCollection.update_one({"ID":0}, {"$set": live_status_update_data}, upsert=True)
                    logger.info(f"Live status updated to 'Open' for position {POSITION_ID}.")

                    time.sleep(60*TF)
                    continue

            else:
                ticker = ohlc.find_one({})
                if not ticker or 'close' not in ticker:
                    logger.warning("No valid ticker data found, waiting briefly...")
                    time.sleep(10)
                    continue

                sl_check = ticker['low']
                tg_check = ticker['high']
                current_price = ticker['close']

                for position in open_positions:
                    position_id = position['ID']
                    side = position['Side']
                    sl_price = position['StopLoss']
                    target_price = position['Target']
                    entry_price = position['EntryPrice']
                    qty = position['QTY']

                    exit_price = None
                    exit_type = None

                    if side == "BUY":
                        if sl_check <=  position['StopLoss']:
                            exit_price = sl_check
                            exit_type = "StopLoss"
                            logger.info(f"Position {position_id}: Stop Loss hit at price {exit_price}")
                            flt = {
                                "Condition":"Executed",
                                "UpdateTime": datetime.now(tz=pytz.utc)
                            }
                            TradeCollection.update_one({"ID":position['SL_ID']},{"$set":flt})

                        elif tg_check >= position['Target']:
                            exit_price = tg_check
                            exit_type = "Target"
                            logger.info(f"Position {position_id}: Target hit at price {exit_price}")
                            flt = {
                                "Condition":"Cancel",
                                "UpdateTime": datetime.now(tz=pytz.utc)
                            }
                            TradeCollection.update_one({"ID":position['SL_ID']},{"$set":flt})

                        elif df['close'].iloc[-2] < df['ema10'].iloc[-2]:
                            exit_price = df['close'].iloc[-1] 
                            exit_type = "EMA10_Crossover"
                            logger.info(f"Position {position_id}: candle close ({df['close'].iloc[-2]:.2f}) < EMA10 ({df['ema10'].iloc[-2]:.2f}). Exiting at {exit_price:.2f}")

                        if exit_price is None:
                            continue

                        pnl = (exit_price - entry_price) * qty
                        logger.info(f"Position {position_id} closed with {exit_type}. PNL: {pnl:.2f}")

                        PositionCollection.update_one(
                            {"ID":  position['ID']},
                            {
                                "$set": {
                                    "Status": "Closed",
                                    "ExitPrice": exit_price,
                                    "ExitTime": datetime.utcnow(),
                                    "ExitType": exit_type,
                                    "PNL": pnl,
                                }
                            }
                        )
                        logger.info(f"Position {position_id} updated as 'Closed' in PositionCollection.")

                        
                        LiveCollection.update_one(
                        {"ID":0}, 
                        {"$set":{
                            "ExitTime": datetime.utcnow(),
                            "ExitPrice": exit_price,
                            "ExitType": exit_type,
                            "PNL": pnl,
                            'Status': 'Completed'
                            ,
                        } })
                        logger.info(f"Live status updated for exit of position {position_id}.")

                        TradeCollection.insert_one({
                            "ID": int(time.perf_counter_ns()),
                            "EntryId": position['ID'],
                            "Exit":True,
                            'Symbol': SYMBOL,
                            "Side": "SELL",
                            "Price": exit_price,
                            "OrderTime": datetime.utcnow(),
                            "OrderType": "MARKET",
                            "QTY": qty,
                            'Status': 'Open',
                        })
                        logger.info(f"Exit trade logged for position {position_id}.")


                time.sleep(10)

        except Exception as e:
            print(f"[{datetime.utcnow()}] Error in main loop: {e}")
            logger.error(f"[{datetime.utcnow()}] Error in main loop: {e}")
            logger.error(traceback.format_exc())
            time.sleep(60)

if __name__ == "__main__":

    # MongoDB connection
    load_dotenv()
    MONGO_LINK = os.getenv("STRATEGY_LINK")
    perTradLoss = 0
    QTY = 0.01

    STRATEGY = str(argv[1])

    if  "BTC" in STRATEGY or "Bit" in STRATEGY:
        SYMBOL = 'BTC-USDT'
        perTradLoss = 0.01

    else:
        print("Symbol not allow")
    
    # Setup logger
    current_file = str(os.path.basename(__file__)).replace('.py','')
    folder = file_path_locator()
    logs_dir = path.join(path.normpath(folder), "logs")
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)

    LOG_file = F"{logs_dir}/{STRATEGY}.log"
    
    logger = setup_logger(name=current_file,log_to_file=True,log_file=LOG_file,capture_print=True,log_to_console=False)
    
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

    # Strategy Parameters
    TF = 3
    TIME_FRAME = F"{TF}min"
    EMA_PERIOD_FAST = 10
    EMA_PERIOD_SLOW = 30
    SL_POINT = 20
    TG_POINT = 50

    print(SYMBOL)

    live_trading_loop()





















































