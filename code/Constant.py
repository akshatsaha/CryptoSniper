import pydantic
from typing import Optional
from enum import Enum
from pydantic import BaseModel
from Utils import *
#### Strategy related constants


class OrderData(BaseModel):
    Entry:bool=False,
    StopLoss:bool=False,
    StopLossUpdate:bool=False,
    Exit:bool=False,
    Symbol: str
    Side: str
    OrderType: str
    PositionType:Optional[str]=None
    Quantity: float
    Price: Optional[float] = None
    StopLossPrice: Optional[float] = None
    TargetPrice: Optional[float] = None
    StrategyId: Optional[str] = None

#### Broker related constants

class AccountType:
    SPOT = "SPOT"
    FUTURE = "FUTURE"
    OPTION = "OPTION"
    MARGIN = "MARGIN"
    ISOLATED_MARGIN = "ISOLATED_MARGIN"
    

class OrderType:
    LIMIT = "LIMIT"
    MARKET = "MARKET"
    STOP="STOP"
    STOP_MARKET = "STOP_MARKET"
    STOP_LOSS_LIMIT = "STOP_LOSS_LIMIT"
    TAKE_PROFIT = "TAKE_PROFIT"
    TAKE_PROFIT_LIMIT = "TAKE_PROFIT_LIMIT"


class PositionType:
    LONG = "LONG"
    SHORT = "SHORT"


class OrderSide:
    BUY = "BUY"
    SELL = "SELL"

