 
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
import pandas as pd





# Configure lifespan events
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup event
    logger.info("Starting up the API server")
    try:
        FastAPICache.init(InMemoryBackend(), prefix="fastapi-cache")
        # Create indexes for better query performance
        await db.users.create_index([('email', 1)], unique=True)
        await db.users.create_index([('referral_code', 1)])
        await db.strategies.create_index([('userId', 1)])
        await db.strategies.create_index([('name', 1)])
        await db.trades.create_index([('userId', 1)])
        await db.trades.create_index([('symbol', 1)])
        await db.trades.create_index([('timestamp', -1)])
        await db.positions.create_index([('symbol', 1)])
        await db.positions.create_index([('positionSide', 1)])
        logger.info("Database indexes created successfully")
    except Exception as e:
        logger.error(f"Failed to create indexes: {str(e)}")
    yield
    # Shutdown event
    logger.info("Shutting down the API server")
    # Close MongoDB connection pool
    client.close()




# Now modify your app configuration to use this response class
app = FastAPI(
    title="Crypto Trading API",
    description="High-performance API for crypto trading",
    version="1.0.0",
    openapi_url=None if os.getenv("ENVIRONMENT") == "production" else "/openapi.json",
    docs_url=None if os.getenv("ENVIRONMENT") == "production" else "/docs",
    lifespan=lifespan,  # Added lifespan manager
    # Custom JSONEncoder to handle MongoDB's ObjectId
)

class MongoJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, datetime):  # Also handle datetime objects
            return obj.isoformat()
        return super().default(obj)

# Custom JSONResponse that uses our encoder
class MongoJSONResponse(JSONResponse):
    def render(self, content) -> bytes:
        return json.dumps(
            content,
            ensure_ascii=False,
            allow_nan=False,
            indent=None,
            separators=(",", ":"),
            cls=MongoJSONEncoder,
        ).encode("utf-8")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("api.log")
    ]
)

logger = logging.getLogger("crypto-api")

# 34.131.119.198

# Configuration
DATABASE_URL = "mongodb+srv://vipinpal7060:lRKAbH2D7W18LMZd@cluster0.fg30pmw.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"


JWT_SECRET = "WFuFIpMpWx2kdYG0fFBb15GqwHlghQMpjI16i8BpP8W6WhC1LWU9TObowQ6F4gSTGygHFFSyfGTzCnX2CbDc3A=="
JWT_ALGORITHM = "HS256"

# CORS configuration
origins = [
    # "http://localhost:7000",      # optional, for local dev
    "https://thecryptosnipers.com",
    "http://thecryptosnipers.com"
    
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add session middleware with optimized settings
app.add_middleware(
    SessionMiddleware,
    secret_key=JWT_SECRET,
    session_cookie="session",
    max_age=1 * 24 * 60 * 60,  # 1 day
    same_site="lax",          # Improve security
    https_only=False,          # Set to True in production with HTTPS
)






# Now let's create a function to override FastAPI's default response class
def configure_custom_responses(app: FastAPI):
    """
    Configure FastAPI app to use custom response class for JSON serialization
    """
    # Override default JSONResponse with our custom one that handles ObjectId
    app.router.default_response_class = MongoJSONResponse

# Now, call this function in your main FastAPI app to apply the custom response class
configure_custom_responses(app)


def serialize_doc(doc):
    if isinstance(doc, list):
        return [serialize_doc(item) for item in doc]
    if isinstance(doc, dict):
        new_doc = {}
        for k, v in doc.items():
            if isinstance(v, ObjectId):
                new_doc[k] = str(v)
            elif isinstance(v, (dict, list)):
                new_doc[k] = serialize_doc(v)
            else:
                new_doc[k] = v
        return new_doc
    return doc




# Rate limiting middleware for high traffic protection
class RateLimitMiddleware:
    def __init__(self, app, rate_limit=100, window=60):
        self.app = app
        self.rate_limit = rate_limit  # requests per window
        self.window = window  # window in seconds
        self.clients = {}
        self._cleanup_task = None
        
    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        
        client_ip = self._get_client_ip(scope)
        current_time = time.time()
        
        # Clean up old requests
        if client_ip in self.clients:
            self.clients[client_ip] = [ts for ts in self.clients[client_ip] if current_time - ts < self.window]
        else:
            self.clients[client_ip] = []
        
        # Check rate limit
        if len(self.clients[client_ip]) >= self.rate_limit:
            return await self._rate_limited_response(scope, receive, send)
        
        # Add current request
        self.clients[client_ip].append(current_time)
        
        # Process request
        return await self.app(scope, receive, send)
    
    def _get_client_ip(self, scope):
        headers = {k.decode('utf8').lower(): v.decode('utf8') 
                  for k, v in scope.get('headers', [])}
        
        # Try X-Forwarded-For first (for proxied requests)
        if 'x-forwarded-for' in headers:
            return headers['x-forwarded-for'].split(',')[0].strip()
        
        # Fall back to client address
        client = scope.get('client')
        if client:
            return client[0]
        
        return 'unknown'
    
    async def _rate_limited_response(self, scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 429,
            'headers': [
                [b'content-type', b'application/json'],
            ],
        })
        
        await send({
            'type': 'http.response.body',
            'body': json.dumps({"detail": "Too many requests"}).encode(),
        })
    
    async def cleanup(self):
        while True:
            await asyncio.sleep(self.window)
            current_time = time.time()
            for client_ip in list(self.clients.keys()):
                self.clients[client_ip] = [ts for ts in self.clients[client_ip] 
                                         if current_time - ts < self.window]
                if not self.clients[client_ip]:
                    del self.clients[client_ip]

# Add rate limiting middleware (100 requests per minute per IP)
# app.add_middleware(RateLimitMiddleware, rate_limit=100, window=60)

# Configure MongoDB with connection pooling for high throughput
client = motor.motor_asyncio.AsyncIOMotorClient(
    DATABASE_URL,
    maxPoolSize=100,               # Maximum number of connections in the pool
    minPoolSize=10,                # Minimum number of connections in the pool
    maxIdleTimeMS=30000,           # Maximum time a connection can remain idle (30 seconds)
    socketTimeoutMS=20000,         # Socket timeout (20 seconds)
    connectTimeoutMS=20000,        # Connection timeout (20 seconds)
    serverSelectionTimeoutMS=20000, # Server selection timeout (20 seconds)
    waitQueueTimeoutMS=10000,      # Wait queue timeout (10 seconds)
    retryWrites=True,              # Retry writes if they fail
    w="majority"                   # Write concern
)


# Database references
db = client.cryptosnipers
clienttrades = client.ClientTrades
clientPositions = client.clientPositions


# Generate a proper Fernet key from the SECRET_KEY
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



async def generate_unique_referral_code(min_length=10, max_length=14, max_attempts=100):
    chars = string.ascii_uppercase + string.digits
    for _ in range(max_attempts):
        code_length = random.randint(min_length, max_length)
        referral_code = ''.join(random.choices(chars, k=code_length))
        
        # Check if referral code already exists
        existing_code = await db.users.find_one({"referral_code": referral_code})
        if not existing_code:
            return referral_code
    
    # If we've exhausted all attempts, generate a unique code with timestamp
    timestamp = int(time.time())
    referral_code = f"{chars[0]}{timestamp}{chars[-1]}"
    return referral_code



def send_otp_email(sender_email: str, sender_password: str, recipient_email: str, smtp_server: str, smtp_port: int, company_name: str, otp:str) -> bool:

    otp_str = str(otp)

    # 2. Create the email message
    message = MIMEMultipart("alternative")
    message["Subject"] = f"Your One-Time Password (OTP) from {company_name}"
    message["From"] = formataddr((company_name, sender_email))
    message["To"] = recipient_email

    # Create the plain-text and HTML versions of your message
    text = f"""\
    Your OTP is: {otp_str}
    Thank you for connecting with us!
    """

    html = f"""\
    <html>
    <head>
        <title>{company_name} OTP</title>
        <style>
            body {{ font-family: sans-serif; }}
            .container {{ width: 80%; margin: 0 auto; padding: 20px; border: 1px solid #ddd; border-radius: 8px; }}
            .header {{ background-color: #f2f2f2; padding: 10px; text-align: center; border-bottom: 1px solid #ddd; }}
            .otp-section {{ margin: 20px 0; text-align: center; }}
            .otp-code {{ font-size: 24px; font-weight: bold; color: #007bff; }}
            .thank-you {{ text-align: center; margin-top: 20px; color: #555; }}
            .company-name {{ font-size: 18px; font-weight: bold; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <p class="company-name">{company_name}</p>
            </div>
            <div class="otp-section">
                <p>Your One-Time Password (OTP) is:</p>
                <p class="otp-code">{otp_str}</p>
                <p>Please use this code to complete your action.</p>
            </div>
            <div class="thank-you">
                <p>Thank you for choosing us!</p>
                <p>If you did not request this, please ignore this email.</p>
            </div>
        </div>
    </body>
    </html>
    """

    # Turn these into MIMEText objects
    part1 = MIMEText(text, "plain")
    part2 = MIMEText(html, "html")

    # Add HTML parts to MIMEMultipart message
    # The email client will try to render the last part first
    message.attach(part1)
    message.attach(part2)

    # 3. Connect to the SMTP server and send the email
    context = ssl.create_default_context()

    try:
        # Use SMTP_SSL for port 465, or SMTP and starttls() for port 587
        if smtp_port == 465:
            with smtplib.SMTP_SSL(smtp_server, smtp_port, context=context) as server:
                server.login(sender_email, sender_password)
                server.sendmail(sender_email, recipient_email, message.as_string())
            print(f"OTP email sent successfully to {recipient_email}")
            return True
        elif smtp_port == 587:
             with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls(context=context) # Secure the connection
                server.login(sender_email, sender_password)
                server.sendmail(sender_email, recipient_email, message.as_string())
             print(f"OTP email sent successfully to {recipient_email}")
             return True
        else:
             print(f"Error: Unsupported SMTP port {smtp_port}. Use 465 (SSL) or 587 (TLS).")
             return False

    except Exception as e:
        print(f"Error sending OTP email: {e}")
        return False



# MongoDB-based storage implementation
class MongoDBStorage:
    def __init__(self, db_client, db_name):
        self.client = db_client
        self.db = self.client[db_name]
        self.users_collection = self.db.users
        self.strategies_collection = self.db.Strategies
        self.positions_collection = self.db.positions
        self.portfolio_snapshots_collection = self.db.portfolio_snapshots
        self.trades_collection = self.db.trades
        
    # User methods
    async def get_user(self, id):
        result = await self.users_collection.find_one({'_id': id})
        return serialize_doc(result) if result else None
    
    async def get_user_by_username(self, username: str):
        result = await self.users_collection.find_one({'username': username})
        return serialize_doc(result) if result else None
        return await self.users_collection.find_one({"username": username})
    
    async def get_user_by_email(self, email: str):
        result = await self.users_collection.find_one({'email': email})
        return serialize_doc(result) if result else None
    
    async def create_user(self, user_data):
        existing_user = await self.users_collection.find_one({"email": user_data["email"]})
        if existing_user:
            return existing_user

        now = datetime.now()
        user_data["created_at"] = now
        user_data["updated_at"] = now
        
        # Generate a unique referral code if not provided
        if "referral_code" not in user_data or not user_data["referral_code"]:
            user_data["referral_code"] = await self._generate_unique_referral_code()
        
        result = await self.users_collection.insert_one(user_data)
        user_data["_id"] = result.inserted_id
        print(user_data)
        return user_data
    
    async def update_user(self, user_id, update_data):
        update_data["updated_at"] = datetime.now()
        result = await self.users_collection.update_one(
            {"_id": user_id},
            {"$set": update_data}
        )
        if result.modified_count > 0:
            return await self.get_user(user_id)
        return None
    
    async def update_broker(self,user_id,broker_name):
        result = await self.users_collection.update_one(
            {"_id": user_id},
            {"$set": {"broker_name":broker_name}}
        )

    
    # Strategy methods
    async def get_strategies(self, user_id):
        cursor = self.strategies_collection.find({"userId": user_id})
        results = await cursor.to_list(length=None)
        return serialize_doc(results) if results else None
    
    async def get_deployed_strategies(self, user_id):
        cursor = self.strategies_collection.find({
            "userId": user_id,
            "isDeployed": True
        })
        results = await cursor.to_list(length=None)
        return serialize_doc(results) if results else None
    
    async def get_strategy(self, strategy_id):
        result = await self.strategies_collection.find_one({'_id': strategy_id})
        return serialize_doc(result) if result else None
    
    async def create_strategy(self, strategy_data):
        now = datetime.now()
        strategy_data["created_at"] = now
        strategy_data["updated_at"] = now
        
        result = await self.strategies_collection.insert_one(strategy_data)
        strategy_data["_id"] = result.inserted_id
        return strategy_data
    
    async def update_strategy(self, strategy_id, update_data):
        update_data["updated_at"] = datetime.now()
        result = await self.strategies_collection.update_one(
            {"_id": strategy_id},
            {"$set": update_data}
        )
        if result.modified_count > 0:
            return await self.get_strategy(strategy_id)
        return None
    
    async def delete_strategy(self, strategy_id):
        result = await self.strategies_collection.delete_one({"_id": strategy_id})
        return result.deleted_count > 0
    
    # Position methods
    async def get_positions(self, user_id):
        cursor = self.positions_collection.find({"userId": user_id})
        results = await cursor.to_list(length=None)
        return serialize_doc(results) if results else None
    
    async def get_position(self, position_id):
        result = await self.positions_collection.find_one({'_id': position_id})
        return serialize_doc(result) if result else None
    
    async def create_position(self, position_data):
        # Use the position_refer structure as a template
        position_template = {
            "positionId": str(position_data.get("positionId", "")),
            "avgClosePrice": position_data.get("avgClosePrice", "0"),
            "avgPrice": position_data.get("avgPrice", "0"),
            "closeAllPositions": position_data.get("closeAllPositions", False),
            "closePositionAmt": position_data.get("closePositionAmt", "0"),
            "isolated": position_data.get("isolated", True),
            "leverage": position_data.get("leverage", 1),
            "netProfit": position_data.get("netProfit", "0"),
            "openTime": position_data.get("openTime", int(datetime.now().timestamp() * 1000)),
            "positionAmt": position_data.get("positionAmt", "0"),
            "positionCommission": position_data.get("positionCommission", "0"),
            "positionSide": position_data.get("positionSide", "LONG"),
            "realisedProfit": position_data.get("realisedProfit", "0"),
            "symbol": position_data.get("symbol", ""),
            "totalFunding": position_data.get("totalFunding", "0"),
            "updateTime": position_data.get("updateTime", int(datetime.now().timestamp() * 1000)),
            "userId": position_data.get("userId", None)
        }
        
        # Merge with any additional fields from position_data
        for key, value in position_data.items():
            if key not in position_template:
                position_template[key] = value
        
        result = await self.positions_collection.insert_one(position_template)
        position_template["_id"] = result.inserted_id
        return position_template
    
    async def update_position(self, position_id, update_data):
        update_data["updateTime"] = int(datetime.now().timestamp() * 1000)
        result = await self.positions_collection.update_one(
            {"_id": position_id},
            {"$set": update_data}
        )
        if result.modified_count > 0:
            return await self.get_position(position_id)
        return None
    
    async def delete_position(self, position_id):
        result = await self.positions_collection.delete_one({"_id": position_id})
        return result.deleted_count > 0
    
    # Trade methods
    async def get_trades(self, user_id=None, symbol=None, limit=100):
        query = {}
        if user_id:
            query["userId"] = user_id
        if symbol:
            query["symbol"] = symbol
            
        cursor = self.trades_collection.find(query).sort("timestamp", -1).limit(limit)
        return await cursor.to_list(length=None)
    
    async def get_trade(self, trade_id):
        result = await self.trades_collection.find_one({'_id': trade_id})
        return serialize_doc(result)
    
    async def create_trade(self, trade_data):
        # Use the trade_refer structure as a template
        trade_template = {
            "ID": trade_data.get("ID", str(int(datetime.now().timestamp() * 1000))),
            "StrategyId": trade_data.get("StrategyId", ""),
            "symbol": trade_data.get("symbol", ""),
            "orderId": trade_data.get("orderId", ""),
            "side": trade_data.get("side", ""),
            "positionSide": trade_data.get("positionSide", ""),
            "type": trade_data.get("type", "LIMIT"),
            "origQty": trade_data.get("origQty", "0"),
            "price": trade_data.get("price", "0"),
            "executedQty": trade_data.get("executedQty", "0"),
            "avgPrice": trade_data.get("avgPrice", "0"),
            "cumQuote": trade_data.get("cumQuote", "0"),
            "stopPrice": trade_data.get("stopPrice", ""),
            "profit": trade_data.get("profit", "0.0000"),
            "timestamp": trade_data.get("timestamp", datetime.now()),
            "status": trade_data.get("status", "NEW"),
            "userId": trade_data.get("userId", None)
        }
        
        # Merge with any additional fields from trade_data
        for key, value in trade_data.items():
            if key not in trade_template:
                trade_template[key] = value
        
        result = await self.trades_collection.insert_one(trade_template)
        trade_template["_id"] = result.inserted_id
        return trade_template
    
    async def update_trade(self, trade_id, update_data):
        result = await self.trades_collection.update_one(
            {"_id": trade_id},
            {"$set": update_data}
        )
        if result.modified_count > 0:
            return await self.get_trade(trade_id)
        return None
    
    # Portfolio methods
    async def get_portfolio_snapshots(self, user_id, limit=30):
        cursor = self.portfolio_snapshots_collection.find({
            "userId": user_id
        }).sort("timestamp", -1).limit(limit)
        return await cursor.to_list(length=None)
    
    async def get_latest_portfolio_snapshot(self, user_id):
        cursor = self.portfolio_snapshots_collection.find({
            "userId": user_id
        }).sort("timestamp", -1).limit(1)
        snapshots = await cursor.to_list(length=1)
        return snapshots[0] if snapshots else None
    
    async def create_portfolio_snapshot(self, snapshot_data):
        now = datetime.now()
        snapshot_data["timestamp"] = now
        
        result = await self.portfolio_snapshots_collection.insert_one(snapshot_data)
        snapshot_data["_id"] = result.inserted_id
        return snapshot_data
    
    # Helper methods
    async def _generate_unique_referral_code(self, min_length=10, max_length=14, max_attempts=100):
        chars = string.ascii_uppercase + string.digits
        for _ in range(max_attempts):
            code_length = random.randint(min_length, max_length)
            referral_code = ''.join(random.choices(chars, k=code_length))
            
            # Check if referral code already exists
            existing_code = await self.users_collection.find_one({"referral_code": referral_code})
            if not existing_code:
                return referral_code
        
        # If we've exhausted all attempts, generate a unique code with timestamp
        timestamp = int(time.time())
        referral_code = f"{chars[0]}{timestamp}{chars[-1]}"
        return referral_code



# Initialize MongoDB storage
storage = MongoDBStorage(client, "cryptosnipers")

# Redis-like in-memory cache for session data (for high throughput)
class SessionCache:
    def __init__(self, expiry_time=3600):  # Default 1 hour expiry
        self.cache = {}
        self.expiry_time = expiry_time
        
    def get(self, key):
        if key in self.cache:
            data, timestamp = self.cache[key]
            # Check if expired
            if time.time() - timestamp > self.expiry_time:
                del self.cache[key]
                return None
            return data
        return None
        
    def set(self, key, value, custom_expiry=None):
        expiry = custom_expiry if custom_expiry else self.expiry_time
        self.cache[key] = (value, time.time())
        
    def delete(self, key):
        if key in self.cache:
            del self.cache[key]
            
    def clear_expired(self):
        current_time = time.time()
        expired_keys = [k for k, (_, timestamp) in self.cache.items() 
                       if current_time - timestamp > self.expiry_time]
        for key in expired_keys:
            del self.cache[key]

# Initialize session cache
session_cache = SessionCache()

# OTP verification store with expiry
otp_store = {}
OTP_EXPIRE_TIME = 5 * 60  # 5 minutes in seconds
your_email_address = "vipinpal.dev@gmail.com"
your_email_password = "rzqr mlqi vniq qtve"
recipient_email_address = "vipinpal7080@gmail.com"
your_smtp_server = "smtp.gmail.com"
your_smtp_port = 587
your_company_name = "Crypto Snippers"



def convert_objectid(obj):
    if isinstance(obj, list):
        return [convert_objectid(item) for item in obj]
    elif isinstance(obj, dict):
        return {k: convert_objectid(v) for k, v in obj.items()}
    elif isinstance(obj, ObjectId):
        return str(obj)
    else:
        return obj

def generate_otp():
    import random
    return str(random.randint(100000, 999999))



SECRET_KEY = "WFuFIpMpWx2kdYG0fFBb15GqwHlghQMpjI16i8BpP8W6WhC1LWU9TObowQ6F4gSTGygHFFSyfGTzCnX2CbDc3A=="
ALGORITHM = "HS256"


async def get_current_user(request: Request):
    # 1. Try JWT from Authorization header
    auth_header = request.headers.get("Authorization")
    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header.split(" ")[1]
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            email = payload.get("email")
            if email:
                user = await storage.get_user_by_email(email)
                if user:
                    session_cache.set(f"user:{str(user['_id'])}", user)
                    return user
        except JWTError as e:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Invalid token: {str(e)}"
            )

    # 2. Try email parameter (fallback)
    if email:
        user = await storage.get_user_by_email(email)
        if user:
            session_cache.set(f"user:{str(user['_id'])}", user)
            return user

    # 3. Fall back to session
    user_id = request.session.get("userId")
    if user_id:
        cached_user = session_cache.get(f"user:{user_id}")
        if cached_user:
            return cached_user

        user = await storage.get_user(user_id)
        if user:
            session_cache.set(f"user:{user_id}", user)
            return user

    # 4. If all failed
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Authentication required"
    )



class User(BaseModel):
    name:str
    email: str
    phone:str
    password:str
    status: str = "pending"
    referral_code: Optional[str] = None
    invited_by: Optional[str] = None
    approved_at: Optional[datetime] = None
    referral_count: int = 0
    created_at: datetime = datetime.now()
    updated_at: datetime = datetime.now()
    broker_name: Optional[str] = None
    strategies: Optional[list] = []
    is_admin: bool = False
    is_active: bool = False
    api_verified: bool = False


class BrokerConnectionCreate(BaseModel):
    userId: str
    broker_name: str
    api_key: str
    secret_key: str
    credentials: Optional[dict] = None
    is_active: bool = False
    created_at: datetime = datetime.now()
    updated_at: datetime = datetime.now()


class StrategyConfig(BaseModel):
    name: str
    type: str
    leverage: int
    margin: float
    created_at: datetime = datetime.now()
    updated_at: datetime = datetime.now()
    is_active: bool = False

class SignupRequest(BaseModel):
    email: EmailStr


class OTPVerifyRequest(BaseModel):
    email: EmailStr
    otp: str


class SigninRequest(BaseModel):
    email: EmailStr

class UserBase(BaseModel):
    username: str
    email: EmailStr
    name: Optional[str] = None
    phone: Optional[str] = None

class UserCreate(UserBase):
    password: str




# Middleware to log API requests
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    path = request.url.path
    
    try:
        response = await call_next(request)
    except Exception as e:
        logger.error(f"Exception in request processing for {request.method} {path}: {e}", exc_info=True)
        # It's important to re-raise the exception so FastAPI's default error handling can take over
        # or so other error handling middleware can process it.
        raise
    
    # Log only if the response was successfully obtained
    if path.startswith("/api"):
        # Ensure response is available before trying to access response.status_code
        # This check might be redundant if an exception in call_next is always re-raised,
        # but it's a good defensive measure.
        if 'response' in locals() and hasattr(response, 'status_code'):
            duration = time.time() - start_time
            log_line = f"{request.method} {path} {response.status_code} in {duration*1000:.2f}ms"
            logger.info(log_line) # Use logger instead of print
        else:
            # This case should ideally not be hit if exceptions are re-raised properly.
            # Log that we couldn't get a status code, possibly due to an earlier unhandled error.
            logger.warning(f"{request.method} {path} - Response object or status_code not available for logging.")

    return response



# Authentication routes
@app.post("/api/auth/signup", status_code=status.HTTP_200_OK)
@app.post("/auth/signup", status_code=status.HTTP_200_OK)
async def signup(data: SignupRequest):
    email = data.email

    if not email:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email is required"
        )
    
    # Check if user already exists (using optimized MongoDB storage)
    existing_user = await storage.get_user_by_email(email)
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User with this email already exists"
        )
    
    # Generate OTP
    otp = generate_otp()

    # Send OTP email asynchronously
    success = send_otp_email(
        sender_email=your_email_address,
        sender_password=your_email_password,
        recipient_email=email,
        smtp_server=your_smtp_server,
        smtp_port=your_smtp_port,
        company_name=your_company_name,
        otp=otp
    )
    
    # Store OTP with timestamp
    otp_store[email] = {"otp": otp, "timestamp": time.time()}
    
    if success:
        return {"message": "OTP sent successfully"}
    else:
        return {"message": "Failed to send email"}

@app.post("/api/auth/verify-otp", status_code=status.HTTP_200_OK)
@app.post("/auth/verify-otp", status_code=status.HTTP_200_OK)
async def verify_otp(request: OTPVerifyRequest):
    if not request.email or not request.otp:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email and OTP are required"
        )
    
    stored_otp = otp_store.get(request.email)
    
    if not stored_otp:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No OTP found for this email"
        )
    
    if time.time() - stored_otp["timestamp"] > OTP_EXPIRE_TIME:
        del otp_store[request.email]
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="OTP has expired"
        )
    
    if stored_otp["otp"] != request.otp:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid OTP"
        )
    
    # OTP verified successfully, remove from store
    del otp_store[request.email]
    
    return {"message": "OTP verified successfully"}

@app.post("/api/auth/complete-profile", status_code=status.HTTP_200_OK)
@app.post("/auth/complete-profile")
async def complete_profile(user_data: User, request: Request):
    # Create user with MongoDB storage
    print("user_data",user_data)
    existing_user = await storage.get_user_by_email(user_data.email)
    if existing_user:
        raise HTTPException(status_code=400, detail="User already exists") 

    user = await storage.create_user(user_data.model_dump())
    
    # Set user in session
    request.session["userId"] = str(user["_id"])
    
    # Cache user for faster access
    session_cache.set(f"user:{user['_id']}", user)
    print(user)
    print("Registration completed successfully")

    return {"message": "Registration completed successfully"}

@app.post("/api/auth/signin", status_code=status.HTTP_200_OK)
@app.post("/auth/signin", status_code=status.HTTP_200_OK)
async def signin(signin_data: SigninRequest, request: Request):
    if not signin_data.email:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email is required"
        )
    
    # Find user by email using MongoDB storage
    user = await storage.get_user_by_email(signin_data.email)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials"
        )
    
    # In a real app, compare hashed passwords
    if user.get("password") != signin_data.password:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials"
        )
    
    # Set user in session
    request.session["userId"] = str(user["_id"])
    
    # Cache user for faster access
    session_cache.set(f"user:{user['_id']}", user)
    
    return {
        "message": "Login successful",
        "user": {
            "id": str(user["_id"]),
            "email": user["email"],
            "name": user.get("name", ""),
        }
    }

@app.post("/api/auth/signout", status_code=status.HTTP_200_OK)
@app.post("/auth/signout", status_code=status.HTTP_200_OK)
async def signout(request: Request):
    user_id = request.session.get("userId")
    if user_id:
        # Clear user from cache
        session_cache.delete(f"user:{user_id}")
    
    # Clear session
    request.session.clear()
    return {"message": "Logout successful"}

@app.get("/api/auth/user", status_code=status.HTTP_200_OK)
@app.get("/auth/user", status_code=status.HTTP_200_OK)
async def get_user(email: str):
    if not email:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="email is required"
        )
    
    # Try to get from cache first
    cached_user = session_cache.get(f"email:{email}")
    if cached_user:
        return {"status": "user_exists", "user": cached_user}
    
    # If not in cache, query database
    user = await storage.users_collection.find_one({"email": email})
    if user:
        # Cache for future requests
        session_cache.set(f"email:{email}", user)
        return {"status": "user_exists", "user": user}
    
    return {"status": "user_not_found"}
    
@app.get("/api/get-broker", status_code=status.HTTP_200_OK)
@app.get("/get-broker", status_code=status.HTTP_200_OK)
async def get_broker(email: str = Query(..., description="Email of the user whose broker info to retrieve")):

    try:
        # Get the user from the database
        user = await storage.users_collection.find_one({"email": email})
        
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )

        # Check if broker information exists in the user document
        if not user.get('broker_name'):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No broker information found for this user"
            )
            
        # Return the broker information from the user document
        response = {
            "broker_name": user.get("broker_name"),
            "api_verified": user.get("api_verified", False)
        }
        
        return response
        
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error getting broker info: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while fetching broker information: {str(e)}"
        )


@app.post("/api/add-broker", status_code=status.HTTP_200_OK)
@app.post("/add-broker", status_code=status.HTTP_200_OK)
async def set_broker(email:str,broker_name: str = Query(..., description="Email of the user whose broker info to update")):

    try:
        # Get the user from the database
        user = await storage.users_collection.find_one({"email": email})
        
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )

        # Check if broker information exists in the user document
        if user.get('broker_name') and user.get("api_verified"):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="broker information found for this user"
            )
            
        updateuser = await storage.update_broker(user['_id'],broker_name)
        # Return the broker information from the user document
        # response = {
        #     "broker_name": user.get("broker_name"),
        #     "api_verified": user.get("api_verified", False)
        # }

        return {"success": True}
        
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error getting broker info: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while fetching broker information: {str(e)}"
        )


@app.post("/api/verify-broker", status_code=status.HTTP_200_OK)
@app.post("/verify-broker", status_code=status.HTTP_200_OK)
@cache(expire=60)
async def verify_broker_endpoint(request: dict):
    """
    Verify broker API credentials and update user's balance
    """
    try:
        email = request.get("email")
        if not email:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Email is required"
            )
        # Get user from database
        user = await storage.users_collection.find_one({"email": email})
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )

        # Get API key and secret from request
        api_key = request.get("api_key")
        secret_key = request.get("secret_key")
        brokerId = request.get("broker_id")
        app_name = request.get("app_name") 

        logger.info(f"[/api/verify-broker] Received for email '{email}': api_key='{api_key}', secret_key='{secret_key}', brokerId='{brokerId}', app_name='{app_name}'")
        # print statements for debugging, can be removed later
        # print("inside verify")
        # print(api_key)
        # print(secret_key)
        # print(brokerId)
        # print(app_name)
        
        # Validate API key and secret key are not empty or placeholders
        if not api_key or not secret_key or api_key in ["api_key", "", None] or secret_key in ["api_secret", "", None]:
            logger.warning(f"[/api/verify-broker] Validation failed for email '{email}': API key or secret key is a placeholder or empty.")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="API key and secret key are required and must not be placeholders"
            )
        logger.info(f"[/api/verify-broker] Validation passed for email '{email}'.")

        try:
            # Initialize BingX client
            logger.info(f"[/api/verify-broker] Initializing BingXClient for email '{email}' with received api_key.")
            client = BingXClient(api_key, secret_key)
            
            # Get assets balance
            response = client.get_user_balance()
            
            # Check if the response contains valid data
            if not response or not response.get("data"):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Failed to fetch balance from broker"
                )
            
            # Process the balance data
            balances = {}
            for asset in response.get("data", []):
                asset_name = asset.get("asset")
                total = float(asset.get("balance", 0))
                
                if total > 0:  # Only include assets with balance > 0
                    balances[asset_name] = total
            
            logger.info(f"[/api/verify-broker] Fetched balances for email '{email}': {balances}")

            # Update user's balance in database
            update_data = {
                "balances": balances,
                "api_verified": True,
                "updated_at": datetime.now()
            }
            
            # If this is the first time verifying, set broker name
            if not user.get("broker_name"):
                update_data["broker_name"] = "BingX"
            

             # Create or update broker connection
            broker_connection = {
                "userId": email,
                "broker_name": "BingX",
                "brokerId":brokerId,
                "credentials": {
                    "app_name": app_name,
                    "api_key": api_key,      # Raw key from request
                    "secret_key": secret_key # Raw secret from request
                },
                "is_active": True,
                "created_at": user.get("created_at", datetime.now()),
                "updated_at": datetime.now()
            }
            logger.info(f"[/api/verify-broker] Broker connection dict initialized for email '{email}': {{'app_name': '{broker_connection['credentials']['app_name']}', 'api_key': '***', 'secret_key': '***'}}") # Avoid logging raw keys here
            
            # Encrypt the credentials
            logger.info(f"[/api/verify-broker] Encrypting credentials for email '{email}' - input api_key='{api_key[:5]}...', secret_key='{secret_key[:5]}...' (partially shown for security)")
            encrypted = encrypt_api_credentials(api_key, secret_key)
            logger.info(f"[/api/verify-broker] Encrypted credentials for email '{email}': {{'api_key': '{encrypted['api_key'][:10]}...', 'secret_key': '{encrypted['api_secret'][:10]}...'}}")

            broker_connection["credentials"]["api_key"] = encrypted["api_key"]
            broker_connection["credentials"]["secret_key"] = encrypted["api_secret"]
            logger.info(f"[/api/verify-broker] Broker connection dict after encryption for email '{email}': {{'app_name': '{broker_connection['credentials']['app_name']}', 'api_key': '{broker_connection['credentials']['api_key'][:10]}...', 'secret_key': '{broker_connection['credentials']['secret_key'][:10]}...'}}")
            
            # Update or insert the broker connection
            logger.info(f"[/api/verify-broker] Updating/inserting broker_connections for email '{email}'. Full broker_connection (credentials partially shown): {{'userId': '{broker_connection['userId']}', 'broker_name': '{broker_connection['broker_name']}', 'brokerId': '{broker_connection['brokerId']}', 'credentials': {{'app_name': '{broker_connection['credentials']['app_name']}', 'api_key': '{broker_connection['credentials']['api_key'][:10]}...', 'secret_key': '{broker_connection['credentials']['secret_key'][:10]}...'}}, 'is_active': {broker_connection['is_active']}}}")
            await storage.db.broker_connections.update_one(
                {"userId": email, "broker_name": "BingX"},
                {"$set": broker_connection},
                upsert=True
            )
            logger.info(f"[/api/verify-broker] broker_connections updated/inserted for email '{email}'.")
            
            # Update user document
            await storage.users_collection.update_one(
                {"email": email},
                {"$set": update_data}
            )
            
            # return {
            #     "status": "success",
            #     "message": "Broker verified and balance updated successfully",
            #     "balances": balances
            # }

            return {"success": True, "data": balances}

            
            
        except Exception as e:
            logger.error(f"Error verifying broker: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to verify broker: {str(e)}"
            )
            
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Unexpected error in verify_broker: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred"
        )


@app.get("/api/cryptolive-data")
@cache(expire=30)  # Cache for 30 seconds
@app.get("/cryptolive-data")
async def get_cryptolive_data():
    """
    Get live cryptocurrency data in the format:
    [
        { symbol: "BTC", price: 2448.6, change: -0.03 },
        { symbol: "ETH", price: 2448.6, change: 0.03 },
        { symbol: "SOL", price: 2448.6, change: 0.03 }
    ]
    """
    try:
        API = "TsYIfUmV2aKpaa5XWJfvSrkF5kGvtCaQjONVLKl6JN5ELEa93g8JTjV2ThP6s9ewIMqKdTCmUCM1O2E2jyA"
        SECRET = "a8hWnhCbQ8EMtlOTwN6zXuKGeMjOSJqhaR4y9y8rhLqpkYaJplxLcYtkflxdBmlsz7YIA4KuQr3Ey5PKjw"

        # Initialize BingX client directly
        client = BingXClient(API, SECRET)
        
        # Define symbols and their display names
        symbols = {
            'BTC-USDT': 'BTC',
            'ETH-USDT': 'ETH',
            'SOL-USDT': 'SOL'
        }
        
        response = []
        for symbol, display_name in symbols.items():
            try:
                # Get ticker data for each symbol
                data = client.get_quote_ticker(symbol=symbol)
                ticker = data['data']
                if ticker and 'lastPrice' in ticker and 'priceChangePercent' in ticker:
                    response.append({
                        'symbol': display_name,
                        'price': float(ticker['lastPrice']),
                        'change': float(ticker['priceChangePercent'])
                    })
                else:
                    response.append({
                        'symbol': display_name,
                        'price': 0,
                        'change': 0
                    })
            except Exception as e:
                logger.error(f"Error fetching data for {symbol}: {str(e)}")
                response.append({
                    'symbol': display_name,
                    'price': 0,
                    'change': 0
                })
        return MongoJSONResponse(content=response)
    except Exception as e:
        logger.error(f"Error in get_cryptolive_data: {str(e)}")
        # Return empty data for all symbols on error
        return [
            {'symbol': 'BTC', 'price': 0, 'change': 0},
            {'symbol': 'ETH', 'price': 0, 'change': 0},
            {'symbol': 'SOL', 'price': 0, 'change': 0}
        ]


@app.get("/api/deployed-strategies", tags=["strategies"])
@app.get("/deployed-strategies")
async def get_deployed_strategies(email: str = Query(..., description="Email of the user whose strategies to retrieve")):
    try:
        res = []
        user = await storage.users_collection.find_one({"email": email},)
        if user:
            stratigies = user.get("strategies", [])
            print("user",user)
            print("stratigies",stratigies)
            for i in stratigies:
                st = await storage.strategies_collection.find_one({"name": i})
                print("st",st)
                if st:
                    res.append(st)

            return res
        return []  
    except Exception as e:
        logger.error(f"Error fetching deployed strategies: {str(e)}")
        return []



@app.post("/api/add-strategy", tags=["strategies"])
@app.post("/add-strategy", tags=["strategies"])
async def add_strategy(email: str, strategy_name: str):
    try:
        print(email,strategy_name)
        user = await storage.users_collection.find_one({"email": email})
        if user:
            # Check if the user already has the strategy
            if strategy_name in user.get("strategies", []):
                return {"message": "Strategy already added"}

            # Add the strategy to the user's list
            user["strategies"].append(strategy_name)
            print(user)
            await storage.users_collection.update_one({"email": email}, {"$set": user})
            return {"message": "Strategy added successfully"}

        return {"message": "User not found"}
    except Exception as e:
        logger.error(f"Error adding strategy: {str(e)}")
        return {"message": "Error adding strategy"}


# Example routes for testing
@app.get("/", tags=["root"])
async def root():
    """Root endpoint to verify API is running"""
    return {"message": "Welcome to Crypto Trading API"}


@app.get("/api/strategies", tags=["strategies"])
@app.get("/strategies", tags=["strategies"])
async def get_strategies():
    """Get list of available trading strategies"""
    Strategies = await db.Strategies.find({},{"_id":0}).to_list(100)
    return serialize_doc(Strategies)




@app.get("/api/markets", tags=["markets"])
@app.get("/markets", tags=["markets"])
async def get_markets():
    """Get list of available trading markets"""
    return [
        {"symbol": "BTC/USDT", "type": "spot", "status": "active"},
        {"symbol": "ETH/USDT", "type": "spot", "status": "active"},
        {"symbol": "BNB/USDT", "type": "spot", "status": "active"}
    ]


@app.post("/api/positions")
@app.post("/positions")
async def get_positions(email:str, skip: int = Query(0, description="Number of items to skip"), limit: int = Query(100, description="Number of items to return"), start_time: Optional[int] = Query(None, description="Start timestamp for filtering"), end_time: Optional[int] = Query(None, description="End timestamp for filtering")):
    try:
        # Get positions for the authenticated user
        if not email:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="User email not found"
            )

        cred = await db.broker_connections.find_one({"userId":email}, {"credentials": 1, "_id": 0}) 
        if not cred:
             raise HTTPException(
                 status_code=status.HTTP_404_NOT_FOUND,
                 detail="Broker credentials not found"
             )

        credentials = cred['credentials']
        # print(credentials)

        dd_cred = decrypt_api_credentials(credentials['api_key'],credentials['secret_key'])

        client = BingXClient(dd_cred['api_key'], dd_cred['secret_key'])
        # Fetch position history for specified symbols
        positions = []
        symbols = ["BTC-USDT", "ETH-USDT", "SOL-USDT"]

        if start_time is None or start_time == 0:
            start_time = int(time.time() * 1000) - 30 * 24 * 60 * 60 * 1000
        if end_time is None or end_time == 0:
            end_time = int(time.time() * 1000)
        if limit is None or limit == 0:
            limit = 500
        
        # Pagination and timestamp filtering
        for symbol in symbols:
            try:
                # Get position history with pagination and timestamp filtering
                
                pos = client.get_position_history(
                    symbol=symbol,
                    startTime=start_time,
                    endTime=end_time,
                    limit=limit
                )

                print(pos)
                
                if pos['data'] and pos['data']['positionHistory']:
                    positions.extend(pos['data']['positionHistory'])
                
                # Handle pagination if more data is available
                while len(positions) < limit and pos.get('has_more', False):
                    pos = client.get_position_history(
                        symbol=symbol,
                        startTime=pos['next_start_time'],
                        endTime=end_time,
                        limit=limit - len(positions)
                    )
                    if pos['data'] and pos['data']['positionHistory']:
                        positions.extend(pos['data']['positionHistory'])
            
            except Exception as e:
                logger.error(f"Failed to get position history for {symbol}: {str(e)}")
                continue
        
        return serialize_doc(positions[:limit])

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching positions: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch positions"
        )
 
@app.post("/api/live-positions")
@app.post("/live-positions")
async def get_live_positions(email:str):
    try:
        if not email:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="User email not found" 
            )
            
        cred = await db.broker_connections.find_one({"userId":email}, {"credentials": 1, "_id": 0}) 
        if not cred:
             raise HTTPException(
                 status_code=status.HTTP_404_NOT_FOUND,
                 detail="Broker credentials not found"
             )

        credentials = cred['credentials']

        dd_cred = decrypt_api_credentials(credentials['api_key'],credentials['secret_key'])

        # print(dd_cred)

        # API = "TsYIfUmV2aKpaa5XWJfvSrkF5kGvtCaQjONVLKl6JN5ELEa93g8JTjV2ThP6s9ewIMqKdTCmUCM1O2E2jyA"
        # SECRET = "a8hWnhCbQ8EMtlOTwN6zXuKGeMjOSJqhaR4y9y8rhLqpkYaJplxLcYtkflxdBmlsz7YIA4KuQr3Ey5PKjw"


        # Initialize BingX client directly 
        client = BingXClient(dd_cred['api_key'], dd_cred['secret_key'])

        pos = client.get_positions()
        print(pos)
        return pos['data'] 

    except HTTPException:
        raise 
    except Exception as e:
        logger.error(f"Error fetching positions: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch positions"
        )
        


@app.get("/api/user-balance")
@app.get("/user-balance")
async def get_user_balance(email: str):
    try:
        if not email:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="User email not found"
            )

        cred = await db.broker_connections.find_one({"userId":email}, {"credentials": 1, "_id": 0}) 
        if not cred:
             raise HTTPException(
                 status_code=status.HTTP_404_NOT_FOUND,
                 detail="Broker credentials not found"
             )

        credentials = cred['credentials']

        dd_cred = decrypt_api_credentials(credentials['api_key'],credentials['secret_key'])

        client = BingXClient(dd_cred['api_key'], dd_cred['secret_key'])

        balance = client.get_user_balance()

        return float(balance['data'][0]['balance'])

    except HTTPException:
        raise 
    except Exception as e:
        logger.error(f"Error fetching balance: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch balance"
        )


@app.get("/api/user-pnl")
@app.get("/user-pnl")
async def get_user_pnl(email: str = Query(..., description="Email of the user whose P&L to retrieve")):
    try:
        if not email or not isinstance(email, str):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Valid email parameter is required"
            )
            
        # Ensure email is properly URL decoded
        import urllib.parse
        email = urllib.parse.unquote(email)
        
        # Get positions using the working query
        positions = await client.clientPositions[email].find(
            {"netProfit": {"$exists": True}}, 
            {"_id": 0}
        ).to_list(100)
        
        if not positions:
            return 0.0
            
        # Calculate sum using pandas as in the working example
        df = pd.DataFrame(positions)
        total_pnl = df.netProfit.astype(float).sum()
        
        return float(total_pnl)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error calculating P&L for user {email}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to calculate P&L: {str(e)}"
        )


# API health check endpoint
@app.get("/health")
@app.get("/api/health")
async def health_check():
    try:
        # Check MongoDB connection
        await client.admin.command('ping')
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "error": str(e)}
        )




from contextlib import asynccontextmanager



# Modify your lifespan function like this:

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup event
    logger.info("Starting up the API server")
    
    try:
        FastAPICache.init(InMemoryBackend(), prefix="fastapi-cache")

        # Create indexes for better query performance, but handle existing indexes gracefully
        try:
            await db.users.create_index([('email', 1)], unique=True)
        except Exception as e:
            logger.warning(f"Index on email already exists: {str(e)}")
        
        try:
            await db.users.create_index([('google_id', 1)])
        except Exception as e:
            logger.warning(f"Index on google_id already exists: {str(e)}")
        
        # This is the index causing the error - add unique parameter to match existing index
        try:
            await db.users.create_index([('referral_code', 1)], unique=True)
        except Exception as e:
            logger.warning(f"Index on referral_code already exists: {str(e)}")
        
        try:
            await db.strategies.create_index([('userId', 1)])
            await db.strategies.create_index([('name', 1)])
        except Exception as e:
            logger.warning(f"Strategy indexes already exist: {str(e)}")
        
        try:
            await db.trades.create_index([('userId', 1)])
            await db.trades.create_index([('symbol', 1)])
            await db.trades.create_index([('timestamp', -1)])
        except Exception as e:
            logger.warning(f"Trade indexes already exist: {str(e)}")
        
        try:
            await db.positions.create_index([('symbol', 1)])
            await db.positions.create_index([('positionSide', 1)])
        except Exception as e:
            logger.warning(f"Position indexes already exist: {str(e)}")
        
        logger.info("Database indexes created or verified successfully")
    except Exception as e:
        logger.error(f"Failed to create indexes: {str(e)}")
    
    yield
    
    # Shutdown event
    logger.info("Shutting down the API server")
    # Close MongoDB connection pool
    client.close()







if __name__ == "__main__":
    import uvicorn
    import socket
    import os
    
    
    API = "TsYIfUmV2aKpaa5XWJfvSrkF5kGvtCaQjONVLKl6JN5ELEa93g8JTjV2ThP6s9ewIMqKdTCmUCM1O2E2jyA"
    SECRET = "a8hWnhCbQ8EMtlOTwN6zXuKGeMjOSJqhaR4y9y8rhLqpkYaJplxLcYtkflxdBmlsz7YIA4KuQr3Ey5PKjw"

    credentils = {
        "api_key": API,
        "secret_key": SECRET,
    }

    my_broker = MyBroker(Brokers.BingX, credentils)

    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    if current_dir not in os.sys.path:
        os.sys.path.append(current_dir)
    
    
    def find_available_port(start_port=8000, max_port=8100):
        for port in range(start_port, max_port):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                try:
                    s.bind(('0.0.0.0', port))
                    return port
                except OSError:
                    continue
        return None
    
    port = 8000     ##find_available_port()
    if not port:
        logger.error("Could not find an available port between 8000 and 8100")
        exit(1)
        sys.exit(1)
    
    logger.info(f"Starting server on port {port}")
    
    project_dir = os.path.dirname(current_dir)
    
    uvicorn.run(
        "backend:app",  # Use module path relative to code directory "34.131.43.0",  #
        host= "0.0.0.0",
        port=port,
        reload=False,    # Disable auto-reload
        workers=1,       # Number of worker processes
        # loop="uvloop",   # Faster event loop implementation
        http="httptools", # Faster HTTP protocol implementation
        log_level="info",
        timeout_keep_alive=65,  # Keep-alive timeout
        access_log=False
    )



