# Crypto Trading API Backend Documentation

## Overview
This document explains the architecture and workflow of the Crypto Trading API backend system, which is built using FastAPI and MongoDB for high-performance cryptocurrency trading operations.

## Tech Stack
- **Framework**: FastAPI
- **Database**: MongoDB (with Motor for async operations)
- **Authentication**: Custom session-based authentication
- **Security**: CORS, Rate Limiting, Password Hashing
- **Email**: SMTP for OTP verification
- **Encryption**: Fernet for API credentials

## Core Components

### 1. Server Configuration
- FastAPI instance configured for high throughput
- Production/Development environment detection
- API documentation endpoints (disabled in production)
- CORS middleware with specific origin allowance
- Session middleware for user authentication

### 2. Database Layer
- MongoDB integration using Motor (async driver)
- Collections:
  - users
  - strategies
  - positions
  - portfolio_snapshots
  - trades

### 3. Security Features
- Rate limiting (100 requests per minute per IP)
- Password hashing using PassLib
- API credential encryption using Fernet
- Session-based authentication
- CORS protection with specific origin allowance

### 4. User Management
- Registration workflow:
  1. Initial signup with email
  2. OTP verification via email
  3. Complete registration with user details
- Authentication:
  - Email-based signin
  - Session management
  - User verification middleware

### 5. Trading Features
- Strategy management
- Position tracking
- Portfolio snapshots
- Trade execution and monitoring
- Broker integration (BingX)

## Authentication Flow
1. User initiates signup with email
2. System generates and sends OTP via email
3. User verifies OTP
4. User completes registration with full details
5. On signin, system creates a session
6. Subsequent requests use session for authentication

## Data Models

### User Model
- name
- email
- google_id
- phone
- password (hashed)
- status
- referral_code
- invited_by
- broker details
- admin status

### Broker Connection
- User ID
- Broker name
- API credentials (encrypted)
- Active status
- Timestamps

### Strategy Configuration
- Name
- Type
- Leverage
- Margin
- Description
- Active status
- Timestamps

## API Endpoints

### Authentication
- `/signup`: Initial user registration
- `/verify-otp`: Email verification
- `/complete-registration`: Complete user profile
- `/signin`: User authentication
- `/signout`: Session termination

### Trading Operations
- Strategy management endpoints
- Market data endpoints
- Position management
- Trade execution
- Portfolio tracking

## Security Measures
1. Rate limiting to prevent abuse
2. Encrypted storage of API credentials
3. Session-based authentication
4. CORS protection
5. Password hashing
6. OTP verification for registration

## Performance Optimizations
1. Async database operations
2. Connection pooling
3. Session caching
4. Background task processing
5. Optimized MongoDB queries

## Error Handling
- Structured error responses
- Comprehensive logging
- Request tracking
- Health check endpoint

## Monitoring
- Request logging middleware
- Performance metrics
- Health check endpoint
- Error tracking

## Development Guidelines
1. Use async/await for database operations
2. Implement proper error handling
3. Follow REST API best practices
4. Maintain security protocols
5. Document API changes
6. Test thoroughly before deployment

## Environment Configuration
- Development mode features:
  - API documentation
  - OpenAPI schema
  - Detailed error messages
- Production mode security:
  - Disabled documentation
  - Limited error information
  - Strict CORS policy
