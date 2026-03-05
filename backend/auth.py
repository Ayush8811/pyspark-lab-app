import os
from datetime import datetime, timedelta
from passlib.context import CryptContext
import jwt

SECRET_KEY = os.getenv("JWT_SECRET", "super-secret-local-key-change-in-prod")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 * 7 # 1 week

# By default, passlib has a compatibility bug with bcrypt >= 4.0.0 during its internal check sequence.
# To bypass, we can use raw bcrypt instead of the passlib wrapper for hashing to avoid truncation crashes.
import bcrypt

def verify_password(plain_password: str, hashed_password: str):
    # Check raw bcrypt
    if isinstance(hashed_password, str):
        hashed_password = hashed_password.encode('utf-8')
    return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password)

def get_password_hash(password: str):
    # Use raw bcrypt, avoiding the passlib bug
    salt = bcrypt.gensalt()
    pwd_bytes = password.encode('utf-8')
    # bcrypt truncates to 72 bytes max internally
    hashed = bcrypt.hashpw(pwd_bytes[:72], salt)
    return hashed.decode('utf-8')

def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def decode_access_token(token: str):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except jwt.PyJWTError:
        return None
