# auth.py
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi import Depends, HTTPException, status
import secrets
import os
from config import settings

security = HTTPBasic()

def verify_basic_auth(credentials: HTTPBasicCredentials = Depends(security)):
    """
    Verify Basic Auth credentials for protected endpoints
    """
    correct_username = secrets.compare_digest(credentials.username, settings.API_USERNAME)
    correct_password = secrets.compare_digest(credentials.password, settings.API_PASSWORD)
    
    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username