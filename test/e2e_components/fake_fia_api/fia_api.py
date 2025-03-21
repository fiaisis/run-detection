"""
Fake API app to be used for FastAPI
"""

from fastapi import FastAPI

from .router import ROUTER

app = FastAPI()

# This must be updated before exposing outside the vpn
ALLOWED_ORIGINS = ["*"]

app.include_router(ROUTER)
