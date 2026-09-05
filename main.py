import os
import httpx
from fastapi import FastAPI, Request

app = FastAPI()

CLIENT_ID = os.getenv("THREADS_CLIENT_ID")
CLIENT_SECRET = os.getenv("THREADS_CLIENT_SECRET")
REDIRECT_URI = os.getenv("THREADS_REDIRECT_URI")

@app.get("/")
async def root():
    return {"status": "ok"}

@app.get("/callback")
async def callback(request: Request):
    code = request.query_params.get("code")
    if not code:
        return {"error": "no code in request"}

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://graph.threads.net/oauth/access_token",
            data={
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "grant_type": "authorization_code",
                "redirect_uri": REDIRECT_URI,
                "code": code,
            },
        )
        short_lived = resp.json()

    if "access_token" not in short_lived:
        return {"error": "token exchange failed", "details": short_lived}

    async with httpx.AsyncClient() as client:
        resp2 = await client.get(
            "https://graph.threads.net/access_token",
            params={
                "grant_type": "th_exchange_token",
                "client_secret": CLIENT_SECRET,
                "access_token": short_lived["access_token"],
            },
        )
        long_lived = resp2.json()

    return {"short_lived": short_lived, "long_lived": long_lived}
