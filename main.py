import os
import httpx
import psycopg2
from fastapi import FastAPI, Request

app = FastAPI()

CLIENT_ID = os.getenv("THREADS_CLIENT_ID")
CLIENT_SECRET = os.getenv("THREADS_CLIENT_SECRET")
REDIRECT_URI = os.getenv("THREADS_REDIRECT_URI")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

GEMINI_MODEL = "gemini-3.6-flash"
GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"


def get_conn():
    return psycopg2.connect(DATABASE_URL)


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


async def call_gemini(system_prompt: str, recent_posts: list[str]) -> str:
    recent_context = ""
    if recent_posts:
        recent_context = "Твои последние посты (не повторяйся, держи стиль):\n" + "\n---\n".join(recent_posts)

    user_prompt = f"{recent_context}\n\nСгенерируй один новый пост для Threads по своей роли. Только текст поста, без кавычек и пояснений."

    payload = {
        "system_instruction": {"parts": [{"text": system_prompt}]},
        "contents": [{"role": "user", "parts": [{"text": user_prompt}]}],
    }

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{GEMINI_URL}?key={GEMINI_API_KEY}",
            json=payload,
        )
        data = resp.json()

    try:
        return data["candidates"][0]["content"]["parts"][0]["text"].strip()
    except (KeyError, IndexError):
        raise ValueError(f"Gemini response error: {data}")


@app.post("/generate")
async def generate_posts():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT id, persona_name, persona_system_prompt FROM accounts WHERE is_active = true;")
    accounts = cur.fetchall()

    results = []

    for account_id, persona_name, system_prompt in accounts:
        if not system_prompt:
            results.append({"account_id": account_id, "persona": persona_name, "error": "no system prompt"})
            continue

        cur.execute(
            "SELECT content FROM posts_queue WHERE account_id = %s ORDER BY created_at DESC LIMIT 3;",
            (account_id,),
        )
        recent = [row[0] for row in cur.fetchall()]

        try:
            new_post = await call_gemini(system_prompt, recent)
        except Exception as e:
            results.append({"account_id": account_id, "persona": persona_name, "error": str(e)})
            continue

        cur.execute(
            "INSERT INTO posts_queue (account_id, content, status) VALUES (%s, %s, 'draft') RETURNING id;",
            (account_id, new_post),
        )
        post_id = cur.fetchone()[0]
        conn.commit()

        results.append({"account_id": account_id, "persona": persona_name, "post_id": post_id, "content": new_post})

    cur.close()
    conn.close()

    return {"generated": results}


@app.get("/posts")
async def list_posts(status: str = None):
    conn = get_conn()
    cur = conn.cursor()

    if status:
        cur.execute(
            "SELECT id, account_id, content, status, created_at FROM posts_queue WHERE status = %s ORDER BY created_at DESC;",
            (status,),
        )
    else:
        cur.execute("SELECT id, account_id, content, status, created_at FROM posts_queue ORDER BY created_at DESC;")

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return {
        "posts": [
            {"id": r[0], "account_id": r[1], "content": r[2], "status": r[3], "created_at": str(r[4])}
            for r in rows
        ]
    }
