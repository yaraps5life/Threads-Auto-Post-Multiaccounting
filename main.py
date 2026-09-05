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


@app.post("/publish")
async def publish_posts():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT pq.id, pq.account_id, pq.content, a.threads_user_id, a.access_token, a.persona_name
        FROM posts_queue pq
        JOIN accounts a ON a.id = pq.account_id
        WHERE pq.status = 'draft';
    """)
    drafts = cur.fetchall()

    results = []

    for post_id, account_id, content, threads_user_id, access_token, persona_name in drafts:
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                container_resp = await client.post(
                    f"https://graph.threads.net/v1.0/{threads_user_id}/threads",
                    data={
                        "media_type": "TEXT",
                        "text": content,
                        "access_token": access_token,
                    },
                )
                container_data = container_resp.json()

            if "id" not in container_data:
                cur.execute(
                    "UPDATE posts_queue SET status = 'failed' WHERE id = %s;",
                    (post_id,),
                )
                conn.commit()
                results.append({"post_id": post_id, "persona": persona_name, "error": container_data})
                continue

            creation_id = container_data["id"]

            async with httpx.AsyncClient(timeout=30) as client:
                publish_resp = await client.post(
                    f"https://graph.threads.net/v1.0/{threads_user_id}/threads_publish",
                    data={
                        "creation_id": creation_id,
                        "access_token": access_token,
                    },
                )
                publish_data = publish_resp.json()

            if "id" not in publish_data:
                cur.execute(
                    "UPDATE posts_queue SET status = 'failed' WHERE id = %s;",
                    (post_id,),
                )
                conn.commit()
                results.append({"post_id": post_id, "persona": persona_name, "error": publish_data})
                continue

            threads_post_id = publish_data["id"]

            cur.execute(
                """UPDATE posts_queue
                   SET status = 'published', published_at = NOW(), threads_post_id = %s
                   WHERE id = %s;""",
                (threads_post_id, post_id),
            )
            conn.commit()

            results.append({"post_id": post_id, "persona": persona_name, "threads_post_id": threads_post_id, "status": "published"})

        except Exception as e:
            cur.execute(
                "UPDATE posts_queue SET status = 'failed' WHERE id = %s;",
                (post_id,),
            )
            conn.commit()
            results.append({"post_id": post_id, "persona": persona_name, "error": str(e)})

    cur.close()
    conn.close()

    return {"published": results}


@app.post("/generate-and-publish")
async def generate_and_publish():
    gen_result = await generate_posts()
    pub_result = await publish_posts()
    return {"generated": gen_result, "published": pub_result}


@app.post("/update-token")
async def update_token(request: Request):
    body = await request.json()
    account_id = body.get("account_id")
    access_token = body.get("access_token")
    threads_user_id = body.get("threads_user_id")

    if not account_id or not access_token:
        return {"error": "account_id and access_token are required"}

    conn = get_conn()
    cur = conn.cursor()

    if threads_user_id:
        cur.execute(
            "UPDATE accounts SET access_token = %s, threads_user_id = %s WHERE id = %s;",
            (access_token, threads_user_id, account_id),
        )
    else:
        cur.execute(
            "UPDATE accounts SET access_token = %s WHERE id = %s;",
            (access_token, account_id),
        )

    conn.commit()
    cur.close()
    conn.close()

    return {"status": "updated", "account_id": account_id}
