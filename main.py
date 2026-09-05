import os
import random
import asyncio
import httpx
import psycopg2
from fastapi import FastAPI, Request

app = FastAPI()

CLIENT_ID = os.getenv("THREADS_CLIENT_ID")
CLIENT_SECRET = os.getenv("THREADS_CLIENT_SECRET")
REDIRECT_URI = os.getenv("THREADS_REDIRECT_URI")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

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
    account_id = request.query_params.get("account_id")

    if not code:
        return {"error": "no code in request"}

    actual_redirect_uri = f"{REDIRECT_URI}?account_id={account_id}" if account_id else REDIRECT_URI

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://graph.threads.net/oauth/access_token",
            data={
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "grant_type": "authorization_code",
                "redirect_uri": actual_redirect_uri,
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

    threads_user_id = short_lived.get("user_id")
    saved_to_db = None

    if account_id and "access_token" in long_lived and threads_user_id:
        try:
            conn = get_conn()
            cur = conn.cursor()
            cur.execute(
                """UPDATE accounts
                   SET access_token = %s, threads_user_id = %s, token_expires_at = NOW() + INTERVAL '60 days'
                   WHERE id = %s;""",
                (long_lived["access_token"], str(threads_user_id), account_id),
            )
            conn.commit()
            cur.close()
            conn.close()
            saved_to_db = f"account_id {account_id} updated directly in DB"
        except Exception as e:
            saved_to_db = f"DB write failed: {e}"

    return {"short_lived": short_lived, "long_lived": long_lived, "saved_to_db": saved_to_db}


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
        text = data["candidates"][0]["content"]["parts"][0]["text"].strip()
        if len(text) > 500:
            text = text[:497].rsplit(" ", 1)[0] + "..."
        return text
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

    for i, (post_id, account_id, content, threads_user_id, access_token, persona_name) in enumerate(drafts):
        if i > 0:
            jitter_seconds = random.randint(30, 240)
            await asyncio.sleep(jitter_seconds)
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

async def fetch_insights(media_id: str, access_token: str) -> dict:
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.get(
            f"https://graph.threads.net/v1.0/{media_id}/insights",
            params={
                "metric": "likes,replies,reposts,quotes,views",
                "access_token": access_token,
            },
        )
        return resp.json()


async def collect_metrics_last_24h() -> list[dict]:
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT pq.id, pq.threads_post_id, a.access_token, a.persona_name
        FROM posts_queue pq
        JOIN accounts a ON a.id = pq.account_id
        WHERE pq.status = 'published'
          AND pq.published_at >= NOW() - INTERVAL '24 hours'
          AND pq.threads_post_id IS NOT NULL;
    """)
    rows = cur.fetchall()

    summary = []

    for post_id, threads_post_id, access_token, persona_name in rows:
        data = await fetch_insights(threads_post_id, access_token)

        metrics = {"likes": 0, "replies": 0, "reposts": 0, "quotes": 0, "views": 0}
        if "data" in data:
            for item in data["data"]:
                name = item.get("name")
                values = item.get("values", [])
                if values and name in metrics:
                    metrics[name] = values[0].get("value", 0)

        cur.execute(
            """INSERT INTO post_metrics (post_id, likes, replies, reposts)
               VALUES (%s, %s, %s, %s);""",
            (post_id, metrics["likes"], metrics["replies"], metrics["reposts"]),
        )
        conn.commit()

        summary.append({
            "persona": persona_name,
            "post_id": post_id,
            "likes": metrics["likes"],
            "replies": metrics["replies"],
            "reposts": metrics["reposts"],
            "views": metrics["views"],
        })

    cur.close()
    conn.close()
    return summary


def format_summary(summary: list[dict]) -> str:
    if not summary:
        return "За последние 24 часа опубликованных постов не найдено."

    lines = ["📊 Метрики за последние 24 часа:\n"]
    total_likes = total_replies = total_reposts = total_views = 0

    for item in summary:
        lines.append(
            f"• {item['persona']}: 👁 {item['views']} | ❤️ {item['likes']} | 💬 {item['replies']} | 🔁 {item['reposts']}"
        )
        total_likes += item["likes"]
        total_replies += item["replies"]
        total_reposts += item["reposts"]
        total_views += item["views"]

    lines.append(f"\nИтого: 👁 {total_views} | ❤️ {total_likes} | 💬 {total_replies} | 🔁 {total_reposts}")
    return "\n".join(lines)


async def send_telegram_message(chat_id: int, text: str):
    async with httpx.AsyncClient(timeout=20) as client:
        await client.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": text},
        )


@app.post("/telegram-webhook")
async def telegram_webhook(request: Request):
    update = await request.json()

    message = update.get("message", {})
    text = message.get("text", "")
    chat_id = message.get("chat", {}).get("id")

    if not chat_id:
        return {"ok": True}

    if text.strip() in ("/stats", "/dashboard"):
        await send_telegram_message(chat_id, "Собираю метрики за последние 24 часа...")
        summary = await collect_metrics_last_24h()
        reply = format_summary(summary)
        await send_telegram_message(chat_id, reply)
    else:
        await send_telegram_message(chat_id, "Доступные команды:\n/stats — метрики за последние 24 часа")

    return {"ok": True}
