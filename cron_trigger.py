import os
import sys
import httpx

APP_URL = os.getenv("APP_URL", "https://threads-auto-post-multiaccounting-production.up.railway.app")

def main():
    resp = httpx.post(f"{APP_URL}/generate-and-publish", timeout=600)
    print(resp.status_code, resp.text[:2000])

if __name__ == "__main__":
    main()
    sys.stdout.flush()
    os._exit(0)
