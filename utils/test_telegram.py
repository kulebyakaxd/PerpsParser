import os
import json
import requests

try:
    from dotenv import load_dotenv, find_dotenv  # type: ignore
    env_path = find_dotenv()
    if env_path:
        load_dotenv(env_path, override=True)
except Exception:
    pass


def mask_token(token: str) -> str:
    if not token:
        return ""
    if len(token) <= 8:
        return "***"
    return token[:4] + "***" + token[-4:]


def main():
    token = os.getenv("TELEGRAM_API", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()

    print("Config:")
    print("  TELEGRAM_API:", mask_token(token))
    print("  TELEGRAM_CHAT_ID:", chat_id or "<empty>")

    if not token:
        print("ERROR: TELEGRAM_API is empty")
        return
    if not chat_id:
        print("ERROR: TELEGRAM_CHAT_ID is empty")
        print("Hint: For direct bot chat, use your personal numeric user id (positive). Ensure you've sent /start to the bot.")
        return

    base = f"https://api.telegram.org/bot{token}"

    # getMe
    try:
        r = requests.get(f"{base}/getMe", timeout=15)
        print("getMe:", r.status_code, r.text[:300])
    except Exception as e:
        print("getMe error:", e)

    # sendMessage
    try:
        payload = {"chat_id": chat_id, "text": "🔎 Test from PerpsParser: direct send"}
        r = requests.post(f"{base}/sendMessage", json=payload, timeout=15)
        print("sendMessage:", r.status_code)
        try:
            print(json.dumps(r.json(), ensure_ascii=False)[:600])
        except Exception:
            print(r.text[:600])
    except Exception as e:
        print("sendMessage error:", e)


if __name__ == "__main__":
    main()


