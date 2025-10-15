import os
import time
import math
import datetime as dt
import requests
from typing import Iterable, List, Dict, Any
from supabase import create_client, Client
from postgrest.exceptions import APIError

WB_API_URL = "https://statistics-api.wildberries.ru/api/v1/supplier/stocks"

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE"]  # service_role key
WB_API_KEY    = os.environ["WB_API_KEY"]

# Параметры
LOOKBACK_MINUTES = int(os.environ.get("LOOKBACK_MINUTES", "31"))  # перекрытие CRON 30 мин
BATCH_SIZE       = int(os.environ.get("BATCH_SIZE", "1000"))
MAX_RETRIES      = int(os.environ.get("MAX_RETRIES", "3"))

def rfc3339(dtobj: dt.datetime) -> str:
    # WB принимает "YYYY-MM-DDTHH:MM:SS"
    return dtobj.replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%S")

def fetch_stocks(date_from_str: str) -> List[Dict[str, Any]]:
    headers = {"Authorization": WB_API_KEY}
    params  = {"dateFrom": date_from_str}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(WB_API_URL, headers=headers, params=params, timeout=120)
            # 429/5xx — бэкоф
            if resp.status_code in (429, 500, 502, 503, 504):
                delay = min(30, 2 ** attempt)
                print(f"WB API {resp.status_code}, retry in {delay}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(delay)
                continue
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, list):
                raise ValueError(f"Unexpected WB response type: {type(data)}")
            return data
        except requests.RequestException as e:
            delay = min(30, 2 ** attempt)
            print(f"WB API error: {e}, retry in {delay}s (attempt {attempt}/{MAX_RETRIES})")
            time.sleep(delay)

    raise SystemExit("WB API: all retries failed")

def normalize_row(r: Dict[str, Any]) -> Dict[str, Any]:
    # ВАЖНО: last_change_ts отправляем СТРОКОЙ (или None) — PostgREST сам приведёт к timestamptz
    # Оставим строку из WB как есть, но если там есть 'Z' или миллисекунды — это ок для PG.
    last_change = r.get("lastChangeDate")
    return {
        "last_change_ts": last_change,  # строка, не datetime!
        "warehouse_name": r.get("warehouseName"),
        "supplier_article": r.get("supplierArticle"),
        "nm_id": r.get("nmId"),
        "barcode": r.get("barcode"),
        "quantity": r.get("quantity"),
        "quantity_full": r.get("quantityFull"),
        "category": r.get("category"),
        "subject": r.get("subject"),
        "brand": r.get("brand"),
        "tech_size": r.get("techSize"),
        "price": r.get("Price"),
        "discount": r.get("Discount"),
        "is_supply": r.get("isSupply"),
        "is_realization": r.get("isRealization"),
        "sc_code": r.get("SCCode"),
        # updated_at заполняется дефолтом в БД
    }

def chunked(iterable: Iterable[Any], size: int) -> Iterable[List[Any]]:
    buf = []
    for x in iterable:
        buf.append(x)
        if len(buf) == size:
            yield buf
            buf = []
    if buf:
        yield buf

def upsert_current(sb: Client, rows: List[Dict[str, Any]]) -> None:
    total = len(rows)
    if total == 0:
        print("No rows to upsert")
        return

    sent = 0
    for batch in chunked(rows, BATCH_SIZE):
        res = sb.table("wb_stocks_current") \
                .upsert(batch, on_conflict="nm_id,barcode,warehouse_name") \
                .execute()

        # APIResponse: проверяем error, data
        if getattr(res, "error", None):
            msg = getattr(res.error, "message", str(res.error))
            raise SystemExit(f"Upsert error: {msg}")

        # data может быть None (если return=minimal настроен на стороне PostgREST)
        affected = len(res.data) if res.data is not None else 0
        sent += len(batch)
        print(f"Upserted batch {len(batch)} (affected≈{affected}), progress {sent}/{total}")

def main():
    sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    # Берём LOOKBACK минут назад, чтобы захватить все изменения
    date_from = rfc3339(dt.datetime.utcnow() - dt.timedelta(minutes=LOOKBACK_MINUTES))
    print(f"Requesting WB stocks since {date_from} (UTC)")

    raw = fetch_stocks(date_from)
    rows = [normalize_row(r) for r in raw]

    # Фильтруем пустые nm_id/barcode/warehouse_name — они обязательны для PK
    cleaned = [
        x for x in rows
        if x.get("nm_id") is not None
        and x.get("barcode")
        and x.get("warehouse_name")
    ]

    dropped = len(rows) - len(cleaned)
    if dropped:
        print(f"Dropped {dropped} rows without required keys (nm_id/barcode/warehouse_name)")

    upsert_current(sb, cleaned)
    print(f"Done. Total processed: {len(cleaned)}")

if __name__ == "__main__":
    main()
