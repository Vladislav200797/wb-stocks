# stocks_sync.py
# Загрузка оперативных остатков WB в одну таблицу Supabase (upsert).
# Поддерживает FULL_SYNC=1 для первой полной загрузки.

import os
import time
import datetime as dt
from typing import Iterable, List, Dict, Any

import requests
from supabase import create_client, Client

WB_API_URL = "https://statistics-api.wildberries.ru/api/v1/supplier/stocks"

# --- Обязательные переменные окружения ---
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE"]  # service_role key
WB_API_KEY   = os.environ["WB_API_KEY"]

# --- Настройки по умолчанию (можно переопределить через env) ---
LOOKBACK_MINUTES = int(os.environ.get("LOOKBACK_MINUTES", "31"))  # перекрытие 30-минутного интервала
BATCH_SIZE       = int(os.environ.get("BATCH_SIZE", "1000"))
MAX_RETRIES      = int(os.environ.get("MAX_RETRIES", "3"))
FULL_SYNC        = os.environ.get("FULL_SYNC", "0") == "1"         # один раз запустить с 1 для полной загрузки


def rfc3339(dtobj: dt.datetime) -> str:
    """WB принимает строку формата YYYY-MM-DDTHH:MM:SS (RFC3339 без миллисекунд)."""
    return dtobj.replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%S")


def fetch_stocks(date_from_str: str) -> List[Dict[str, Any]]:
    """Тянем остатки из WB с ретраями на 429/5xx."""
    headers = {"Authorization": WB_API_KEY}
    params = {"dateFrom": date_from_str}

    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(WB_API_URL, headers=headers, params=params, timeout=120)
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

        except Exception as e:
            last_err = e
            delay = min(30, 2 ** attempt)
            print(f"WB API error: {e!r}, retry in {delay}s (attempt {attempt}/{MAX_RETRIES})")
            time.sleep(delay)

    raise SystemExit(f"WB API: all retries failed. Last error: {last_err!r}")


def normalize_row(r: Dict[str, Any]) -> Dict[str, Any]:
    """
    Нормализуем поля под таблицу wb_stocks_current.
    last_change_ts оставляем строкой (PostgREST приведёт к timestamptz).
    """
    return {
        "last_change_ts": r.get("lastChangeDate"),
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
        # updated_at заполнится дефолтом в БД
    }


def chunked(iterable: Iterable[Any], size: int) -> Iterable[List[Any]]:
    buf: List[Any] = []
    for x in iterable:
        buf.append(x)
        if len(buf) == size:
            yield buf
            buf = []
    if buf:
        yield buf


def upsert_current(sb: Client, rows: List[Dict[str, Any]]) -> None:
    """Апсертим в wb_stocks_current батчами. Проверяем APIResponse.error."""
    total = len(rows)
    if total == 0:
        print("No rows to upsert")
        return

    sent = 0
    for batch in chunked(rows, BATCH_SIZE):
        res = sb.table("wb_stocks_current") \
                .upsert(batch, on_conflict="nm_id,barcode,warehouse_name") \
                .execute()

        # В supabase-py v2 возвращается APIResponse с полями .data / .error
        if getattr(res, "error", None):
            msg = getattr(res.error, "message", str(res.error))
            raise SystemExit(f"Upsert error: {msg}")

        affected = len(res.data) if res.data is not None else 0
        sent += len(batch)
        print(f"Upserted batch {len(batch)} (affected≈{affected}), progress {sent}/{total}")


def main() -> None:
    sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    if FULL_SYNC:
        # Самая ранняя дата, чтобы WB вернул все актуальные позиции
        date_from = "2019-06-20T00:00:00"
        print(f"FULL_SYNC=1 -> requesting COMPLETE stocks since {date_from}")
    else:
        date_from = rfc3339(dt.datetime.utcnow() - dt.timedelta(minutes=LOOKBACK_MINUTES))
        print(f"Incremental -> requesting since {date_from} (UTC)")

    raw = fetch_stocks(date_from)
    print(f"WB returned {len(raw)} rows")

    rows = [normalize_row(r) for r in raw]

    # Фильтруем строки без обязательных полей для PK
    cleaned: List[Dict[str, Any]] = []
    dropped_missing = 0
    missing_nm = 0
    missing_bc = 0
    missing_wh = 0

    for x in rows:
        ok_nm = x.get("nm_id") is not None
        ok_bc = bool(x.get("barcode"))
        ok_wh = bool(x.get("warehouse_name"))
        if ok_nm and ok_bc and ok_wh:
            cleaned.append(x)
        else:
            dropped_missing += 1
            if not ok_nm:
                missing_nm += 1
            if not ok_bc:
                missing_bc += 1
            if not ok_wh:
                missing_wh += 1

    if dropped_missing:
        print(
            f"Dropped {dropped_missing} rows missing PK fields "
            f"(nm_id missing: {missing_nm}, barcode missing: {missing_bc}, warehouse_name missing: {missing_wh})"
        )

    upsert_current(sb, cleaned)
    print(f"Done. Total processed: {len(cleaned)}")


if __name__ == "__main__":
    main()
