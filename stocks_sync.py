# stocks_sync.py
# Оперативные остатки WB через отчет Seller Analytics: warehouse_remains
# Создаём задачу (GET) -> ждём done -> скачиваем (GET) -> upsert в wb_stocks_current.

import os
import time
from typing import Iterable, List, Dict, Any, Optional

import requests
from supabase import create_client, Client

# --- ENV ---
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE"]  # service_role
WB_API_KEY   = os.environ["WB_API_KEY"]

# Базовые URL отчёта «остатки на складах»
SA_BASE = "https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains"

# Параметры отчёта (переопределяй через env при необходимости)
REPORT_LOCALE     = os.environ.get("REPORT_LOCALE", "ru")  # ru|en|zh
GROUP_BY_BRAND    = os.environ.get("GROUP_BY_BRAND", "false").lower() == "true"
GROUP_BY_SUBJECT  = os.environ.get("GROUP_BY_SUBJECT", "false").lower() == "true"
GROUP_BY_SA       = os.environ.get("GROUP_BY_SA", "true").lower() == "true"    # vendorCode
GROUP_BY_NM       = os.environ.get("GROUP_BY_NM", "true").lower() == "true"    # nmId
GROUP_BY_BARCODE  = os.environ.get("GROUP_BY_BARCODE", "true").lower() == "true"
GROUP_BY_SIZE     = os.environ.get("GROUP_BY_SIZE", "true").lower() == "true"

# Тайминги опроса
STATUS_POLL_SEC    = int(os.environ.get("STATUS_POLL_SEC", "3"))
STATUS_TIMEOUT_SEC = int(os.environ.get("STATUS_TIMEOUT_SEC", "180"))  # 3 мин на генерацию

BATCH_SIZE   = int(os.environ.get("BATCH_SIZE", "1000"))
MAX_RETRIES  = int(os.environ.get("MAX_RETRIES", "3"))                 # ретраи HTTP на 429/5xx


def _auth_headers() -> Dict[str, str]:
    return {"Authorization": WB_API_KEY}


def _retryable_request(method: str, url: str, **kwargs) -> requests.Response:
    last_err: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.request(method, url, timeout=120, **kwargs)
            if resp.status_code in (429, 500, 502, 503, 504):
                delay = min(30, 2 ** attempt)
                print(f"{method} {url} -> {resp.status_code}, retry in {delay}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(delay)
                continue
            resp.raise_for_status()
            return resp
        except Exception as e:
            last_err = e
            delay = min(30, 2 ** attempt)
            print(f"{method} {url} error: {e!r}, retry in {delay}s (attempt {attempt}/{MAX_RETRIES})")
            time.sleep(delay)
    raise SystemExit(f"HTTP failed after retries: {method} {url} last_err={last_err!r}")


def create_report_task() -> str:
    """
    Создаём задачу отчёта (ИМЕННО GET!).
    Ответ: {"data":{"taskId":"..."}}
    """
    params = {
        "locale": REPORT_LOCALE,
        "groupByBrand": str(GROUP_BY_BRAND).lower(),
        "groupBySubject": str(GROUP_BY_SUBJECT).lower(),
        "groupBySa": str(GROUP_BY_SA).lower(),
        "groupByNm": str(GROUP_BY_NM).lower(),
        "groupByBarcode": str(GROUP_BY_BARCODE).lower(),
        "groupBySize": str(GROUP_BY_SIZE).lower(),
    }
    url = SA_BASE
    resp = _retryable_request("GET", url, headers=_auth_headers(), params=params)
    js = resp.json()
    task_id = js.get("data", {}).get("taskId")
    if not task_id:
        raise SystemExit(f"Unexpected create_task response: {js}")
    print(f"Report task created: {task_id}")
    return task_id


def wait_task_done(task_id: str) -> None:
    """
    Пуллим статус до 'done' (или 'failed') с таймаутом.
    GET /api/v1/warehouse_remains/tasks/{task_id}/status
    """
    url = f"{SA_BASE}/tasks/{task_id}/status"
    deadline = time.time() + STATUS_TIMEOUT_SEC
    while True:
        resp = _retryable_request("GET", url, headers=_auth_headers())
        js = resp.json()
        status = js.get("data", {}).get("status")
        print(f"task {task_id} status: {status}")
        if status == "done":
            return
        if status == "failed":
            raise SystemExit(f"Report task {task_id} failed")
        if time.time() > deadline:
            raise SystemExit(f"Report task {task_id} timeout after {STATUS_TIMEOUT_SEC}s")
        time.sleep(STATUS_POLL_SEC)


def download_report(task_id: str) -> List[Dict[str, Any]]:
    """
    Скачиваем готовый отчёт.
    GET /api/v1/warehouse_remains/tasks/{task_id}/download
    """
    url = f"{SA_BASE}/tasks/{task_id}/download"
    resp = _retryable_request("GET", url, headers=_auth_headers())
    data = resp.json()
    if not isinstance(data, list):
        raise SystemExit(f"Unexpected download payload: {type(data)}")
    print(f"Report rows: {len(data)}")
    return data


def flatten_rows(report_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Разворачиваем массив warehouses в плоские строки по складам.
    Пустые склады (quantity=0) WB не присылает — их пропускаем.
    """
    flat: List[Dict[str, Any]] = []
    for item in report_rows:
        nm_id = item.get("nmId")
        barcode = item.get("barcode")
        tech_size = item.get("techSize")
        brand = item.get("brand")
        supplier_article = item.get("vendorCode")
        subject_name = item.get("subjectName")
        warehouses = item.get("warehouses") or []

        for wh in warehouses:
            flat.append({
                "last_change_ts": None,                      # отчёт не отдаёт lastChange
                "warehouse_name": wh.get("warehouseName"),
                "supplier_article": supplier_article,
                "nm_id": nm_id,
                "barcode": barcode,
                "quantity": wh.get("quantity"),
                "quantity_full": None,
                "category": None,
                "subject": subject_name,
                "brand": brand,
                "tech_size": tech_size,
                "price": None,
                "discount": None,
                "is_supply": None,
                "is_realization": None,
                "sc_code": None,
            })
    print(f"Flattened rows: {len(flat)}")
    return flat


def chunked(iterable, size):
    buf = []
    for x in iterable:
        buf.append(x)
        if len(buf) == size:
            yield buf
            buf = []
    if buf:
        yield buf


def upsert_current(sb: Client, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        print("No rows to upsert")
        return

    sent = 0
    for batch in chunked(rows, BATCH_SIZE):
        res = sb.table("wb_stocks_current") \
                .upsert(batch, on_conflict="nm_id,barcode,warehouse_name") \
                .execute()
        if getattr(res, "error", None):
            msg = getattr(res.error, "message", str(res.error))
            raise SystemExit(f"Upsert error: {msg}")
        affected = len(res.data) if res.data is not None else 0
        sent += len(batch)
        print(f"Upserted batch {len(batch)} (affected≈{affected}), progress {sent}/{len(rows)}")


def main():
    print("Creating report task…")
    task_id = create_report_task()
    print("Waiting for report to be ready…")
    wait_task_done(task_id)
    print("Downloading report…")
    report = download_report(task_id)
    rows = flatten_rows(report)

    # Фильтруем обязательные ключи PK
    cleaned = [
        x for x in rows
        if x.get("nm_id") is not None and x.get("barcode") and x.get("warehouse_name")
    ]
    if len(cleaned) != len(rows):
        print(f"Dropped {len(rows) - len(cleaned)} rows without nm_id/barcode/warehouse_name")

    # Апсерт
    sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    upsert_current(sb, cleaned)
    print(f"Done. Total processed: {len(cleaned)}")


if __name__ == "__main__":
    main()
