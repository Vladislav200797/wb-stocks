#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
stocks_sync.py — загрузка остатков по складам WB в Supabase.

Что делаем:
- Создаём задание на отчёт /api/v1/warehouse_remains (Seller Analytics)
- Ждём готовности
- Скачиваем массив карточек с warehouses[]
- "Расплющиваем" в строки по складам
- Сохраняем все реальные склады + оба «в пути», НО
  не сохраняем агрегат «Всего находится на складах», чтобы не было двойного учёта.
- Upsert в Supabase батчами.

Переменные окружения:
- SUPABASE_URL
- SUPABASE_SERVICE_ROLE     (service_role ключ)
- WB_API_KEY                (токен WB с категорией "Аналитика продавца")

pip:
- supabase==2.*
- requests
"""

import os
import sys
import time
import math
import json
import typing as t
from datetime import datetime, timezone

import requests
from supabase import create_client, Client


# ---------- Константы и настройки ----------

SELLER_ANALYTICS_BASE = "https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains"

# "Специальные" названия складов в ответе отчёта:
IN_WAY_TO_CLIENT   = "В пути до получателей"
IN_WAY_FROM_CLIENT = "В пути возвраты на склад WB"
TOTAL_ACROSS       = "Всего находится на складах"

# Параметры группы в отчёте (можно менять по необходимости)
REPORT_PARAMS = {
    "locale": "ru",
    "groupByBrand": False,
    "groupBySubject": False,
    "groupBySa": True,       # артикул продавца
    "groupByNm": True,       # артикул WB
    "groupByBarcode": True,  # баркод
    "groupBySize": True      # размер
}

# Паузы/ретраи
HTTP_RETRIES = 3
HTTP_BACKOFF = 2  # экспоненциальный: 2s, 4s, 8s
POLL_INTERVAL_SECONDS = 2
POLL_TIMEOUT_SECONDS = 120  # общее ожидание готовности отчёта


# ---------- Утилиты HTTP ----------

def http_request(method: str, url: str, headers: dict, params: dict | None = None, json_body: dict | None = None) -> requests.Response:
    last_err = None
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            resp = requests.request(method=method, url=url, headers=headers, params=params, json=json_body, timeout=60)
            resp.raise_for_status()
            return resp
        except requests.HTTPError as e:
            last_err = e
            if attempt < HTTP_RETRIES:
                wait = HTTP_BACKOFF ** attempt
                print(f"{method} {url} error: {e!r}, retry in {wait}s (attempt {attempt}/{HTTP_RETRIES})")
                time.sleep(wait)
            else:
                print(f"HTTP failed after retries: {method} {url} last_err={e!r}")
                raise
        except requests.RequestException as e:
            last_err = e
            if attempt < HTTP_RETRIES:
                wait = HTTP_BACKOFF ** attempt
                print(f"{method} {url} error: {e!r}, retry in {wait}s (attempt {attempt}/{HTTP_RETRIES})")
                time.sleep(wait)
            else:
                print(f"HTTP failed after retries: {method} {url} last_err={e!r}")
                raise
    # theoretically unreachable
    raise last_err


# ---------- WB Seller Analytics: создание/ожидание/загрузка отчёта ----------

def create_report_task(wb_api_key: str) -> str:
    headers = {"Authorization": wb_api_key}
    print("Creating report task…")
    resp = http_request("POST", SELLER_ANALYTICS_BASE, headers=headers, params=REPORT_PARAMS)
    data = resp.json()
    task_id = data.get("data", {}).get("taskId")
    if not task_id:
        raise RuntimeError(f"WB create task: unexpected response: {data}")
    print(f"Report task created: {task_id}")
    return task_id


def wait_report_ready(wb_api_key: str, task_id: str) -> None:
    headers = {"Authorization": wb_api_key}
    url = f"{SELLER_ANALYTICS_BASE}/tasks/{task_id}/status"
    print("Waiting for report to be ready…")
    deadline = time.time() + POLL_TIMEOUT_SECONDS
    last_status = None
    while time.time() < deadline:
        resp = http_request("GET", url, headers=headers)
        data = resp.json()
        status = data.get("data", {}).get("status")
        if status != last_status:
            print(f"task {task_id} status: {status}")
            last_status = status
        if status == "done":
            return
        if status in {"error", "failed"}:
            raise RuntimeError(f"WB task failed: {data}")
        time.sleep(POLL_INTERVAL_SECONDS)
    raise TimeoutError(f"WB task {task_id} not done within {POLL_TIMEOUT_SECONDS}s")


def download_report(wb_api_key: str, task_id: str) -> list[dict]:
    headers = {"Authorization": wb_api_key}
    url = f"{SELLER_ANALYTICS_BASE}/tasks/{task_id}/download"
    print("Downloading report…")
    resp = http_request("GET", url, headers=headers)
    arr = resp.json()
    if not isinstance(arr, list):
        raise RuntimeError(f"WB download: unexpected response type: {type(arr)}")
    print(f"Report rows: {len(arr)}")
    return arr


# ---------- Преобразование (flatten) ----------

def flatten_report_item(item: dict) -> list[dict]:
    """
    На входе одна карточка из download-отчёта:
    {
      "brand": ...,
      "subjectName": ...,
      "vendorCode": ...,
      "nmId": 123,
      "barcode": "...",
      "techSize": "...",
      "volume": 1.23,
      "warehouses": [
        {"warehouseName": "...", "quantity": 5}, ...
      ]
    }

    Возвращаем список строк:
    - сохраняем ВСЕ реальные склады
    - сохраняем оба «в пути» (до получателей, возвраты)
    - НЕ сохраняем «Всего находится на складах» (агрегат)
    """
    base = {
        "nm_id": item.get("nmId"),
        "supplier_article": item.get("vendorCode"),
        "barcode": item.get("barcode"),
        "tech_size": item.get("techSize"),
        "brand": item.get("brand"),
        "subject_name": item.get("subjectName"),
        "volume": item.get("volume"),
    }

    rows: list[dict] = []
    real_sum = 0
    wb_total = None

    for w in item.get("warehouses", []) or []:
        wname = (w.get("warehouseName") or "").strip()
        qty = int(w.get("quantity") or 0)

        # агрегат «Всего…» не сохраняем — приводит к двойному учёту
        if wname == TOTAL_ACROSS:
            wb_total = qty
            continue

        # реальные склады и оба «в пути» сохраняем как есть
        if wname not in (IN_WAY_TO_CLIENT, IN_WAY_FROM_CLIENT):
            real_sum += qty

        rows.append({
            **base,
            "warehouse_name": wname,
            "quantity": qty,
        })

    # мягкая проверка согласованности: если WB-агрегат присутствует, сверим с суммой реальных складов
    if wb_total is not None and wb_total != real_sum:
        print(f"[WARN] nm_id={base['nm_id']} total_mismatch: wb_total={wb_total} != sum_real={real_sum}")

    return rows


def flatten_report(arr: list[dict]) -> list[dict]:
    out: list[dict] = []
    for item in arr:
        out.extend(flatten_report_item(item))
    print(f"Flattened rows: {len(out)}")
    # добавим updated_at (UTC) на все строки разом
    now_utc = datetime.now(timezone.utc).isoformat()
    for r in out:
        r["updated_at"] = now_utc
    return out


# ---------- Запись в Supabase ----------

def supabase_client() -> Client:
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SERVICE_ROLE"]
    return create_client(url, key)


def upsert_rows(client: Client, table: str, rows: list[dict], batch_size: int = 1000) -> None:
    total = len(rows)
    if total == 0:
        print("Nothing to upsert.")
        return
    done = 0
    for i in range(0, total, batch_size):
        batch = rows[i:i + batch_size]
        # Если у тебя есть уникальный индекс (например, по (nm_id, barcode, tech_size, warehouse_name)),
        # можно добавить on_conflict="(nm_id, barcode, tech_size, warehouse_name)"
        res = client.table(table).upsert(batch).execute()
        # postgrest APIResponse: res.count может быть None; просто логируем прогресс
        done += len(batch)
        print(f"Upserted batch {len(batch)} (affected≈{len(batch)}), progress {done}/{total}")


# ---------- main ----------

def main():
    # env
    wb_api_key = os.environ.get("WB_API_KEY")
    if not wb_api_key:
        raise RuntimeError("WB_API_KEY env is required")
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE")
    if not (supabase_url and supabase_key):
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE envs are required")

    # 1) создаём отчёт
    task_id = create_report_task(wb_api_key)

    # 2) ждём готовности
    wait_report_ready(wb_api_key, task_id)

    # 3) скачиваем
    report = download_report(wb_api_key, task_id)

    # 4) расплющиваем (без «Всего находится на складах»)
    rows = flatten_report(report)

    # 5) upsert в Supabase
    client = supabase_client()
    upsert_rows(client, table="wb_stocks_current", rows=rows)

    print("Done. Total processed:", len(rows))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", repr(e))
        sys.exit(1)
