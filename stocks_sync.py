#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Синхронизация остатков WB в Supabase через отчёт Seller Analytics:
- Создаём задачу /api/v1/warehouse_remains
- Ждём статуса "done"
- Скачиваем отчёт
- Плющим по складам + добавляем агрегат "Всего находится на складах"
- Апсертим батчами в таблицу stocks_current

ENV (GitHub Actions -> Secrets):
  SUPABASE_URL
  SUPABASE_SERVICE_ROLE
  WB_API_KEY              # токен категории "Аналитика продавца" (без Bearer!)
Опционально:
  TABLE_NAME   (default: stocks_current)
  BATCH_SIZE   (default: 1000)
"""

from __future__ import annotations
import os
import sys
import time
import json
from typing import Any, Dict, List, Optional

import requests
from requests import Response
from supabase import create_client, Client

# --------------------
# Конфиг
# --------------------

SELLER_ANALYTICS_BASE = "https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains"

TABLE_NAME = os.getenv("TABLE_NAME", "stocks_current")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))

REPORT_PARAMS_BOOL = {
    "groupByBrand": False,
    "groupBySubject": False,
    "groupBySa": True,
    "groupByNm": True,
    "groupByBarcode": True,
    "groupBySize": True,
}
REPORT_LOCALE = "ru"

POLL_INTERVAL_SEC = 2
POLL_TIMEOUT_SEC = 180

# --------------------
# Утилиты
# --------------------

def env_required(name: str) -> str:
    val = os.getenv(name)
    if not val:
        print(f"ERROR: required env var {name} is not set", file=sys.stderr)
        sys.exit(2)
    return val

def _encode_bool_params(params: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in params.items():
        if isinstance(v, bool):
            out[k] = "true" if v else "false"
        else:
            out[k] = v
    return out

def http_request(
    method: str,
    url: str,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    timeout: int = 60,
    retries: int = 1,
) -> Response:
    """
    Обёртка над requests с лаконичными ретраями (по умолчанию 1 попытка).
    Под каждую стратегию create_task делаем свой вызов.
    """
    last_exc: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.request(
                method=method,
                url=url,
                headers=headers or {},
                params=params,
                json=json_body,
                timeout=timeout,
            )
            if resp.status_code >= 400:
                # печатаем небольшой кусок тела для диагностики
                snippet = resp.text[:300].replace("\n", " ")
                print(f"{method} {url} -> {resp.status_code}; body~: {snippet}")
            resp.raise_for_status()
            return resp
        except requests.HTTPError as e:
            last_exc = e
            if attempt < retries:
                time.sleep(1.5)
    if last_exc:
        raise last_exc
    raise RuntimeError("http_request failed without exception?")

# --------------------
# WB Seller Analytics: создание/ожидание/скачивание
# --------------------

def create_report_task(wb_api_key: str) -> str:
    """
    Делаем 3 стратегии, т.к. сервер WB может требовать разные формы:
      S1: POST + query + json={}
      S2: POST + json={locale, groupBy* as top-level}
      S3: POST + json={locale, groupBy:{...}} (альтернативная вложенная форма)
    """
    base_headers = {
        "Authorization": wb_api_key,        # важно: без "Bearer"
        "Content-Type": "application/json", # сервер иногда требует явно
        "Accept": "application/json",
    }

    # ------- Strategy 1: query + пустое тело -------
    params_q = {"locale": REPORT_LOCALE, **REPORT_PARAMS_BOOL}
    params_q = _encode_bool_params(params_q)
    print("Creating report task (S1: query+empty-json)…")
    try:
        resp = http_request(
            "POST",
            SELLER_ANALYTICS_BASE,
            headers=base_headers,
            params=params_q,
            json_body={},   # пустой JSON — это важно для некоторых гейтов WB
            retries=1,
        )
        data = resp.json()
        tid = data.get("data", {}).get("taskId")
        if tid:
            print(f"Report task created (S1): {tid}")
            return tid
        else:
            print(f"S1 returned no taskId, response: {data}")
    except Exception as e:
        print(f"S1 failed: {repr(e)}")

    # ------- Strategy 2: JSON top-level flags -------
    body_top = {"locale": REPORT_LOCALE}
    body_top.update(REPORT_PARAMS_BOOL)
    print("Creating report task (S2: body with top-level flags)…")
    try:
        resp = http_request(
            "POST",
            SELLER_ANALYTICS_BASE,
            headers=base_headers,
            json_body=body_top,
            retries=1,
        )
        data = resp.json()
        tid = data.get("data", {}).get("taskId")
        if tid:
            print(f"Report task created (S2): {tid}")
            return tid
        else:
            print(f"S2 returned no taskId, response: {data}")
    except Exception as e:
        print(f"S2 failed: {repr(e)}")

    # ------- Strategy 3: JSON с groupBy объектом -------
    # Пробуем переложить флаги в объект groupBy — встречается в альтернативной спеки
    body_group = {
        "locale": REPORT_LOCALE,
        "groupBy": {
            # проецируем семантику:
            "brand": REPORT_PARAMS_BOOL["groupByBrand"],
            "subject": REPORT_PARAMS_BOOL["groupBySubject"],
            "sa": REPORT_PARAMS_BOOL["groupBySa"],
            "nm": REPORT_PARAMS_BOOL["groupByNm"],
            "barcode": REPORT_PARAMS_BOOL["groupByBarcode"],
            "size": REPORT_PARAMS_BOOL["groupBySize"],
        },
        # без фильтра (можно добавить, если понадобится)
    }
    print("Creating report task (S3: body with groupBy object)…")
    try:
        resp = http_request(
            "POST",
            SELLER_ANALYTICS_BASE,
            headers=base_headers,
            json_body=body_group,
            retries=1,
        )
        data = resp.json()
        tid = data.get("data", {}).get("taskId")
        if tid:
            print(f"Report task created (S3): {tid}")
            return tid
        else:
            print(f"S3 returned no taskId, response: {data}")
    except Exception as e:
        print(f"S3 failed: {repr(e)}")

    raise RuntimeError("Failed to create report task via all strategies (S1/S2/S3).")

def wait_report_ready(wb_api_key: str, task_id: str) -> None:
    headers = {"Authorization": wb_api_key, "Accept": "application/json"}
    status_url = f"{SELLER_ANALYTICS_BASE}/tasks/{task_id}/status"

    print("Waiting for report to be ready…")
    started = time.time()
    while True:
        resp = http_request("GET", status_url, headers=headers)
        data = resp.json()
        status = data.get("data", {}).get("status")
        print(f"task {task_id} status: {status}")
        if status == "done":
            return
        if status in ("fail", "failed", "error"):
            raise RuntimeError(f"WB report task failed: {data}")
        if time.time() - started > POLL_TIMEOUT_SEC:
            raise TimeoutError("Timed out waiting for report to be ready")
        time.sleep(POLL_INTERVAL_SEC)

def download_report(wb_api_key: str, task_id: str) -> List[Dict[str, Any]]:
    headers = {"Authorization": wb_api_key, "Accept": "application/json"}
    download_url = f"{SELLER_ANALYTICS_BASE}/tasks/{task_id}/download"
    print("Downloading report…")
    resp = http_request("GET", download_url, headers=headers)
    try:
        data = resp.json()
    except json.JSONDecodeError:
        raise RuntimeError(f"WB download returned non-json: {resp.text[:500]}")
    if not isinstance(data, list):
        raise RuntimeError(f"WB download: unexpected response shape: {type(data)}")
    print(f"Report rows: {len(data)}")
    return data

# --------------------
# Трансформация отчёта -> строки БД
# --------------------

def flatten_rows(report_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    flat: List[Dict[str, Any]] = []
    now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    for row in report_rows:
        brand = row.get("brand")
        subject_name = row.get("subjectName")
        vendor_code = row.get("vendorCode")
        nm_id = row.get("nmId")
        barcode = row.get("barcode")
        tech_size = row.get("techSize")
        volume = row.get("volume")
        warehouses = row.get("warehouses") or []

        qty_sum = 0
        in_way_to_sum = 0
        in_way_from_sum = 0
        qty_full_sum = 0

        for w in warehouses:
            warehouse_id = w.get("warehouseId")
            warehouse_name = (w.get("warehouseName") or "").strip()

            quantity = int(w.get("quantity") or 0)
            in_way_to = int(w.get("inWayToClient") or 0)
            in_way_from = int(w.get("inWayFromClient") or 0)

            q_full = w.get("quantityFull")
            if q_full is None:
                q_full = quantity + in_way_to + in_way_from
            q_full = int(q_full or 0)

            if not warehouse_name.startswith("В пути"):
                qty_sum += quantity
                in_way_to_sum += in_way_to
                in_way_from_sum += in_way_from
                qty_full_sum += q_full

            flat.append(
                {
                    "nm_id": nm_id,
                    "supplier_article": vendor_code,
                    "barcode": barcode,
                    "tech_size": tech_size,
                    "brand": brand,
                    "subject_name": subject_name,
                    "volume_l": volume,
                    "warehouse_id": warehouse_id,
                    "warehouse_name": warehouse_name,
                    "quantity": quantity,
                    "in_way_to_client": in_way_to,
                    "in_way_from_client": in_way_from,
                    "quantity_full": q_full,
                    "source": "seller_analytics_warehouse_remains",
                    "locale": REPORT_LOCALE,
                    "updated_utc": now_utc,
                }
            )

        flat.append(
            {
                "nm_id": nm_id,
                "supplier_article": vendor_code,
                "barcode": barcode,
                "tech_size": tech_size,
                "brand": brand,
                "subject_name": subject_name,
                "volume_l": volume,
                "warehouse_id": None,
                "warehouse_name": "Всего находится на складах",
                "quantity": qty_sum,
                "in_way_to_client": in_way_to_sum,
                "in_way_from_client": in_way_from_sum,
                "quantity_full": qty_full_sum if qty_full_sum else (qty_sum + in_way_to_sum + in_way_from_sum),
                "source": "seller_analytics_warehouse_remains",
                "locale": REPORT_LOCALE,
                "updated_utc": now_utc,
            }
        )

    print(f"Flattened rows: {len(flat)}")
    return flat

# --------------------
# Supabase
# --------------------

def supabase_client() -> Client:
    url = env_required("SUPABASE_URL")
    key = env_required("SUPABASE_SERVICE_ROLE")
    return create_client(url, key)

def upsert_batches(sb: Client, table: str, rows: List[Dict[str, Any]], batch_size: int = BATCH_SIZE) -> None:
    total = len(rows)
    if total == 0:
        print("Nothing to upsert.")
        return
    for i in range(0, total, batch_size):
        chunk = rows[i : i + batch_size]
        res = sb.table(table).upsert(
            chunk,
            ignore_duplicates=False,
            on_conflict="nm_id,barcode,tech_size,warehouse_name",
        ).execute()
        affected = getattr(res, "count", None) or len(chunk)
        print(f"Upserted batch {len(chunk)} (affected≈{affected}), progress {min(i+batch_size, total)}/{total}")

# --------------------
# main
# --------------------

def main():
    wb_api_key = env_required("WB_API_KEY")  # токен категории "Аналитика продавца" (НЕ Bearer!)
    sb = supabase_client()

    # 1) создаём задачу (с тройным фолбэком)
    task_id = create_report_task(wb_api_key)

    # 2) ждём готовности
    wait_report_ready(wb_api_key, task_id)

    # 3) скачиваем
    report_rows = download_report(wb_api_key, task_id)

    # 4) плющим
    rows = flatten_rows(report_rows)

    # 5) апсерт
    upsert_batches(sb, TABLE_NAME, rows, batch_size=BATCH_SIZE)

    print(f"Done. Total processed: {len(rows)}")

if __name__ == "__main__":
    main()
