#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Синхронизация остатков WB в Supabase через отчёт Seller Analytics:
- Создаём задачу на отчёт /api/v1/warehouse_remains
- Пулим статус, дожидаемся "done"
- Скачиваем отчёт
- "Плющим" склады в строки (по складу/штрихкоду/размеру и т.п.)
- Апсертим батчами в таблицу stocks_current

Переменные окружения (GitHub Actions -> Secrets):
  SUPABASE_URL
  SUPABASE_SERVICE_ROLE
  WB_API_KEY  (токен категории "Аналитика продавца" / Seller Analytics)
Опционально:
  TABLE_NAME (по умолчанию stocks_current)
  BATCH_SIZE (по умолчанию 1000)
"""

from __future__ import annotations

import os
import sys
import time
import json
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests import Response
from supabase import create_client, Client

# --------------------
# Константы / настройки
# --------------------

SELLER_ANALYTICS_BASE = "https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains"

TABLE_NAME = os.getenv("TABLE_NAME", "stocks_current")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))

# Группировки — включаем всё, чтобы получить максимум деталей (бренд/предмет/артикулы/баркоды/размеры/объём)
REPORT_PARAMS_BOOL = {
    "groupByBrand": False,
    "groupBySubject": False,
    "groupBySa": True,
    "groupByNm": True,
    "groupByBarcode": True,
    "groupBySize": True,
}
REPORT_LOCALE = "ru"  # ru|en|zh

POLL_INTERVAL_SEC = 2
POLL_TIMEOUT_SEC = 180  # запас по времени ожидания готовности отчёта

# --------------------
# Утилиты
# --------------------

def env_required(name: str) -> str:
    val = os.getenv(name)
    if not val:
        print(f"ERROR: required env var {name} is not set", file=sys.stderr)
        sys.exit(2)
    return val

def http_request(
    method: str,
    url: str,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    timeout: int = 60,
    retries: int = 3,
    backoff_base: int = 2,
) -> Response:
    """
    Обёртка над requests с простыми ретраями.
    """
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
            resp.raise_for_status()
            return resp
        except requests.HTTPError as e:
            if attempt < retries:
                wait = backoff_base ** attempt
                print(f"{method} {url} error: {repr(e)}, retry in {wait}s (attempt {attempt}/{retries})")
                time.sleep(wait)
                continue
            print(
                f"HTTP failed after retries: {method} {url} last_err={repr(e)}",
                file=sys.stderr,
            )
            raise

def _encode_bool_params(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    В query у WB нужно строго 'true'/'false' в нижнем регистре.
    """
    out: Dict[str, Any] = {}
    for k, v in params.items():
        if isinstance(v, bool):
            out[k] = "true" if v else "false"
        else:
            out[k] = v
    return out

# --------------------
# WB Seller Analytics: создание/ожидание/загрузка отчёта
# --------------------

def create_report_task(wb_api_key: str) -> str:
    headers = {"Authorization": wb_api_key}
    params = {"locale": REPORT_LOCALE, **REPORT_PARAMS_BOOL}
    params = _encode_bool_params(params)

    print("Creating report task…")
    resp = http_request("POST", SELLER_ANALYTICS_BASE, headers=headers, params=params)
    data = resp.json()
    task_id = data.get("data", {}).get("taskId")
    if not task_id:
        raise RuntimeError(f"WB create task: unexpected response: {data}")
    print(f"Report task created: {task_id}")
    return task_id

def wait_report_ready(wb_api_key: str, task_id: str) -> None:
    headers = {"Authorization": wb_api_key}
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
    headers = {"Authorization": wb_api_key}
    download_url = f"{SELLER_ANALYTICS_BASE}/tasks/{task_id}/download"
    print("Downloading report…")
    resp = http_request("GET", download_url, headers=headers)
    # Ответ — JSON-массив карточек с полем warehouses, если quantity>0
    try:
        data = resp.json()
    except json.JSONDecodeError:
        raise RuntimeError(f"WB download returned non-json: {resp.text[:500]}")
    if not isinstance(data, list):
        raise RuntimeError(f"WB download: unexpected response shape: {type(data)}")
    print(f"Report rows: {len(data)}")
    return data

# --------------------
# Трансформация отчёта в строки для БД
# --------------------

def flatten_rows(report_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Превращаем структуру:
      [{ brand, subjectName, vendorCode, nmId, barcode, techSize, volume, warehouses: [...]}, ...]
    в плоские строки по каждому складу (если warehouses есть и quantity>0), при этом
    добавляем агрегаты:
      - quantity_total_on_warehouses: сумма quantity по складам (что фактически и есть "Всего находится на складах")
      - in_way_to_client_total
      - in_way_from_client_total
      - quantity_full_total (если WB вернёт; если нет — считаем quantity + inWayToClient + inWayFromClient)
    Также добавляем 2 "агрегатные" строки с warehouse_name = "Всего находится на складах"
    (и отдельные суммы по in_way*) — чтобы быстро сравнивать.
    """
    flat: List[Dict[str, Any]] = []

    for row in report_rows:
        brand = row.get("brand")
        subject_name = row.get("subjectName")
        vendor_code = row.get("vendorCode")
        nm_id = row.get("nmId")
        barcode = row.get("barcode")
        tech_size = row.get("techSize")
        volume = row.get("volume")
        warehouses = row.get("warehouses") or []

        # сумма по складам только по реальным складам (без "в пути")
        qty_sum = 0
        in_way_to_sum = 0
        in_way_from_sum = 0
        qty_full_sum = 0

        # сначала разворачиваем реальные склады
        for w in warehouses:
            warehouse_id = w.get("warehouseId")
            warehouse_name = w.get("warehouseName") or ""
            # WB обычно кладёт поля:
            quantity = int(w.get("quantity") or 0)
            in_way_to = int(w.get("inWayToClient") or 0)
            in_way_from = int(w.get("inWayFromClient") or 0)

            # quantityFull бывает на карточке, но иногда отдают только на складском уровне — учитываем оба случая
            q_full = w.get("quantityFull")
            if q_full is None:
                # если нет — посчитаем сами как quantity + в пути (как в ЛК)
                q_full = quantity + in_way_to + in_way_from
            q_full = int(q_full or 0)

            # Реальные склады — это не "в пути…"
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
                    # служебные
                    "source": "seller_analytics_warehouse_remains",
                    "locale": REPORT_LOCALE,
                    "updated_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                }
            )

        # агрегатная строка "Всего находится на складах" (по реальным складам)
        # чтобы легко сравнивать с числом из ЛК
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
                # quantity_full для total считаем так же, как сумма по складам
                "quantity_full": qty_full_sum if qty_full_sum else (qty_sum + in_way_to_sum + in_way_from_sum),
                "source": "seller_analytics_warehouse_remains",
                "locale": REPORT_LOCALE,
                "updated_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
        )

    print(f"Flattened rows: {len(flat)}")
    return flat

# --------------------
# Запись в Supabase
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

    # Индекс/ключ для upsert — на текущие остатки логично взять (nm_id, barcode, tech_size, warehouse_name)
    # warehouse_id иногда None у агрегатной строки — тогда ключом будет warehouse_name (тоже уникально).
    # Создайте в таблице соответствующий UNIQUE индекс.
    for i in range(0, total, batch_size):
        chunk = rows[i : i + batch_size]
        res = sb.table(table).upsert(chunk, ignore_duplicates=False, on_conflict="nm_id,barcode,tech_size,warehouse_name").execute()
        affected = getattr(res, "count", None) or len(chunk)
        print(f"Upserted batch {len(chunk)} (affected≈{affected}), progress {min(i+batch_size, total)}/{total}")

# --------------------
# main
# --------------------

def main():
    wb_api_key = env_required("WB_API_KEY")  # токен категории "Аналитика продавца"
    sb = supabase_client()

    # 1) создать задачу
    task_id = create_report_task(wb_api_key)

    # 2) дождаться готовности
    wait_report_ready(wb_api_key, task_id)

    # 3) скачать
    report_rows = download_report(wb_api_key, task_id)

    # 4) плоские строки
    rows = flatten_rows(report_rows)

    # 5) апсерты в таблицу
    upsert_batches(sb, TABLE_NAME, rows, batch_size=BATCH_SIZE)

    print(f"Done. Total processed: {len(rows)}")

if __name__ == "__main__":
    main()
