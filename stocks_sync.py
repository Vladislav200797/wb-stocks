#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
WB stocks -> Supabase (как в твоём GAS-скрипте):
- Берём статистику остатков: GET /api/v1/supplier/stocks
- Пишем поминутно по складам
- Отдельно добавляем агрегат "Всего находится на складах" (без складов "В пути …")
- (опционально) подтягиваем скидки через Discounts API

ENV (GitHub Actions -> Secrets):
  SUPABASE_URL
  SUPABASE_SERVICE_ROLE

  # Тот же токен, что в Google-таблице в "Технический!B1"
  # Для статистики он идёт без "Bearer"
  WB_STATS_API_KEY

  # Для Discounts API нужен Bearer-токен (если такой же — просто скопируй)
  WB_DISCOUNTS_BEARER   (опционально)

Опционально:
  TABLE_NAME (default: stocks_current)
  BATCH_SIZE (default: 1000)
  WITH_DISCOUNTS (default: false)   # "true" чтобы дергать discounts API
"""

from __future__ import annotations
import os
import sys
import time
import json
from typing import Any, Dict, List, Optional, Tuple

import requests
from supabase import create_client, Client

# -------------------- Конфиг --------------------

STATS_URL = "https://statistics-api.wildberries.ru/api/v1/supplier/stocks"
DATE_FROM = "2021-01-01T00:00:00Z"

DISCOUNTS_URL = "https://discounts-prices-api.wildberries.ru/api/v2/list/goods/filter"
DISCOUNTS_LIMIT = 1000

TABLE_NAME = os.getenv("TABLE_NAME", "stocks_current")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
WITH_DISCOUNTS = os.getenv("WITH_DISCOUNTS", "false").lower() == "true"

# -------------------- Утилиты --------------------

def env_required(name: str) -> str:
    val = os.getenv(name)
    if not val:
        print(f"ERROR: required env var {name} is not set", file=sys.stderr)
        sys.exit(2)
    return val

def http_get_json(url: str, headers: Dict[str, str], params: Optional[Dict[str, Any]] = None, retry: int = 2) -> Any:
    last = None
    for i in range(retry):
        resp = requests.get(url, headers=headers, params=params, timeout=60)
        if resp.status_code >= 400:
            print(f"GET {url} -> {resp.status_code}; body~ {resp.text[:300]}")
        try:
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last = e
            time.sleep(1.5)
    raise last or RuntimeError("http_get_json failed")

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
        print(f"Upserted {len(chunk)} (≈{affected}) | {min(i+batch_size, total)}/{total}")

# -------------------- Discounts (опционально) --------------------

def fetch_discounts_map(bearer: str) -> Dict[int, float]:
    """
    Возвращает {nmID: discount_float_0..1}
    """
    headers = {"Authorization": f"Bearer {bearer}"}
    offset = 0
    result: Dict[int, float] = {}

    while True:
        params = {"limit": DISCOUNTS_LIMIT, "offset": offset}
        data = http_get_json(DISCOUNTS_URL, headers=headers, params=params)
        lst = (((data or {}).get("data") or {}).get("listGoods")) or []
        if not lst:
            break
        for it in lst:
            nm = it.get("nmID")
            disc = it.get("discount")
            if nm is not None and disc is not None:
                try:
                    result[int(nm)] = float(disc) / 100.0
                except Exception:
                    pass
        if len(lst) < DISCOUNTS_LIMIT:
            break
        offset += DISCOUNTS_LIMIT
        time.sleep(0.4)  # чутка притормозим
    print(f"Discounts loaded: {len(result)} nmIDs")
    return result

# -------------------- Основная логика --------------------

def fetch_stocks(stats_api_key: str) -> List[Dict[str, Any]]:
    headers = {"Authorization": stats_api_key}
    params = {"dateFrom": DATE_FROM}
    data = http_get_json(STATS_URL, headers=headers, params=params)
    if not isinstance(data, list):
        raise RuntimeError(f"Unexpected stocks payload type: {type(data)}")
    print(f"Stocks rows (raw): {len(data)}")
    return data

def split_rows_and_aggregate(raw: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Из плоских строк делаем:
      — такие же строки по складам
      — + агрегат «Всего находится на складах» (сумма без «В пути …»)
    """
    out: List[Dict[str, Any]] = []
    now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    # ключ агрегата: (nmId, barcode, techSize)
    sums: Dict[Tuple[int, str, str], Dict[str, int]] = {}

    def add_to_sum(key: Tuple[int, str, str], q: int, to_cli: int, from_cli: int, q_full: Optional[int]):
        agg = sums.setdefault(key, {"q": 0, "to": 0, "from": 0, "full": 0})
        agg["q"] += q
        agg["to"] += to_cli
        agg["from"] += from_cli
        if q_full is not None:
            agg["full"] += q_full
        else:
            agg["full"] += (q + to_cli + from_cli)

    for it in raw:
        # поля ровно как в твоём GAS
        lastChangeDate = it.get("lastChangeDate")
        warehouseName = (it.get("warehouseName") or "").strip()
        supplierArticle = it.get("supplierArticle")
        nmId = it.get("nmId")
        barcode = it.get("barcode")
        quantity = int(it.get("quantity") or 0)
        inWayToClient = int(it.get("inWayToClient") or 0)
        inWayFromClient = int(it.get("inWayFromClient") or 0)
        quantityFull = it.get("quantityFull")
        if quantityFull is None:
            quantityFull = quantity + inWayToClient + inWayFromClient
        quantityFull = int(quantityFull or 0)

        category = it.get("category")
        subject = it.get("subject")
        brand = it.get("brand")
        techSize = it.get("techSize")
        price = it.get("Price")
        discount_field = it.get("Discount")

        row = {
            "updated_utc": now_utc,
            "last_change_date": lastChangeDate,

            "warehouse_name": warehouseName,
            "supplier_article": supplierArticle,
            "nm_id": nmId,
            "barcode": barcode,

            "quantity": quantity,
            "in_way_to_client": inWayToClient,
            "in_way_from_client": inWayFromClient,
            "quantity_full": quantityFull,

            "category": category,
            "subject_name": subject,
            "brand": brand,
            "tech_size": techSize,

            "price": price,
            "discount_api": discount_field,   # как пришло из stocks (если есть)
            "source": "statistics_api_v1",
        }
        out.append(row)

        # суммируем для агрегата (но исключаем «В пути …»)
        if not warehouseName.startswith("В пути"):
            key = (int(nmId) if nmId is not None else 0, str(barcode or ""), str(techSize or ""))
            add_to_sum(key, quantity, inWayToClient, inWayFromClient, quantityFull)

    # агрегатные строки
    for (nmId, barcode, techSize), s in sums.items():
        out.append({
            "updated_utc": now_utc,
            "last_change_date": None,

            "warehouse_name": "Всего находится на складах",
            "supplier_article": None,  # не всегда однозначный (можно опционально вычислить)
            "nm_id": nmId,
            "barcode": barcode,
            "tech_size": techSize,

            "quantity": s["q"],
            "in_way_to_client": s["to"],
            "in_way_from_client": s["from"],
            "quantity_full": s["full"],

            "category": None,
            "subject_name": None,
            "brand": None,

            "price": None,
            "discount_api": None,
            "source": "statistics_api_v1_aggregate",
        })

    print(f"Prepared rows (with aggregate): {len(out)}")
    return out

def inject_discounts(rows: List[Dict[str, Any]], bearer: str) -> None:
    """
    Дополняем скидку из Discounts API: кладём в поле discount_pct (0..1).
    """
    disc_map = fetch_discounts_map(bearer)
    hit = 0
    for r in rows:
        nm = r.get("nm_id")
        if isinstance(nm, int) and nm in disc_map:
            r["discount_pct"] = disc_map[nm]
            hit += 1
    print(f"Discounts matched rows: {hit}")

# -------------------- main --------------------

def main():
    sb = supabase_client()

    stats_key = env_required("WB_STATS_API_KEY")
    discounts_bearer = os.getenv("WB_DISCOUNTS_BEARER", "").strip()

    print("Fetching WB stocks (statistics-api)…")
    raw = fetch_stocks(stats_key)

    print("Transforming…")
    rows = split_rows_and_aggregate(raw)

    if WITH_DISCOUNTS:
        if not discounts_bearer:
            print("WITH_DISCOUNTS=true, но WB_DISCOUNTS_BEARER не задан — пропускаю подтяжку скидок.")
        else:
            print("Fetching discounts…")
            inject_discounts(rows, discounts_bearer)

    print("Upserting to Supabase…")
    upsert_batches(sb, TABLE_NAME, rows, batch_size=BATCH_SIZE)

    print(f"Done. Total processed: {len(rows)}")

if __name__ == "__main__":
    main()
