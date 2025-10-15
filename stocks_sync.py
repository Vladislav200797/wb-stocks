import os
import datetime as dt
import requests
from supabase import create_client

WB_API_URL = "https://statistics-api.wildberries.ru/api/v1/supplier/stocks"

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE"]
WB_API_KEY = os.environ["WB_API_KEY"]

def rfc3339(dtobj):
    return dtobj.strftime("%Y-%m-%dT%H:%M:%S")

def fetch_stocks(date_from):
    headers = {"Authorization": WB_API_KEY}
    params = {"dateFrom": date_from}
    resp = requests.get(WB_API_URL, headers=headers, params=params, timeout=120)
    resp.raise_for_status()
    return resp.json()

def normalize_row(r):
    # Вернём last_change_ts как ISO-строку, а не как datetime
    def norm_ts(s: str | None):
        if not s:
            return None
        # WB иногда присылает с 'Z' и миллисекундами — срежем до секунд
        try:
            import datetime as dt
            ts = dt.datetime.fromisoformat(s.replace("Z", ""))
            return ts.replace(microsecond=0).isoformat()
        except Exception:
            # если вдруг формат неожиданный — отправим как есть (строкой)
            return s

    return {
        "last_change_ts": norm_ts(r.get("lastChangeDate")),
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
    }

def chunked(iterable, size):
    buf = []
    for x in iterable:
        buf.append(x)
        if len(buf) == size:
            yield buf
            buf = []
    if buf:
        yield buf

def main():
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    date_from = rfc3339(dt.datetime.utcnow() - dt.timedelta(minutes=31))
    data = fetch_stocks(date_from)
    if not isinstance(data, list) or not data:
        print("No data returned")
        return

    rows = [normalize_row(r) for r in data]

    for batch in chunked(rows, 1000):
        res = sb.table("wb_stocks_current") \
            .upsert(batch, on_conflict="nm_id,barcode,warehouse_name") \
            .execute()
        if res.get("status_code") not in (None, 200, 201):
            print("Upsert error:", res)
            return

    print(f"Updated {len(rows)} records in wb_stocks_current")

if __name__ == "__main__":
    main()
