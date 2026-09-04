# Extra Putaway Sorting Guide

Scan UPC / SKU → select warehouse → view current locations + pending putaway recommendations.

## Warehouses

| Code | Name |
|------|------|
| 001  | LA   |
| 002  | NJ   |
| 101  | ON   |

Zones (002 / NJ only):
- Zone 1 → **NJFC**
- Zone 2 → **SFC**

## Run

```bash
cd Extra_Putaway_Sorting
cp .env.example .env    # fill in credentials
pip install -r requirements.txt
uvicorn server:app --host 0.0.0.0 --port 8505 --reload
```

Then open `http://localhost:8505`

## API Endpoints

| Endpoint | Params | Description |
|----------|--------|-------------|
| `GET /api/lookup` | `upc`, `wh` | Look up item + locations |
| `GET /api/warehouses` | — | List warehouses |

## TODO

- [ ] `get_current_locations()` — fill in SQL
- [ ] `get_pending_locations()` — fill in SQL + algorithm
