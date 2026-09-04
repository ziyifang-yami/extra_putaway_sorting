import os, sys
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "skills" / "config"))
try:
    from settings import engine
except ImportError:
    from sqlalchemy import create_engine
    engine = create_engine(
        f"mysql+pymysql://{os.environ['MYSQL_USER']}:{os.environ['MYSQL_PASS']}@{os.environ.get('MYSQL_HOST','rds.g3.yamibuy.net')}/yamibuy_wh?charset=utf8mb4"
    )

import pandas as pd
from sqlalchemy import text

sku = "1019226441"
wh  = "002"

sql = text("""
    SELECT li.location_no, sl.zone_id,
           li.location_no LIKE 'S%%' AS is_s_prefix,
           li.quantity
    FROM yamibuy_wh.wh_lot_inventory li
    JOIN yamibuy_wh.wh_storage_location sl
        ON li.location_no = sl.location_no
        AND li.warehouse_number = sl.warehouse_number
    WHERE li.item_number = :sku
      AND li.warehouse_number = :wh
      AND li.quantity > 0
      AND sl.location_type = 4
""")

with engine.connect() as c:
    df = pd.read_sql(sql, c, params={"sku": sku, "wh": wh})

print(f"Total rows: {len(df)}")
print(df.to_string())

# NJFC filter: NOT LIKE 'S%' AND zone_id != 2
njfc = df[(df['is_s_prefix'] == 0) & (df['zone_id'] != 2)]
print(f"\nNJFC rows (NOT S%, zone!=2): {len(njfc)}")
print(njfc.to_string())
