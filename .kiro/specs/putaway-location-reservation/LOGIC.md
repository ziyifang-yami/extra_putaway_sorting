# Putaway Location Reservation — Logic Document

## 问题背景

连续贴标场景下，LR API 推荐的空库位不会因本 session 的贴标操作而更新，
导致多个不同 SKU 被推荐到同一个空库位。

## 解决方案

本地 SQLite 记录「预占」，自写 SQL 推荐空库位，完全脱离 WMS LR API。

---

## 兜底链（6 层，按优先级）

```
STEP 1  wh_pending_item assigned       → banner: pending
STEP 2  wh_lot_inventory 当前 zone     → banner: current_zone
STEP 3  wh_lot_inventory 其他 zone     → banner: current_other_zone
STEP 4  SQLite 同 SKU 已预占           → banner: reserved
STEP 5  本地 SQL 推荐空库位             → banner: local_rec   ← 新
STEP 6  WMS queryAvaiBinList 纯空保底  → banner: empty_bin
```

步骤 1–3 逻辑不变，步骤 4–5 是新增，步骤 6 是原有兜底。

---

## STEP 4：SQLite 复用（同 SKU）

```
SELECT location_no FROM reservations
WHERE sku = :sku AND wh = :wh AND zone_label = :zone AND released_at IS NULL
ORDER BY created_at DESC LIMIT 1
```

- 找到 → 直接复用，不重新找库位（合并上架）
- 找不到 → 进入 STEP 5

---

## STEP 5：本地 SQL 推荐空库位

### 5a. 确定 size_id（商品应该放哪种尺寸的库位）

**方法 1（优先）**：查 SKU 历史绑定过的 size
```sql
SELECT l.size_id, COUNT(1) AS cnt
FROM wh_storage_location_item sli
JOIN wh_storage_location l ON sli.storage_location_id = l.rec_id ...
WHERE sli.item_number = :sku AND l.location_type = 4
GROUP BY l.size_id ORDER BY cnt DESC LIMIT 1
```

**方法 2（新 SKU 兜底）**：按 tote 总体积匹配最小合适 size
```
所需体积 = tote_qty × 商品单件体积
→ 找 size.volume >= 所需体积 的最小 size
```
`tote_qty` 来自 `ext_totes[current_tote].total_qty`，未知时默认为 1。

### 5b. 查可用 Bin

筛选条件：
- `location_type = 4`，`warehouse_number = :wh`，`size_id = :size_id`
- `business_flag = 0`，`location_no NOT LIKE 'DC%'`
- 满足以下任一：
  - `item_count = 0`（完全空）
  - `has_bind = 1`（已绑定该 SKU）
  - `can_random = 1`（允许混放）且有效格数未满（见下）
- `capacity >= 1`（体积上还放得下）
- Zone 过滤：NJFC → `NOT LIKE 'S%'`，SFC → `LIKE 'S%'`

**有效格数计算**（Python 层，针对 can_random 库位）：
```
effective_item_count = DB.item_count + SQLite 里该库位的其他 SKU 预占数
可分配 ← max_sku > effective_item_count
```

排序：`has_bind DESC, capacity ASC`（优先有历史的库位，其次选最小的）

---

## 预占写入

**触发时机**：点击 Print 按钮
**触发条件**：`banner_source` 为 `local_rec` 或 `empty_bin`（有库存的推荐不写预占）

```
Print 点击
  → window.print()
  → if banner_source in ["local_rec", "empty_bin"]:
       POST /api/reserve {tote_id, sku, location_no, wh, zone_label}
       → SQLite UPSERT (tote_id, sku, wh) UNIQUE
```

同一 tote+SKU 重复打印 = 更新 location_no（upsert），不产生重复记录。

---

## 预占释放

**粒度**：`(tote_id, sku)` 对，同 tote 不同 SKU 独立释放

**释放条件**：
```sql
SELECT SUM(problem_qty) AS total, SUM(done_qty) AS done
FROM wh_pending_item
WHERE target = :tote_id AND item_number = :sku AND status IN (0, 1)
-- total == done 或 no rows → 该 SKU 已全部转出 → 释放
```

**触发时机**：Session 开始时（而非定时轮询）

---

## Session 对账机制

```
每次 GET /api/lookup 开始时：
  if now - last_activity > 300s:   ← 新 session（5 分钟无操作）
      release_stale()              ← 同步对账，耗时约 < 1s
  last_activity = now
```

- 无后台线程
- 连续扫码时：对账只在每个 session 开始时跑一次，中间扫码零额外开销
- 5 分钟无操作 = session 结束，下次扫码自动触发下一次对账

---

## UI 规则

| banner_source | 标题 | Banner 颜色 |
|--------------|------|------------|
| `pending` | Assigned Location | 绿/蓝（同 zone）or 黄（跨 zone） |
| `current_zone` | Recommended Location | 绿/蓝 |
| `current_other_zone` | Recommended Location | 黄 |
| `reserved` | Recommended Location | 绿/蓝 or 黄 |
| `local_rec` | **Recommend Empty Location** | **红** |
| `empty_bin` | **Recommend Empty Location** | **红** |

红色 banner = 新分配的空库位，没有历史库存，提示操作员注意。

---

## 新文件 / 修改文件

| 文件 | 变更 |
|------|------|
| `reservation.py` | 新建：`ReservationStore` + `ReservationPoller` |
| `server.py` | 修改：加载 reservation 模块，更新 lookup 兜底链，新增 `/api/reserve` 端点，`get_recommended_bins()` + `get_size_id_*()`，更新 JS `printLabel()` + 新增 state 变量 |
| `requirements.txt` | 无新依赖（`sqlite3` 是标准库） |
