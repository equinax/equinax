# 数据管理模块

统一管理量化系统的数据下载、缓存和导入。

## 目录结构

```
data/
├── downloads/     # 下载脚本 (AKShare/BaoStock)
│   ├── download_a_stock_data.py      # A股日线数据
│   ├── download_etf_data.py          # ETF日线数据
│   ├── download_market_cap.py        # 市值数据
│   ├── download_index_constituents.py # 指数成分股
│   ├── download_industry_data.py     # 行业分类
│   ├── download_northbound_holdings.py # 北向持仓
│   └── download_institutional_holdings.py # 机构持仓
├── cache/         # SQLite 缓存 (~3GB, git-ignored)
├── fixtures/      # 小样本数据 (~1.5MB, git-tracked)
└── README.md      # 本文档
```

## 快速开始

### 开发模式 (30秒启动)

```bash
# 使用内置小样本初始化数据库
python -m scripts.data_cli init
```

### 全量数据

```bash
# 方式1: 复制现有数据 (推荐，如果已有 trading_data/)
python -m scripts.data_cli copy-cache

# 方式2: 重新下载 (~3小时)
python -m scripts.data_cli download --full

# 导入到 PostgreSQL
python -m scripts.data_cli load --full
```

### 增量更新

```bash
# 更新今日数据
python -m scripts.data_cli update
```

### 查看状态

```bash
python -m scripts.data_cli status
```

## CLI 命令参考

| 命令 | 说明 |
|------|------|
| `init` | 使用 fixtures 初始化数据库 (开发用) |
| `download` | 从数据源下载数据到 cache/ |
| `load` | 将 cache/ 数据导入 PostgreSQL |
| `update` | 增量更新今日数据 |
| `status` | 显示数据状态 |
| `copy-cache` | 从 trading_data/ 复制数据到 cache/ |
| `generate-fixtures` | 生成小样本数据 |

## 数据源

| 数据类型 | 数据源 | 更新频率 |
|---------|--------|---------|
| A股日线 | AKShare / BaoStock | 每日 |
| ETF日线 | AKShare | 每日 |
| 市值数据 | AKShare | 每日 |
| 指数成分 | AKShare | 每周 |
| 行业分类 | AKShare (申万) | 每月 |
| 北向持仓 | AKShare | 每日 |
| 机构持仓 | AKShare | 每季度 |

## Worker 任务

后台 Worker 提供自动化数据更新，在 `workers/data_tasks.py` 中定义。

### 可用任务

| 任务函数 | 描述 |
|---------|------|
| `daily_data_update` | 每日完整数据更新流程 |
| `download_stock_data` | 下载股票数据 |
| `download_etf_data` | 下载ETF数据 |
| `download_northbound_data` | 下载北向持仓 |
| `download_market_cap_data` | 下载市值数据 |
| `import_stock_data` | 导入股票数据到 PostgreSQL |
| `import_etf_data` | 导入ETF数据到 PostgreSQL |
| `check_data_status` | 检查数据库状态 |
| `get_download_status` | 检查缓存文件状态 |

### 定时任务

Worker 配置了自动定时任务 (在 `workers/settings.py` 中)：

- `daily_data_update`: 每天 16:30 CST (收盘后) 自动运行

### 手动触发任务

```python
from app.core.arq import get_arq_pool

# 触发每日更新
pool = await get_arq_pool()
await pool.enqueue_job("daily_data_update")

# 触发特定下载
await pool.enqueue_job("download_stock_data", recent_days=1)

# 检查状态
await pool.enqueue_job("check_data_status")
```

### 启动 Worker

```bash
# 在 backend/ 目录
arq workers.settings.WorkerSettings
```

## 小样本规格

| 类型 | 数量 | 天数 | 大小 |
|------|------|------|------|
| 股票 | 86只 | 30天 | ~0.9MB |
| ETF | 18只 | 30天 | ~0.2MB |
| 指数 | 3个 | - | ~0.2MB |
| 行业 | 31个 | - | ~0.2MB |

股票选择标准:
- 沪深300成分股
- 中证500成分股
- 中证1000成分股
- 北向重仓股
- ST股票
- 次新股

## 数据流

```
┌─────────────────────────────────────────────────────────────────┐
│                    数据源 (AKShare / BaoStock)                   │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│              下载脚本 (data/downloads/)                          │
└──────────────────────────────┬──────────────────────────────────┘
                               │
              ┌────────────────┼────────────────┐
              ▼                                 ▼
┌──────────────────────────┐     ┌──────────────────────────────┐
│   小样本 (fixtures/)      │     │     缓存 (cache/)             │
│   ~1.5MB, git-tracked    │     │     ~3GB, git-ignored        │
└──────────────────────────┘     └──────────────────────────────┘
              │                                 │
              └────────────────┬────────────────┘
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│              data_cli.py: init (fixtures) / load (cache)        │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    PostgreSQL (Docker)                           │
│  asset_meta | market_daily | index_constituents | ...           │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│              Worker: daily_data_update (16:30 CST)               │
└─────────────────────────────────────────────────────────────────┘
```

## 相关文件

- `scripts/data_cli.py` - CLI 工具入口
- `scripts/migrate_all_data.py` - SQLite → PostgreSQL 迁移
- `scripts/generate_fixtures.py` - 生成小样本数据
- `workers/data_tasks.py` - Worker 数据更新任务
- `workers/settings.py` - Worker 配置

## 常见问题

### Q: init 命令报错 "No fixture files found"

确保 `data/fixtures/` 目录存在且包含 `sample_*.db` 文件。如果缺失，运行：

```bash
python -m scripts.data_cli generate-fixtures --source /Users/dan/Code/q/trading_data
```

### Q: 下载脚本报错

1. 检查网络连接
2. 确保 akshare 已安装: `pip install akshare`
3. 部分数据源有访问限制，请稍后重试

### Q: PostgreSQL 连接失败

确保 Docker 服务运行中：

```bash
docker compose up -d postgres
```
