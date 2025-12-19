# Equinax - 量化交易回测系统

生产级量化交易回测系统，支持策略代码数据库存储、批量回测、策略对比分析。

## 技术栈

- **后端**: FastAPI + PostgreSQL + ARQ + Backtrader
- **前端**: React + TypeScript + shadcn/ui + TradingView Charts
- **部署**: Docker Compose

## 快速开始

### 前置要求

- Docker & Docker Compose
- [Just](https://github.com/casey/just) 命令运行器

```bash
# macOS
brew install just

# Linux
curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | bash -s -- --to /usr/local/bin
```

### 首次设置

```bash
# 克隆项目
git clone https://github.com/equinax/trader.git
cd trader

# 一键设置（创建 .env + 启动服务 + 初始化数据库 + 导入种子数据）
just setup

# 访问应用
# 前端: http://localhost:3000
# API文档: http://localhost:8000/api/docs
```

### 日常开发

```bash
# 启动服务
just up

# 停止服务
just down

# 重启服务
just restart

# 查看状态
just status

# 查看所有可用命令
just
```

## 命令参考

运行 `just` 或 `just --list` 查看所有可用命令。

### 核心命令

| 命令 | 描述 |
|------|------|
| `just up` | 启动所有服务 |
| `just down` | 停止所有服务 |
| `just restart` | 重启服务 |
| `just status` | 查看服务状态 |
| `just setup` | 首次项目设置 |
| `just destroy` | 销毁所有 Docker 容器和镜像（需确认） |

### 数据库命令

| 命令 | 描述 |
|------|------|
| `just db-migrate` | 运行数据库迁移 + TimescaleDB 设置 |
| `just db-migrate-new "msg"` | 创建新迁移 |
| `just db-migrate-down` | 回滚一个版本 |
| `just db-migrate-status` | 查看迁移状态 |
| `just db-console` | 打开 psql 控制台 |

### 开发命令

| 命令 | 描述 |
|------|------|
| `just dev-logs` | 查看所有服务日志 |
| `just dev-logs-api` | 查看 API 日志 |
| `just dev-logs-worker` | 查看 Worker 日志 |
| `just dev-shell` | 进入 API 容器 |
| `just dev-python` | 打开 Python REPL |
| `just dev-lint` | 运行代码检查 |
| `just dev-format` | 格式化代码 |
| `just dev-api-gen` | 生成前端 API 客户端 |
| `just fe-install` | 同步前端依赖到容器 |
| `just fe-add <pkg>` | 添加前端依赖 |

### 测试命令

| 命令 | 描述 |
|------|------|
| `just test-backend` | 运行后端测试 |
| `just test-coverage` | 运行测试并生成覆盖率报告 |

### 数据管理命令（本地开发）

这些命令统一管理所有数据：用户、策略、市场数据等。需要本地 Python 环境。

| 命令 | 描述 |
|------|------|
| `just data-init` | 完整初始化（用户 + 策略 + 市场数据，~30秒） |
| `just data-status` | 查看数据库数据状态 |
| `just data-seed-user` | 创建默认系统用户 |
| `just data-seed-strategy` | 加载默认策略 |
| `just data-db-reset` | 重置数据库（需确认） |
| `just data-db-refresh` | 刷新 TimescaleDB 连续聚合 |
| `just data-download` | 从数据源下载数据到 cache/ |
| `just data-load` | 从 cache/ 导入数据到 PostgreSQL |
| `just data-update` | 增量更新今日数据 |
| `just data-copy-cache` | 复制 trading_data/ 到 cache/ |
| `just data-generate-fixtures` | 生成小样本 fixtures |

## 示例数据

项目内置了小样本数据（`backend/data/fixtures/`），用于快速开发调试：

| 类型 | 数量 | 描述 |
|------|------|------|
| 股票 | 86只 | 沪深300/中证500/中证1000代表性股票 |
| ETF | 18只 | 主要宽基ETF |
| 指数成分 | 1,470条 | HS300/ZZ500/ZZ1000 |
| 行业分类 | 31个 | 申万L1行业 |

### 快速初始化（开发用）

```bash
# 使用内置小样本，约30秒
just data-init
```

### 导入完整数据集

如需完整数据（5,000+股票，数百万条记录）：

```bash
# 方法1: 从现有 trading_data/ 复制（推荐）
just data-copy-cache ../trading_data
just data-load --full

# 方法2: 从数据源下载（耗时数小时）
just data-download --full
just data-load --full
```

## 数据管理

项目使用独立的数据管理模块管理市场数据（股票、ETF、指数成分、行业分类等）。

### 快速开始（开发模式）

使用内置小样本数据快速启动（约 30 秒）：

```bash
# 确保 Docker 服务已启动
just up

# 初始化数据库
just data-init

# 查看数据状态
just data-status
```

### 完整数据流程

如需使用完整数据集：

```bash
# 方法 1: 使用现有 trading_data/ 数据（推荐）
just data-copy-cache ../trading_data
just data-load --full

# 方法 2: 从数据源下载（耗时数小时）
just data-download --full
just data-load --full
```

### 日常更新

```bash
# 增量更新今日数据
just data-update
```

### 数据模块结构

```
backend/data/
├── downloads/     # 下载脚本 (AKShare/BaoStock)
├── cache/         # SQLite 缓存 (~3GB, git-ignored)
└── fixtures/      # 小样本数据 (~1.5MB, git-tracked)
```

详细文档见 `backend/data/README.md`。

## 前端开发

前端代码在 Docker 容器中运行，但 `src/` 和 `public/` 目录已挂载，**代码修改会自动热更新**。

### 依赖管理

由于 `node_modules` 在容器内独立管理，添加/更新依赖需要通过 just 命令：

```bash
# 添加新依赖
just fe-add @radix-ui/react-scroll-area

# 添加开发依赖
just fe-add -D @types/some-package

# 本地编辑 package.json 后同步到容器
just fe-install
```

### API 客户端生成

后端 API 变更后，重新生成前端类型：

```bash
just dev-api-gen
```

## 项目结构

```
trader/
├── justfile                # 开发命令定义
├── docker-compose.yml      # Docker 服务定义
├── backend/
│   ├── .env.docker         # Docker 开发环境配置
│   ├── .env.example        # 环境变量模板
│   ├── .env                # 本地开发配置 (gitignore)
│   ├── app/
│   │   ├── api/v1/         # API 路由
│   │   ├── db/models/      # 数据库模型
│   │   ├── services/       # 业务逻辑
│   │   └── domain/engine/  # Backtrader 集成
│   ├── alembic/            # 数据库迁移
│   │   └── versions/       # 迁移文件
│   ├── data/               # 数据管理模块
│   │   ├── downloads/      # 数据下载脚本
│   │   ├── cache/          # SQLite 缓存 (git-ignored)
│   │   └── fixtures/       # 小样本数据 + 默认策略
│   ├── workers/            # ARQ 任务 (回测、分类、数据更新)
│   └── scripts/            # 统一 CLI 脚本 (data_cli.py)
├── frontend/
│   ├── .env                # 前端配置 (VITE_API_URL)
│   ├── src/
│   │   ├── components/     # UI 组件
│   │   ├── api/generated/  # 自动生成的 API 客户端
│   │   └── pages/          # 页面组件
│   └── orval.config.ts     # API 生成配置
```

## API 端点

- `POST /api/v1/strategies` - 创建策略
- `GET /api/v1/strategies` - 策略列表
- `POST /api/v1/strategies/validate-code` - 验证策略代码
- `GET /api/v1/strategies/templates/list` - 获取策略模板
- `POST /api/v1/backtests` - 创建回测任务
- `GET /api/v1/backtests/{id}/results` - 获取回测结果
- `GET /api/v1/stocks` - 股票列表
- `GET /api/v1/stocks/{code}/kline` - K线数据

## 内置策略模板

- **SMA Crossover** - 双均线交叉策略
- **RSI Strategy** - RSI 超买超卖策略
- **MACD Strategy** - MACD 金叉死叉策略
- **Bollinger Bands** - 布林带均值回归策略

## 环境变量

### 后端 (`backend/.env.docker`)

```env
DATABASE_URL=postgresql+asyncpg://quant:quant_dev_password@db:5432/quantdb
REDIS_URL=redis://redis:6379/0
SECRET_KEY=dev-secret-key-change-in-production
JWT_SECRET_KEY=dev-jwt-secret-key-change-in-production
DEBUG=true
CORS_ORIGINS=["http://localhost:3000","http://localhost:5173"]
```

### 前端

```env
VITE_API_URL=http://localhost:8000/api
```

## CLI 命令（本地开发）

所有 CLI 命令已统一到 `scripts/data_cli.py`，通过 `just data-*` 调用：

```bash
# 查看所有数据命令
just --list | grep data-

# 常用命令
just data-init              # 完整初始化（用户 + 策略 + 市场数据）
just data-status            # 查看数据状态
just data-seed-user         # 创建默认用户
just data-seed-strategy     # 加载默认策略
just data-db-reset          # 重置数据库（需确认）
just data-db-refresh        # 刷新连续聚合

# 直接调用 CLI（本地开发）
cd backend && source .venv/bin/activate
python -m scripts.data_cli --help
python -m scripts.data_cli status
python -m scripts.data_cli init
```

## License

MIT
