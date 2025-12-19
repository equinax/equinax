# Equinax - 量化回测系统

生产级量化交易回测系统，支持策略代码数据库存储、批量回测、策略对比分析。

**技术栈**: FastAPI + PostgreSQL + TimescaleDB + React + TradingView

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

### 一键启动

```bash
git clone https://github.com/equinax/trader.git
cd trader
just setup
```

访问:
- **前端**: http://localhost:3000
- **API 文档**: http://localhost:8000/api/docs
- **Metabase**: http://localhost:7600

## 常用命令

### 核心

| 命令 | 说明 |
|------|------|
| `just up` | 启动服务 |
| `just down` | 停止服务 |
| `just restart` | 重启服务 |
| `just status` | 查看状态 |
| `just destroy` | 销毁所有容器和镜像（需确认） |

### 数据

| 命令 | 说明 |
|------|------|
| `just data-init` | 完整初始化（用户 + 策略 + 市场数据） |
| `just data-status` | 查看数据状态 |
| `just data-download stocks --years 2024` | 下载数据到 cache |
| `just data-load --years 2024` | 导入到 PostgreSQL |
| `just data-update` | 增量更新今日数据 |
| `just data-fixtures status` | 查看 fixtures 状态 |
| `just data-fixtures generate` | 从 cache 生成 fixtures |
| `just data-fixtures download all` | 从 API 下载 fixtures |

### 数据库

| 命令 | 说明 |
|------|------|
| `just db-migrate` | 运行迁移 + TimescaleDB 设置 |
| `just db-console` | 打开 psql 控制台 |
| `just db-migrate-new "描述"` | 创建新迁移 |
| `just db-migrate-down` | 回滚一个版本 |
| `just data-db-reset` | 重置数据库（需确认） |
| `just data-db-refresh` | 刷新 TimescaleDB 连续聚合 |

### 开发

| 命令 | 说明 |
|------|------|
| `just dev-logs` | 查看所有服务日志 |
| `just dev-logs-api` | 查看 API 日志 |
| `just dev-shell` | 进入 API 容器 |
| `just dev-python` | 打开 Python REPL |
| `just dev-lint` | 代码检查 |
| `just dev-format` | 代码格式化 |
| `just fe-add <pkg>` | 添加前端依赖 |
| `just fe-install` | 同步前端依赖到容器 |
| `just dev-api-gen` | 生成前端 API 客户端 |

### 测试

| 命令 | 说明 |
|------|------|
| `just test-backend` | 运行后端测试 |
| `just test-coverage` | 测试 + 覆盖率报告 |

## 数据管理

### 开发模式（推荐）

使用内置 fixtures（~100 只股票 × 30 天），约 30 秒：

```bash
just data-init
```

### 完整数据

导入完整数据集（5,000+ 股票）：

```bash
# 方法 1: 从现有数据复制（推荐）
just data-copy-cache --source ../trading_data
just data-load --full

# 方法 2: 从 API 下载（耗时数小时）
just data-download --full
just data-load --full
```

### 日常更新

```bash
just data-update
```

## 前端开发

前端代码热更新，但依赖需通过容器管理：

```bash
just fe-add @radix-ui/react-scroll-area  # 添加依赖
just fe-install                          # 同步 package.json
just dev-api-gen                         # 更新 API 类型
```

## 项目结构

```
trader/
├── justfile                # 开发命令
├── docker-compose.yml
├── backend/
│   ├── app/                # FastAPI 应用
│   │   ├── api/v1/         # API 路由
│   │   ├── db/models/      # 数据库模型
│   │   ├── services/       # 业务逻辑
│   │   └── domain/engine/  # Backtrader 集成
│   ├── data/               # 数据模块
│   │   ├── downloads/      # 下载脚本 (AKShare/BaoStock)
│   │   ├── cache/          # SQLite 缓存 (git-ignored)
│   │   └── fixtures/       # 小样本数据
│   ├── scripts/            # CLI 工具 (data_cli.py, fixtures.py)
│   ├── workers/            # ARQ 后台任务
│   └── alembic/            # 数据库迁移
└── frontend/
    └── src/                # React 应用
        ├── components/     # UI 组件
        ├── api/generated/  # 自动生成的 API 客户端
        └── pages/          # 页面组件
```

## 环境变量

### 后端 (`backend/.env.docker`)

```env
DATABASE_URL=postgresql+asyncpg://quant:quant_dev_password@db:5432/quantdb
REDIS_URL=redis://redis:6379/0
SECRET_KEY=dev-secret-key-change-in-production
```

### 前端 (`frontend/.env`)

```env
VITE_API_URL=http://localhost:8000/api
```

## 许可证

MIT
