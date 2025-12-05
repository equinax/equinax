# Quant Backtest - 量化交易回测系统

生产级量化交易回测系统，支持策略代码数据库存储、批量回测、策略对比分析。

## 技术栈

- **后端**: FastAPI + PostgreSQL + ARQ + Backtrader
- **前端**: React + TypeScript + shadcn/ui + TradingView Charts
- **部署**: Docker Compose

## 快速开始

### 1. 启动服务

```bash
# 启动所有服务
docker-compose up -d

# 查看日志
docker-compose logs -f api
```

### 2. 迁移数据

```bash
# 进入后端容器
docker-compose exec api bash

# 运行数据迁移 (SQLite -> PostgreSQL)
python scripts/migrate_sqlite.py
```

### 3. 访问应用

- **前端**: http://localhost:3000
- **API文档**: http://localhost:8000/api/docs
- **OpenAPI JSON**: http://localhost:8000/api/openapi.json

## 开发

### 后端开发

```bash
cd backend

# 创建虚拟环境
python -m venv .venv
source .venv/bin/activate

# 安装依赖
pip install -e ".[dev]"

# 启动开发服务器
uvicorn app.main:app --reload
```

### 前端开发

```bash
cd frontend

# 安装依赖
pnpm install

# 启动开发服务器
pnpm dev

# 生成 API 客户端 (需要后端运行)
pnpm run api:generate
```

### 数据库迁移

```bash
cd backend

# 创建新迁移
alembic revision --autogenerate -m "description"

# 执行迁移
alembic upgrade head
```

## 项目结构

```
v1/
├── backend/
│   ├── app/
│   │   ├── api/v1/          # API 路由
│   │   ├── db/models/       # 数据库模型
│   │   ├── services/        # 业务逻辑
│   │   └── domain/engine/   # Backtrader 集成
│   ├── workers/             # ARQ 任务
│   └── scripts/             # 工具脚本
├── frontend/
│   ├── src/
│   │   ├── components/      # UI 组件
│   │   ├── features/        # 功能模块
│   │   ├── api/generated/   # 自动生成的 API 客户端
│   │   └── pages/           # 页面组件
│   └── orval.config.ts      # API 生成配置
└── docker-compose.yml
```

## API 端点

- `POST /api/v1/strategies` - 创建策略
- `GET /api/v1/strategies` - 策略列表
- `POST /api/v1/backtests` - 创建回测任务
- `GET /api/v1/backtests/{id}/results` - 获取回测结果
- `GET /api/v1/stocks` - 股票列表
- `GET /api/v1/stocks/{code}/kline` - K线数据

## 数据源

- A股日线数据: `/Users/dan/Code/q/trading_data/a_stock_2024.db`
- 包含 5662 只股票，136 万条日线记录 (2024年全年)

## License

Private
