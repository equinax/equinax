# Quant Backtest - 量化交易回测系统

生产级量化交易回测系统，支持策略代码数据库存储、批量回测、策略对比分析。

## 技术栈

- **后端**: FastAPI + PostgreSQL + ARQ + Backtrader
- **前端**: React + TypeScript + shadcn/ui + TradingView Charts
- **部署**: Docker Compose

## 快速开始

### 方式一：Docker Compose（推荐）

```bash
# 克隆项目
git clone https://github.com/equinax/trader.git
cd trader

# 启动所有服务
docker-compose up -d

# 等待服务启动后，运行数据库迁移
docker-compose exec api alembic upgrade head

# 导入示例数据
docker-compose exec api python scripts/migrate_sqlite.py

# 访问应用
# 前端: http://localhost:3000
# API文档: http://localhost:8000/api/docs
```

### 方式二：本地开发

#### 1. 准备数据库

```bash
# 确保 PostgreSQL 和 Redis 已启动
# 创建数据库
createdb quantdb
```

#### 2. 启动后端

```bash
cd backend

# 创建虚拟环境
python -m venv .venv
source .venv/bin/activate

# 安装依赖
pip install -e ".[dev]"

# 配置环境变量
cp .env.example .env
# 编辑 .env 设置数据库连接

# 运行数据库迁移
alembic upgrade head

# 导入示例数据（内置15只股票）
python scripts/migrate_sqlite.py

# 或导入完整数据集（如果有）
# python scripts/migrate_sqlite.py --source /path/to/a_stock_2024.db

# 启动 API 服务
uvicorn app.main:app --reload
```

#### 3. 启动前端

```bash
cd frontend

# 安装依赖
pnpm install

# 配置环境变量
cp .env.example .env

# 生成 API 客户端（需要后端运行）
pnpm run api:generate

# 启动开发服务器
pnpm dev
```

### 3. 访问应用

- **前端**: http://localhost:5173 (本地开发) 或 http://localhost:3000 (Docker)
- **API文档**: http://localhost:8000/api/docs
- **OpenAPI JSON**: http://localhost:8000/api/openapi.json

## 示例数据

项目内置了精选的示例数据（`backend/examples/data/sample_data.db`），包含：

| 类型 | 股票 |
|------|------|
| 主要指数 | 上证综指、沪深300、深证成指、创业板指 |
| 蓝筹股 | 贵州茅台、中国平安、平安银行、五粮液、宁德时代、比亚迪 |
| 金融股 | 招商银行、工商银行 |
| 消费/医药 | 美的集团、恒瑞医药、海康威视 |

共 15 只股票，约 3,600 条日线数据（2024年全年）。

如需完整数据（5,662只股票，136万条记录），请使用外部数据源：

```bash
python scripts/migrate_sqlite.py --source /path/to/a_stock_2024.db
```

## 项目结构

```
trader/
├── backend/
│   ├── app/
│   │   ├── api/v1/          # API 路由
│   │   ├── db/models/       # 数据库模型
│   │   ├── services/        # 业务逻辑
│   │   └── domain/engine/   # Backtrader 集成
│   ├── alembic/             # 数据库迁移
│   │   └── versions/        # 迁移文件
│   ├── workers/             # ARQ 任务
│   ├── scripts/             # 工具脚本
│   └── examples/
│       ├── data/            # 示例数据 (sample_data.db)
│       └── strategies/      # 策略代码示例
├── frontend/
│   ├── src/
│   │   ├── components/      # UI 组件
│   │   ├── api/generated/   # 自动生成的 API 客户端
│   │   └── pages/           # 页面组件
│   └── orval.config.ts      # API 生成配置
└── docker-compose.yml
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

## 数据库迁移

### 本地开发

```bash
cd backend

# 创建新迁移
alembic revision --autogenerate -m "description"

# 执行迁移
alembic upgrade head

# 回滚迁移
alembic downgrade -1
```

### Docker 环境

```bash
# 执行迁移
docker-compose exec api alembic upgrade head

# 创建新迁移（修改模型后）
docker-compose exec api alembic revision --autogenerate -m "add new field"

# 查看迁移历史
docker-compose exec api alembic history

# 查看当前版本
docker-compose exec api alembic current

# 回滚到上一版本
docker-compose exec api alembic downgrade -1

# 回滚到指定版本
docker-compose exec api alembic downgrade <revision_id>
```

**注意**: 在 Docker 中创建迁移后，迁移文件会生成在容器内的 `/app/alembic/versions/` 目录。
由于 `docker-compose.yml` 已配置 volume 映射 (`./backend/alembic:/app/alembic:cached`)，
新生成的迁移文件会自动同步到本地 `backend/alembic/versions/` 目录。

### 常见操作流程

1. **添加新模型或修改字段**:
   ```bash
   # 1. 修改 backend/app/db/models/ 中的模型代码
   # 2. 生成迁移文件
   docker-compose exec api alembic revision --autogenerate -m "add user avatar field"
   # 3. 检查生成的迁移文件 (backend/alembic/versions/xxx_add_user_avatar_field.py)
   # 4. 执行迁移
   docker-compose exec api alembic upgrade head
   ```

2. **重置数据库** (开发环境):
   ```bash
   # 删除所有数据并重新创建
   docker-compose down -v
   docker-compose up -d
   docker-compose exec api alembic upgrade head
   docker-compose exec api python scripts/migrate_sqlite.py
   ```

## 环境变量

### 后端 (backend/.env)

```env
DATABASE_URL=postgresql+asyncpg://user:pass@localhost:5432/quantdb
REDIS_URL=redis://localhost:6379/0
SECRET_KEY=your-secret-key
DEBUG=true
```

### 前端 (frontend/.env)

```env
VITE_API_URL=http://localhost:8000/api
```

## License

Private
