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

### 数据库命令

| 命令 | 描述 |
|------|------|
| `just db-setup` | 完整数据库初始化（迁移 + 种子数据） |
| `just db-migrate` | 运行数据库迁移 |
| `just db-migrate-new "msg"` | 创建新迁移 |
| `just db-migrate-down` | 回滚一个版本 |
| `just db-migrate-status` | 查看迁移状态 |
| `just db-reset` | 重置数据库（需确认） |
| `just db-status` | 查看数据统计 |
| `just db-console` | 打开 psql 控制台 |

### 种子数据命令

| 命令 | 描述 |
|------|------|
| `just seed-all` | 导入所有种子数据 |
| `just seed-user` | 创建默认用户 |
| `just seed-strategy` | 加载默认策略 |
| `just seed-stocks` | 加载示例股票数据（内置15只） |
| `just seed-stocks-file FILE` | 从外部 SQLite 文件导入股票数据 |
| `just seed-stocks-clear` | 清空股票数据 |

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

## 示例数据

项目内置了精选的示例数据（`backend/examples/data/sample_data.db`），包含：

| 类型 | 股票 |
|------|------|
| 主要指数 | 上证综指、沪深300、深证成指、创业板指 |
| 蓝筹股 | 贵州茅台、中国平安、平安银行、五粮液、宁德时代、比亚迪 |
| 金融股 | 招商银行、工商银行 |
| 消费/医药 | 美的集团、恒瑞医药、海康威视 |

共 15 只股票，约 3,600 条日线数据（2024年全年）。

### 导入完整数据集

如需完整数据（5,662只股票，136万条记录），请使用外部数据源：

```bash
just seed-stocks-file /path/to/a_stock_2024.db
```

### 切换数据源

如需切换到不同的数据源，先清空现有数据：

```bash
just seed-stocks-clear
just seed-stocks-file /path/to/new_data.db
```

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
│   │   ├── cli/            # CLI 命令 (db, seed)
│   │   ├── db/models/      # 数据库模型
│   │   ├── services/       # 业务逻辑
│   │   └── domain/engine/  # Backtrader 集成
│   ├── alembic/            # 数据库迁移
│   │   └── versions/       # 迁移文件
│   ├── workers/            # ARQ 任务
│   ├── cmd.py              # CLI 入口
│   └── examples/
│       ├── data/           # 示例数据 + 默认策略
│       └── strategies/     # 策略代码示例
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

## CLI 命令（容器内）

在容器内可以直接使用 Python CLI：

```bash
# 进入容器
just dev-shell

# 数据库命令
python cmd.py db reset          # 重置数据库
python cmd.py db status         # 查看状态

# 种子数据命令
python cmd.py seed user         # 创建默认用户
python cmd.py seed strategy     # 加载默认策略
python cmd.py seed stocks       # 加载股票数据
python cmd.py seed stocks --clear   # 清空股票数据
python cmd.py seed stocks --source /path/to/data.db  # 从指定文件加载
python cmd.py seed all          # 加载所有种子数据
```

## License

MIT
