# 江鑫数据报表 API

基于 FastAPI 的门店数据报表生成服务，支持日报、周报、月报和自定义报表的生成与下载。

## 功能特性

- **4种报表类型**: 日报、周报、月报、自定义报表
- **连接池复用**: 数据库连接池单例模式，避免连接泄漏
- **请求排队**: 信号量控制并发，支持同时处理5个请求
- **Docker 部署**: 一键部署到云服务器

## 项目结构

```
jx_data_api/
├── app/
│   ├── main.py              # FastAPI 入口
│   ├── api/
│   │   └── routes.py        # API 路由
│   ├── core/
│   │   ├── config.py        # 配置
│   │   ├── database.py      # 数据库连接池
│   │   └── queue.py         # 请求队列
│   └── services/
│       └── report.py        # 报表生成逻辑
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── deploy.sh                # 部署脚本
```

## API 接口

| 接口 | 方法 | 说明 |
|------|------|------|
| `/api/report/daily` | POST | 生成日报 |
| `/api/report/weekly` | POST | 生成周报 |
| `/api/report/monthly` | POST | 生成月报 |
| `/api/report/custom` | POST | 生成自定义报表 |

## 快速部署

### 1. 上传代码到服务器

```bash
# 克隆代码到服务器
git clone <repository-url>
cd jx_data_api
```

### 2. 一键部署

```bash
chmod +x deploy.sh
./deploy.sh
```

### 3. 手动部署（可选）

```bash
# 构建并启动
docker compose up -d --build

# 查看日志
docker compose logs -f

# 停止服务
docker compose down
```

## 接口调用示例

### 生成日报

```bash
curl -X POST http://localhost:8000/api/report/daily \
  -H "Content-Type: application/json" \
  -d '{"report_date": "2025-12-18", "accounts": ["13718175572a"]}' \
  --output daily_report.xlsx
```

### 生成周报

```bash
curl -X POST http://localhost:8000/api/report/weekly \
  -H "Content-Type: application/json" \
  -d '{
    "week1_start": "2025-12-01",
    "week1_end": "2025-12-07",
    "week2_start": "2025-12-08",
    "week2_end": "2025-12-14"
  }' \
  --output weekly_report.xlsx
```

### 生成月报

```bash
curl -X POST http://localhost:8000/api/report/monthly \
  -H "Content-Type: application/json" \
  -d '{
    "month1_start": "2025-11-01",
    "month1_end": "2025-11-30",
    "month2_start": "2025-12-01",
    "month2_end": "2025-12-31"
  }' \
  --output monthly_report.xlsx
```

### 生成自定义报表

```bash
curl -X POST http://localhost:8000/api/report/custom \
  -H "Content-Type: application/json" \
  -d '{
    "period1_start": "2025-12-01",
    "period1_end": "2025-12-07",
    "period2_start": "2025-12-08",
    "period2_end": "2025-12-14",
    "accounts": ["13718175572a", "19318574226a"]
  }' \
  --output custom_report.xlsx
```

## API 文档

服务启动后访问: `http://your-server:8000/docs`

## 配置说明

可通过环境变量覆盖默认配置：

| 变量 | 默认值 | 说明 |
|------|--------|------|
| DB_HOST | 8.146.210.145 | 数据库地址 |
| DB_PORT | 3306 | 数据库端口 |
| DB_USER | root | 数据库用户 |
| DB_PASSWORD | - | 数据库密码 |
| DB_NAME | jx_data_info | 数据库名称 |
| DB_POOL_SIZE | 20 | 连接池大小 |
| MAX_WORKERS | 5 | 最大并发处理数 |
