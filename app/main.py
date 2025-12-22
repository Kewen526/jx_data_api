#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
江鑫数据报表 API 服务
FastAPI 主入口
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import router as report_router
from app.core.config import settings
from app.services.report import ensure_temp_dir


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    # 启动时
    print("=" * 60)
    print("江鑫数据报表 API 服务启动中...")
    print(f"最大并发处理数: {settings.MAX_WORKERS}")
    print(f"数据库连接池大小: {settings.DB_POOL_SIZE}")
    print("=" * 60)

    # 确保临时目录存在
    ensure_temp_dir()

    yield

    # 关闭时
    print("服务关闭中...")


# 创建 FastAPI 应用
app = FastAPI(
    title="江鑫数据报表 API",
    description="""
## 功能说明

提供门店数据报表生成服务，支持以下报表类型：

- **日报**: 指定日期的门店数据报表
- **周报**: 两周数据对比报表
- **月报**: 两个月数据对比报表
- **自定义报表**: 自定义时间段对比报表

## 使用说明

1. 调用对应接口，传入所需参数
2. 系统会排队处理请求（最多同时处理5个）
3. 处理完成后返回 Excel 文件下载

## 注意事项

- 所有日期格式为: `YYYY-MM-DD`
- `accounts` 参数为可选，用于筛选特定账号的门店
    """,
    version="1.0.0",
    lifespan=lifespan
)

# CORS 配置（允许所有来源）
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册路由
app.include_router(report_router)


# 健康检查接口
@app.get("/health", tags=["系统"])
async def health_check():
    """健康检查接口"""
    return {"status": "ok", "message": "服务运行正常"}


@app.get("/", tags=["系统"])
async def root():
    """根路径"""
    return {
        "name": "江鑫数据报表 API",
        "version": "1.0.0",
        "docs": "/docs",
        "endpoints": {
            "日报": "POST /api/report/daily",
            "周报": "POST /api/report/weekly",
            "月报": "POST /api/report/monthly",
            "自定义": "POST /api/report/custom"
        }
    }
