#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
配置模块
"""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # 数据库配置
    DB_HOST: str = "8.146.210.145"
    DB_PORT: int = 3306
    DB_USER: str = "root"
    DB_PASSWORD: str = "Kewen888@"
    DB_NAME: str = "jx_data_info"
    DB_CHARSET: str = "utf8mb4"

    # 连接池配置
    DB_POOL_SIZE: int = 20
    DB_POOL_NAME: str = "jx_pool"

    # 队列配置
    MAX_WORKERS: int = 5  # 最大并发处理数

    # 临时文件目录
    TEMP_DIR: str = "/tmp/jx_reports"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
