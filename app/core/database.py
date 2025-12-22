#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库连接池单例模块
确保整个应用只使用一个连接池实例
"""

import mysql.connector
from mysql.connector import pooling
from contextlib import contextmanager
from typing import Optional

from app.core.config import settings


class DatabasePool:
    """数据库连接池单例类"""

    _instance: Optional['DatabasePool'] = None
    _pool: Optional[pooling.MySQLConnectionPool] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if self._pool is None:
            self._init_pool()

    def _init_pool(self):
        """初始化连接池"""
        db_config = {
            'host': settings.DB_HOST,
            'port': settings.DB_PORT,
            'user': settings.DB_USER,
            'password': settings.DB_PASSWORD,
            'database': settings.DB_NAME,
            'charset': settings.DB_CHARSET,
            'use_unicode': True,
            'autocommit': True
        }

        self._pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name=settings.DB_POOL_NAME,
            pool_size=settings.DB_POOL_SIZE,
            pool_reset_session=True,
            **db_config
        )
        print(f"数据库连接池已初始化: pool_size={settings.DB_POOL_SIZE}")

    def get_connection(self):
        """获取数据库连接"""
        return self._pool.get_connection()

    @contextmanager
    def get_cursor(self, dictionary=True):
        """
        获取游标的上下文管理器
        自动管理连接和游标的关闭
        """
        conn = self.get_connection()
        cursor = conn.cursor(dictionary=dictionary)
        try:
            yield cursor
        finally:
            cursor.close()
            conn.close()


# 全局单例实例
db_pool = DatabasePool()


def get_db_pool() -> DatabasePool:
    """获取数据库连接池实例"""
    return db_pool
