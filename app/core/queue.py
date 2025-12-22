#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
请求队列管理器
使用信号量控制并发数，实现请求排队处理
"""

import asyncio
from typing import Callable, Any
from functools import wraps

from app.core.config import settings


class TaskQueue:
    """任务队列管理器"""

    _instance = None
    _semaphore: asyncio.Semaphore = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        # 注意：Semaphore 需要在事件循环中创建
        pass

    def get_semaphore(self) -> asyncio.Semaphore:
        """获取或创建信号量"""
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(settings.MAX_WORKERS)
        return self._semaphore

    async def run_task(self, func: Callable, *args, **kwargs) -> Any:
        """
        在队列中运行任务
        使用信号量控制并发数
        """
        semaphore = self.get_semaphore()
        async with semaphore:
            # 在线程池中运行同步任务
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, lambda: func(*args, **kwargs))
            return result


# 全局队列实例
task_queue = TaskQueue()


def queued_task(func: Callable):
    """
    装饰器：将同步函数包装为队列任务
    使用方式：
    @queued_task
    def my_sync_function():
        pass

    # 调用时
    result = await my_sync_function()
    """

    @wraps(func)
    async def wrapper(*args, **kwargs):
        return await task_queue.run_task(func, *args, **kwargs)

    return wrapper


def get_task_queue() -> TaskQueue:
    """获取任务队列实例"""
    return task_queue
