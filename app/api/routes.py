#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
API 路由定义
提供4个报表生成接口
"""

import os
from typing import List, Optional
from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from app.core.queue import get_task_queue
from app.services.report import (
    generate_daily_report,
    generate_weekly_report,
    generate_monthly_report,
    generate_custom_report
)

router = APIRouter(prefix="/api/report", tags=["报表生成"])


# ==================== 请求模型 ====================
class DailyReportRequest(BaseModel):
    """日报请求参数"""
    report_date: str = Field(..., description="报表日期，格式: YYYY-MM-DD", example="2025-12-18")
    accounts: Optional[List[str]] = Field(None, description="门店账号列表", example=["13718175572a", "19318574226a"])


class WeeklyReportRequest(BaseModel):
    """周报请求参数"""
    week1_start: str = Field(..., description="第一周开始日期", example="2025-12-01")
    week1_end: str = Field(..., description="第一周结束日期", example="2025-12-07")
    week2_start: str = Field(..., description="第二周开始日期", example="2025-12-08")
    week2_end: str = Field(..., description="第二周结束日期", example="2025-12-14")
    accounts: Optional[List[str]] = Field(None, description="门店账号列表")


class MonthlyReportRequest(BaseModel):
    """月报请求参数"""
    month1_start: str = Field(..., description="第一个月开始日期", example="2025-11-01")
    month1_end: str = Field(..., description="第一个月结束日期", example="2025-11-30")
    month2_start: str = Field(..., description="第二个月开始日期", example="2025-12-01")
    month2_end: str = Field(..., description="第二个月结束日期", example="2025-12-31")
    accounts: Optional[List[str]] = Field(None, description="门店账号列表")


class CustomReportRequest(BaseModel):
    """自定义报表请求参数"""
    period1_start: str = Field(..., description="第一个时期开始日期", example="2025-12-01")
    period1_end: str = Field(..., description="第一个时期结束日期", example="2025-12-07")
    period2_start: str = Field(..., description="第二个时期开始日期", example="2025-12-08")
    period2_end: str = Field(..., description="第二个时期结束日期", example="2025-12-14")
    shop_ids: Optional[List[str]] = Field(None, description="门店ID列表")
    accounts: Optional[List[str]] = Field(None, description="门店账号列表")


# ==================== API 路由 ====================
@router.post("/daily", summary="生成日报", description="生成指定日期的门店日报")
async def create_daily_report(request: DailyReportRequest):
    """
    生成日报
    - 传入日期和可选的账号列表
    - 返回 Excel 文件下载
    """
    try:
        queue = get_task_queue()
        file_path = await queue.run_task(
            generate_daily_report,
            request.report_date,
            request.accounts
        )

        if not file_path or not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail="报表生成失败，未找到文件")

        filename = os.path.basename(file_path)
        return FileResponse(
            path=file_path,
            filename=filename,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"报表生成失败: {str(e)}")


@router.post("/weekly", summary="生成周报", description="生成两周对比的周报")
async def create_weekly_report(request: WeeklyReportRequest):
    """
    生成周报
    - 传入两周的起止日期
    - 返回 Excel 文件下载
    """
    try:
        queue = get_task_queue()
        file_path = await queue.run_task(
            generate_weekly_report,
            request.week1_start,
            request.week1_end,
            request.week2_start,
            request.week2_end,
            request.accounts
        )

        if not file_path or not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail="报表生成失败，未找到文件")

        filename = os.path.basename(file_path)
        return FileResponse(
            path=file_path,
            filename=filename,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"报表生成失败: {str(e)}")


@router.post("/monthly", summary="生成月报", description="生成两个月对比的月报")
async def create_monthly_report(request: MonthlyReportRequest):
    """
    生成月报
    - 传入两个月的起止日期
    - 返回 Excel 文件下载
    """
    try:
        queue = get_task_queue()
        file_path = await queue.run_task(
            generate_monthly_report,
            request.month1_start,
            request.month1_end,
            request.month2_start,
            request.month2_end,
            request.accounts
        )

        if not file_path or not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail="报表生成失败，未找到文件")

        filename = os.path.basename(file_path)
        return FileResponse(
            path=file_path,
            filename=filename,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"报表生成失败: {str(e)}")


@router.post("/custom", summary="生成自定义报表", description="生成两个自定义时间段对比的报表")
async def create_custom_report(request: CustomReportRequest):
    """
    生成自定义报表
    - 传入两个时间段的起止日期
    - 可选传入门店ID列表或账号列表进行筛选
    - 返回 Excel 文件下载
    """
    try:
        queue = get_task_queue()
        file_path = await queue.run_task(
            generate_custom_report,
            request.period1_start,
            request.period1_end,
            request.period2_start,
            request.period2_end,
            request.shop_ids,
            request.accounts
        )

        if not file_path or not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail="报表生成失败，未找到文件")

        filename = os.path.basename(file_path)
        return FileResponse(
            path=file_path,
            filename=filename,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"报表生成失败: {str(e)}")
