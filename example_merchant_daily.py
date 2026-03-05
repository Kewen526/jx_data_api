#!/usr/bin/env python3
"""抖音商家日报 API 请求示例"""

import requests

API_URL = "http://localhost:8000/api/report/merchant-daily"

response = requests.post(API_URL, json={"data_date": "2026-03-04"})

if response.status_code == 200:
    filename = "抖音商家日报_20260304.xlsx"
    with open(filename, "wb") as f:
        f.write(response.content)
    print(f"报表已保存: {filename}")
else:
    print(f"请求失败 [{response.status_code}]: {response.text}")
