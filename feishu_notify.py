#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
飞书消息推送工具
用于将报告内容发送到飞书
"""

import json
import requests

# 飞书配置
FEISHU_APP_ID = "cli_a912ec6b53f8dcc1"
FEISHU_APP_SECRET = "WhxP812QPaXW5HoVepuLUdkeb7 ETzBoC"
FEISHU_OPEN_ID = "ou_02adad263d6f1d91a66e00367d3b8567"


def get_tenant_token():
    """获取飞书 tenant_access_token"""
    resp = requests.post(
        "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
        json={"app_id": FEISHU_APP_ID, "app_secret": FEISHU_APP_SECRET}
    )
    data = resp.json()
    token = data.get("tenant_access_token")
    if not token:
        print(f"❌ 获取飞书 token 失败: {data}")
        return None
    return token


def send_to_feishu(title, content, open_id=None):
    """
    发送 Markdown 消息到飞书，内容过长时分多条发送
    
    Args:
        title: 消息标题
        content: 消息内容（Markdown）
        open_id: 接收者 open_id，默认使用配置的值
    """
    token = get_tenant_token()
    if not token:
        return False
    
    target_open_id = open_id or FEISHU_OPEN_ID
    
    # 按段落拆分，每段不超过 2800 字符（飞书消息限制）
    chunks = []
    current = ""
    for line in content.split('\n'):
        if len(current) + len(line) + 1 > 2800:
            if current:
                chunks.append(current)
            current = line
        else:
            current = current + '\n' + line if current else line
    if current:
        chunks.append(current)
    
    # 如果没有内容，发送一个简短消息
    if not chunks or (len(chunks) == 1 and len(chunks[0].strip()) == 0):
        chunks = ["报告内容为空"]
    
    success = True
    for i, chunk in enumerate(chunks):
        part = f" ({i+1}/{len(chunks)})" if len(chunks) > 1 else ""
        
        # 使用卡片消息，内容用代码块包裹保持格式
        card_content = {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": f"{title}{part}"},
                "template": "blue"
            },
            "elements": [
                {"tag": "markdown", "content": f"```\n{chunk}\n```"}
            ]
        }
        
        resp = requests.post(
            "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=open_id",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "receive_id": target_open_id,
                "msg_type": "interactive",
                "content": json.dumps(card_content)
            }
        )
        
        result = resp.json()
        if result.get("code") != 0:
            print(f"❌ 飞书发送失败 (第{i+1}条): {result}")
            success = False
        else:
            print(f"✅ 飞书发送成功 (第{i+1}/{len(chunks)}条)")
    
    return success
