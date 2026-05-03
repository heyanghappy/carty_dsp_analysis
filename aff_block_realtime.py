#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
实时查询最近10分钟各 AFF 请求量、block比例、曝光量、点击量、作弊率
结果保存为 md 文件
"""

import pymysql
import pandas as pd
from datetime import datetime, timedelta

SR_HOST = "fe-c-907795efe3201917.starrocks.aliyuncs.com"
SR_PORT = 9030
SR_USER = "sandy"
SR_PASSWORD = "MXeptLkEkoi2$FMX"

OUTPUT_DIR = "/home/node/.openclaw/workspace/repos/carty_dsp_analysis"


def get_conn():
    return pymysql.connect(host=SR_HOST, port=SR_PORT, user=SR_USER, password=SR_PASSWORD,
                           charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)


def query_request(conn, dt, hh, ts_start, ts_now):
    """请求量 + block率，包含全部 aff（Paimon表，可能因Jindo STS region问题失败）"""
    sql = f"""
        SELECT affiliate_id, affiliate_name,
               COUNT(*) AS requests,
               SUM(CASE WHEN is_block = 'true' THEN 1 ELSE 0 END) AS blocked
        FROM assembly.dsp.ods_dsp_request
        WHERE dt = '{dt}' AND hh = '{hh}'
          AND CAST(kafka_timestamp AS BIGINT) >= {ts_start}
          AND CAST(kafka_timestamp AS BIGINT) <= {ts_now}
        GROUP BY affiliate_id, affiliate_name
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=['affiliate_id','affiliate_name','requests','blocked'])


def query_imp(conn, dt, hh, ts_start, ts_now):
    """曝光量 + 曝光作弊率"""
    sql = f"""
        SELECT affiliate_id,
               COUNT(DISTINCT bid_id) AS imps,
               SUM(CASE WHEN cheat = 'true' THEN 1 ELSE 0 END) AS cheat_imps
        FROM assembly.dsp.ods_dsp_imp
        WHERE dt = '{dt}' AND hh = '{hh}'
          AND CAST(kafka_timestamp AS BIGINT) >= {ts_start}
          AND CAST(kafka_timestamp AS BIGINT) <= {ts_now}
        GROUP BY affiliate_id
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=['affiliate_id','imps','cheat_imps'])


def query_click(conn, dt, hh, ts_start, ts_now):
    """点击量 + 点击作弊率"""
    sql = f"""
        SELECT affiliate_id,
               COUNT(DISTINCT bid_id) AS clicks,
               SUM(CASE WHEN cheat = 'true' THEN 1 ELSE 0 END) AS cheat_clicks
        FROM assembly.dsp.ods_dsp_click
        WHERE dt = '{dt}' AND hh = '{hh}'
          AND CAST(kafka_timestamp AS BIGINT) >= {ts_start}
          AND CAST(kafka_timestamp AS BIGINT) <= {ts_now}
        GROUP BY affiliate_id
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=['affiliate_id','clicks','cheat_clicks'])


def main():
    import sys
    # 支持传入指定时间，格式：YYYY-MM-DD HH:MM，例如 "2026-03-25 01:05"
    if len(sys.argv) > 1:
        now = datetime.strptime(sys.argv[1], '%Y-%m-%d %H:%M')
        print(f"使用指定时间: {now}")
    else:
        now = datetime.now()

    ts_now = int(now.timestamp() * 1000)
    ts_start = int((now - timedelta(minutes=10)).timestamp() * 1000)

    utc_hour = now.hour - 8
    if utc_hour < 0:
        utc_hour += 24
        dt = (now - timedelta(days=1)).strftime('%Y%m%d')  # 跨天
    else:
        dt = now.strftime('%Y%m%d')
    hh = str(utc_hour).zfill(2)

    start_time = datetime.fromtimestamp(ts_start / 1000).strftime('%H:%M:%S')
    end_time = now.strftime('%H:%M:%S')
    print(f"查询时间窗口: {start_time} ~ {end_time}")

    conn = get_conn()
    try:
        # ods_dsp_request 是 Paimon 表，可能因 StarRocks Jindo STS region 配置问题失败
        # 失败时降级：仅输出 imp+click 实时作弊率，block 数据标注为不可用
        req_error = None
        try:
            req_df = query_request(conn, dt, hh, ts_start, ts_now)
        except Exception as e:
            req_error = str(e)
            print(f"⚠️ ods_dsp_request 查询失败（已知 Jindo STS region 问题）: {e}")
            req_df = pd.DataFrame(columns=['affiliate_id','affiliate_name','requests','blocked'])

        imp_df = query_imp(conn, dt, hh, ts_start, ts_now)
        click_df = query_click(conn, dt, hh, ts_start, ts_now)
    finally:
        conn.close()

    if req_df.empty and imp_df.empty and click_df.empty:
        print("暂无数据（当前分区可能尚未写入）")
        return

    # 以 imp 或 click 为基础（req 不可用时）
    if req_df.empty:
        base_df = imp_df[['affiliate_id']].drop_duplicates() if not imp_df.empty else click_df[['affiliate_id']].drop_duplicates()
        # affiliate_name 不可用时用 id 代替
        base_df = base_df.copy()
        base_df['affiliate_name'] = base_df['affiliate_id'].astype(str)
        base_df['requests'] = 0
        base_df['blocked'] = 0
    else:
        base_df = req_df.copy()

    df = base_df.merge(imp_df, on='affiliate_id', how='outer')
    df = df.merge(click_df, on='affiliate_id', how='outer')
    df = df.fillna(0)

    # affiliate_name 可能因 outer join 变 NaN
    if 'affiliate_name' in df.columns:
        df['affiliate_name'] = df.apply(
            lambda r: r['affiliate_name'] if r['affiliate_name'] and r['affiliate_name'] != '0' else str(int(r['affiliate_id'])),
            axis=1
        )

    for col in ['requests','blocked','imps','cheat_imps','clicks','cheat_clicks']:
        df[col] = df[col].astype(int)

    df['block_rate'] = (df['blocked'] / df['requests'].replace(0, 1) * 100).round(2) if not req_df.empty else None
    df['imp_cheat_rate'] = (df['cheat_imps'] / df['imps'].replace(0, 1) * 100).round(2)
    df['click_cheat_rate'] = (df['cheat_clicks'] / df['clicks'].replace(0, 1) * 100).round(2)
    sort_col = 'requests' if not req_df.empty else 'imps'
    df = df.sort_values(sort_col, ascending=False)

    # 汇总行
    total = {
        'requests': df['requests'].sum(),
        'blocked': df['blocked'].sum(),
        'imps': df['imps'].sum(),
        'cheat_imps': df['cheat_imps'].sum(),
        'clicks': df['clicks'].sum(),
        'cheat_clicks': df['cheat_clicks'].sum(),
    }
    total['block_rate'] = round(total['blocked'] / max(total['requests'], 1) * 100, 2) if not req_df.empty else None
    total['imp_cheat_rate'] = round(total['cheat_imps'] / max(total['imps'], 1) * 100, 2)
    total['click_cheat_rate'] = round(total['cheat_clicks'] / max(total['clicks'], 1) * 100, 2)

    block_unavailable = req_df.empty  # block 数据是否不可用

    # 生成 md
    lines = []
    lines.append(f"# AFF 流量质量实时报告\n")
    lines.append(f"**查询时间:** {now.strftime('%Y-%m-%d %H:%M:%S')}  ")
    lines.append(f"**时间窗口:** {start_time} ~ {end_time}（最近10分钟）\n")

    if block_unavailable:
        lines.append(f"> ⚠️ **block 率数据不可用**：ods_dsp_request（Paimon表）查询失败，已知 StarRocks Jindo STS region 配置问题，需 DBA 修复。以下仅展示曝光/点击作弊率。\n")

    lines.append("## 汇总\n")
    lines.append("| 指标 | 数值 |")
    lines.append("|------|------|")
    if not block_unavailable:
        lines.append(f"| 总请求量 | {total['requests']:,} |")
        lines.append(f"| 总 blocked | {total['blocked']:,} |")
        lines.append(f"| 整体 block 率 | {total['block_rate']}% |")
    lines.append(f"| 总曝光量 | {total['imps']:,} |")
    lines.append(f"| 曝光作弊率 | {total['imp_cheat_rate']}% |")
    lines.append(f"| 总点击量 | {total['clicks']:,} |")
    lines.append(f"| 点击作弊率 | {total['click_cheat_rate']}% |")
    lines.append(f"| AFF 数量 | {len(df)} |")

    lines.append("\n## 各 AFF 明细\n")
    if not block_unavailable:
        lines.append("| aff_id | aff_name | 请求量 | block率 | 曝光量 | 曝光作弊率 | 点击量 | 点击作弊率 |")
        lines.append("|--------|----------|-------:|---------|-------:|----------|-------:|----------|")
        for _, row in df.iterrows():
            flag = " **!!**" if row['block_rate'] >= 60 else ""
            lines.append(
                f"| {row['affiliate_id']} | {row['affiliate_name']} "
                f"| {int(row['requests']):,} | {row['block_rate']}%{flag} "
                f"| {int(row['imps']):,} | {row['imp_cheat_rate']}% "
                f"| {int(row['clicks']):,} | {row['click_cheat_rate']}% |"
            )
        lines.append("\n## 高 block 率 AFF（≥60%）\n")
        high_block = df[df['block_rate'] >= 60].sort_values('block_rate', ascending=False)
        if not high_block.empty:
            lines.append("| aff_id | aff_name | 请求量 | block率 |")
            lines.append("|--------|----------|-------:|---------|")
            for _, row in high_block.iterrows():
                lines.append(f"| {row['affiliate_id']} | {row['affiliate_name']} | {int(row['requests']):,} | {row['block_rate']}% |")
    else:
        lines.append("| aff_id | aff_name | 曝光量 | 曝光作弊率 | 点击量 | 点击作弊率 |")
        lines.append("|--------|----------|-------:|----------|-------:|----------|")
        for _, row in df.iterrows():
            imp_flag = " **!!**" if row['imp_cheat_rate'] >= 30 else ""
            click_flag = " **!!**" if row['click_cheat_rate'] >= 30 else ""
            lines.append(
                f"| {row['affiliate_id']} | {row['affiliate_name']} "
                f"| {int(row['imps']):,} | {row['imp_cheat_rate']}%{imp_flag} "
                f"| {int(row['clicks']):,} | {row['click_cheat_rate']}%{click_flag} |"
            )

    report = "\n".join(lines)

    filename = f"{OUTPUT_DIR}/aff_block_{now.strftime('%Y%m%d_%H%M')}.md"
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(report)

    if block_unavailable:
        print(f"✅ 报告已保存（降级模式，无block率）: {filename}")
        print(f"   AFF 数量: {len(df)}, 曝光作弊率: {total['imp_cheat_rate']}%, 点击作弊率: {total['click_cheat_rate']}%")
    else:
        print(f"✅ 报告已保存: {filename}")
        print(f"   AFF 数量: {len(df)}, 总请求: {total['requests']:,}, block率: {total['block_rate']}%")


if __name__ == "__main__":
    main()
