#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
广告主作弊分析周报
支持参数：
  无参数：默认最近7天
  1个参数：指定天数（如 7 表示最近7天）
  2个参数：指定日期范围 start_dt end_dt（YYYYMMDD 格式）
"""

import pymysql
import pandas as pd
from datetime import datetime, timedelta
import requests
import json
import sys

# 从 daily_cheat_report 导入配置
from daily_cheat_report import ADV_GROUPS, ADV_INFO, send_to_feishu

# StarRocks配置
SR_HOST = "fe-c-907795efe3201917.starrocks.aliyuncs.com"
SR_PORT = 9030
SR_USER = "sandy"
SR_PASSWORD = "MXeptLkEkoi2$FMX"

OUTPUT_DIR = "/Users/gztd-03-01457/Work/claude/data_log"


def make_conn():
    return pymysql.connect(host=SR_HOST, port=SR_PORT, user=SR_USER, password=SR_PASSWORD,
                           charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)


def query_all(conn, table, start_dt, end_dt, all_adv_ids):
    """按天循环查询（避免单次 CPU 超限），全量广告主每天一次，最后聚合"""
    placeholders = ','.join([f"'{x}'" for x in all_adv_ids])
    start_date = datetime.strptime(start_dt, '%Y%m%d')
    end_date = datetime.strptime(end_dt, '%Y%m%d')
    all_rows = []
    current = start_date
    while current <= end_date:
        dt_str = current.strftime('%Y%m%d')
        sql = f"""
        SELECT adv_id,
            COUNT(*) AS total_logs,
            COUNT(DISTINCT bid_id) AS distinct_bids,
            COUNT(DISTINCT CASE WHEN cheat = 'true' THEN bid_id END) AS cheat_bids
        FROM {table}
        WHERE dt = '{dt_str}'
          AND adv_id IN ({placeholders})
          AND (cheat = 'true' OR cheat = 'false')
        GROUP BY adv_id
        """
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                rows = cursor.fetchall()
            if rows:
                all_rows.extend(rows)
            print(f"  ✓ {table} {dt_str}: {len(rows) if rows else 0}条")
        except Exception as e:
            print(f"  ✗ {table} {dt_str} 失败: {e}")
        current += timedelta(days=1)
    if not all_rows:
        return pd.DataFrame(columns=['adv_id', 'total_logs', 'distinct_bids', 'cheat_bids'])
    df = pd.DataFrame(all_rows)
    return df.groupby('adv_id', as_index=False).sum()


def main():
    # 解析参数
    if len(sys.argv) == 3:
        start_dt, end_dt = sys.argv[1], sys.argv[2]
        print(f"开始分析: {start_dt} ~ {end_dt} (指定日期范围)")
    elif len(sys.argv) == 2:
        days = int(sys.argv[1])
        end_date = datetime.now() - timedelta(days=1)
        start_date = end_date - timedelta(days=days - 1)
        start_dt = start_date.strftime('%Y%m%d')
        end_dt = end_date.strftime('%Y%m%d')
        print(f"开始分析: {start_dt} ~ {end_dt} (最近{days}天)")
    else:
        end_date = datetime.now() - timedelta(days=1)
        start_date = end_date - timedelta(days=6)
        start_dt = start_date.strftime('%Y%m%d')
        end_dt = end_date.strftime('%Y%m%d')
        print(f"开始分析: {start_dt} ~ {end_dt} (默认最近7天)")

    # 所有广告主合并，只跑 2 次 SQL（imp + click）
    all_adv_ids = [str(i) for ids in ADV_GROUPS.values() for i in ids]
    # adv_id -> group 映射
    adv_to_group = {str(i): g for g, ids in ADV_GROUPS.items() for i in ids}

    conn = make_conn()
    try:
        imp_df = query_all(conn, 'assembly.dsp.ods_dsp_imp', start_dt, end_dt, all_adv_ids)
        click_df = query_all(conn, 'assembly.dsp.ods_dsp_click', start_dt, end_dt, all_adv_ids)
    finally:
        conn.close()

    merged = pd.merge(imp_df, click_df, on='adv_id', how='outer', suffixes=('_imp', '_click')).fillna(0)
    merged['imp_total_logs'] = merged['total_logs_imp'].astype(int)
    merged['imp_distinct_bids'] = merged['distinct_bids_imp'].astype(int)
    merged['imp_cheat_bids'] = merged['cheat_bids_imp'].astype(int)
    merged['imp_cheat_rate_percent'] = (merged['imp_cheat_bids'] / merged['imp_distinct_bids'].replace(0, 1) * 100).round(2)
    merged['click_total_logs'] = merged['total_logs_click'].astype(int)
    merged['click_distinct_bids'] = merged['distinct_bids_click'].astype(int)
    merged['click_cheat_bids'] = merged['cheat_bids_click'].astype(int)
    merged['click_cheat_rate_percent'] = (merged['click_cheat_bids'] / merged['click_distinct_bids'].replace(0, 1) * 100).round(2)
    merged['total_logs'] = merged['imp_total_logs'] + merged['click_total_logs']
    merged['total_distinct_bids'] = merged['imp_distinct_bids'] + merged['click_distinct_bids']
    merged['total_cheat_bids'] = merged['imp_cheat_bids'] + merged['click_cheat_bids']
    merged['total_cheat_rate_percent'] = (merged['total_cheat_bids'] / merged['total_distinct_bids'].replace(0, 1) * 100).round(2)
    merged['group'] = merged['adv_id'].map(adv_to_group)

    # 按 group 拆分
    all_data = {g: merged[merged['group'] == g].copy() for g in ADV_GROUPS}

    # 各分组汇总
    summary_list = []
    for group in ADV_GROUPS:
        df = all_data[group]
        s_imp_logs = df['imp_total_logs'].sum()
        s_imp_bids = df['imp_distinct_bids'].sum()
        s_imp_cheat = df['imp_cheat_bids'].sum()
        s_clk_logs = df['click_total_logs'].sum()
        s_clk_bids = df['click_distinct_bids'].sum()
        s_clk_cheat = df['click_cheat_bids'].sum()
        s_total_logs = s_imp_logs + s_clk_logs
        s_total_bids = s_imp_bids + s_clk_bids
        s_total_cheat = s_imp_cheat + s_clk_cheat
        summary_list.append({
            'group': group,
            'imp_total_logs': s_imp_logs, 'imp_distinct_bids': s_imp_bids, 'imp_cheat_bids': s_imp_cheat,
            'imp_cheat_rate': round(s_imp_cheat/max(s_imp_bids,1)*100, 2),
            'click_total_logs': s_clk_logs, 'click_distinct_bids': s_clk_bids, 'click_cheat_bids': s_clk_cheat,
            'click_cheat_rate': round(s_clk_cheat/max(s_clk_bids,1)*100, 2),
            'total_logs': s_total_logs, 'total_distinct_bids': s_total_bids, 'total_cheat_bids': s_total_cheat,
            'total_cheat_rate': round(s_total_cheat/max(s_total_bids,1)*100, 2),
        })

    # 生成报告（Markdown格式）
    report_lines = []
    report_lines.append(f"# 广告主作弊分析周报 {start_dt}~{end_dt}\n")

    # 零、总体汇总
    summary_df = pd.DataFrame(summary_list)
    total_imp_logs = summary_df['imp_total_logs'].sum()
    total_imp_bids = summary_df['imp_distinct_bids'].sum()
    total_imp_cheat = summary_df['imp_cheat_bids'].sum()
    total_clk_logs = summary_df['click_total_logs'].sum()
    total_clk_bids = summary_df['click_distinct_bids'].sum()
    total_clk_cheat = summary_df['click_cheat_bids'].sum()
    total_all_logs = summary_df['total_logs'].sum()
    total_all_bids = summary_df['total_distinct_bids'].sum()
    total_all_cheat = summary_df['total_cheat_bids'].sum()

    report_lines.append("## 零、总体汇总\n")
    report_lines.append("| 维度 | 总日志数 | 唯一bid数 | 作弊bid数 | 作弊率 |")
    report_lines.append("|------|----------|----------|----------|--------|")
    report_lines.append(f"| 曝光 | {total_imp_logs:,} | {total_imp_bids:,} | {total_imp_cheat:,} | "
                       f"{round(total_imp_cheat/max(total_imp_bids,1)*100, 2)}% |")
    report_lines.append(f"| 点击 | {total_clk_logs:,} | {total_clk_bids:,} | {total_clk_cheat:,} | "
                       f"{round(total_clk_cheat/max(total_clk_bids,1)*100, 2)}% |")
    report_lines.append(f"| 综合 | {total_all_logs:,} | {total_all_bids:,} | {total_all_cheat:,} | "
                       f"{round(total_all_cheat/max(total_all_bids,1)*100, 2)}% |")

    # 一、各分组汇总
    report_lines.append("\n## 一、各分组汇总\n")
    report_lines.append("| 分组 | 曝光日志 | 曝光bid | 曝光作弊 | 曝光作弊率 | 点击日志 | 点击bid | 点击作弊 | 点击作弊率 | 综合作弊率 |")
    report_lines.append("|------|-------:|-------:|-------:|----------|-------:|-------:|-------:|----------|----------|")

    for _, row in summary_df.iterrows():
        report_lines.append(f"| {row['group']} | {row['imp_total_logs']:,} | {row['imp_distinct_bids']:,} | "
                           f"{row['imp_cheat_bids']:,} | {row['imp_cheat_rate']}% | "
                           f"{row['click_total_logs']:,} | {row['click_distinct_bids']:,} | "
                           f"{row['click_cheat_bids']:,} | {row['click_cheat_rate']}% | {row['total_cheat_rate']}% |")

    # 二、各分组分行业汇总
    report_lines.append("\n## 二、各分组分行业汇总\n")
    for group, df in all_data.items():
        report_lines.append(f"### {group}\n")
        df = df.copy()
        df['industry'] = df['adv_id'].apply(lambda x: ADV_INFO.get(int(x), ('', '其他'))[1])
        industry_agg = df.groupby('industry').agg(
            imp_total_logs=('imp_total_logs', 'sum'),
            imp_distinct_bids=('imp_distinct_bids', 'sum'),
            imp_cheat_bids=('imp_cheat_bids', 'sum'),
            click_total_logs=('click_total_logs', 'sum'),
            click_distinct_bids=('click_distinct_bids', 'sum'),
            click_cheat_bids=('click_cheat_bids', 'sum'),
            total_distinct_bids=('total_distinct_bids', 'sum'),
            total_cheat_bids=('total_cheat_bids', 'sum'),
        )
        industry_agg['imp_cheat_rate'] = (industry_agg['imp_cheat_bids'] / industry_agg['imp_distinct_bids'].replace(0,1) * 100).round(2)
        industry_agg['click_cheat_rate'] = (industry_agg['click_cheat_bids'] / industry_agg['click_distinct_bids'].replace(0,1) * 100).round(2)
        industry_agg['total_cheat_rate'] = (industry_agg['total_cheat_bids'] / industry_agg['total_distinct_bids'].replace(0,1) * 100).round(2)
        industry_agg = industry_agg.sort_values('total_cheat_rate', ascending=False)
        report_lines.append("| 行业 | 曝光作弊率 | 点击作弊率 | 综合作弊率 |")
        report_lines.append("|------|----------|----------|----------|")
        for ind, r in industry_agg.iterrows():
            report_lines.append(f"| {ind} | {r['imp_cheat_rate']}% | "
                               f"{r['click_cheat_rate']}% | {r['total_cheat_rate']}% |")
        report_lines.append("")

    # 三、各广告主明细
    report_lines.append("\n## 三、各广告主明细\n")
    for group, df in all_data.items():
        report_lines.append(f"### {group}\n")
        report_lines.append("| adv_id | 广告主 | 行业 | 曝光量 | 曝光作弊率 | 点击量 | 点击作弊率 | 综合作弊率 |")
        report_lines.append("|--------|-------|------|------:|----------|------:|----------|----------|")
        detail = df.sort_values('total_cheat_rate_percent', ascending=False)
        for _, row in detail.iterrows():
            aid = int(row['adv_id'])
            name, ind = ADV_INFO.get(aid, (str(aid), '其他'))
            flag = " **!!**" if row['total_cheat_rate_percent'] >= 15 else ""
            report_lines.append(
                f"| {aid} | {name} | {ind} | {int(row['imp_distinct_bids']):,} | {row['imp_cheat_rate_percent']}% | "
                f"{int(row['click_distinct_bids']):,} | {row['click_cheat_rate_percent']}% | "
                f"{row['total_cheat_rate_percent']}%{flag} |"
            )
        report_lines.append("")

    # 四、高危广告主
    report_lines.append("\n## 四、高危广告主（作弊率>=15%）\n")
    report_lines.append("| 分组 | adv_id | 广告主 | 行业 | 曝光作弊率 | 点击作弊率 | 综合作弊率 |")
    report_lines.append("|------|--------|-------|------|----------|----------|----------|")
    for group, df in all_data.items():
        high_risk = df[df['total_cheat_rate_percent'] >= 15].sort_values('total_cheat_rate_percent', ascending=False)
        for _, row in high_risk.iterrows():
            aid = int(row['adv_id'])
            name, ind = ADV_INFO.get(aid, (str(aid), '其他'))
            report_lines.append(f"| {group} | {aid} | {name} | {ind} | "
                                f"{row['imp_cheat_rate_percent']}% | {row['click_cheat_rate_percent']}% | {row['total_cheat_rate_percent']}% |")

    report = "\n".join(report_lines)

    # 保存文件
    filename = f"{OUTPUT_DIR}/weekly_cheat_report_{start_dt}_{end_dt}.md"
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(report)
    print(f"✅ 报告已保存: {filename}")

    # 发送到飞书
    if send_to_feishu(f"📊 作弊分析周报 {start_dt}~{end_dt}", report):
        print("✅ 已发送到飞书")
    else:
        print("❌ 飞书发送失败")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"❌ 执行失败: {e}")
        sys.exit(1)
