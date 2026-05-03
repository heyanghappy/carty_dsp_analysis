#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
指定广告主的曝光/点击/转化在各风险画像上的分布分析
"""

import pymysql
import sys
from datetime import datetime, timedelta

SR_HOST = "fe-c-907795efe3201917.starrocks.aliyuncs.com"
SR_PORT = 9030
SR_USER = "sandy"
SR_PASSWORD = "MXeptLkEkoi2$FMX"

OUTPUT_DIR = "/home/node/.openclaw/workspace/repos/carty_dsp_analysis"

# 风险等级字段
RISK_FIELDS = [
    'rl_final', 'rl_imp_fraud', 'rl_click_fraud', 'rl_game_af', 
    'rl_game_adjust', 'rl_bundle', 'rl_soigame', 'rl_anura',
    'rl_visible_imp', 'rl_unintent_click', 'rl_N_ctr'
]

RISK_NAMES = {
    'rl_final': '最终风险',
    'rl_imp_fraud': '曝光作弊',
    'rl_click_fraud': '点击作弊',
    'rl_game_af': '游戏AF拒绝',
    'rl_game_adjust': '游戏Adjust拒绝',
    'rl_bundle': 'Bundle',
    'rl_soigame': 'SOI Game',
    'rl_anura': 'Anura',
    'rl_visible_imp': '可见曝光',
    'rl_unintent_click': '非意图点击',
    'rl_N_ctr': 'N CTR',
}

def fmt_num(n):
    return f"{int(n):,}"

def fmt_pct(n, total):
    if not total:
        return "0.00%"
    return f"{n/total*100:.2f}%"

def main():
    if len(sys.argv) < 3:
        print("用法: python3 adv_risk_profile_analysis.py <日期> <广告主ID列表>")
        print("示例: python3 adv_risk_profile_analysis.py 20260423 640,681,724")
        sys.exit(1)
    
    dt = sys.argv[1]
    adv_ids = [int(x.strip()) for x in sys.argv[2].split(',')]
    
    print(f"分析日期: {dt}")
    print(f"广告主: {adv_ids}")
    
    conn = pymysql.connect(
        host=SR_HOST, port=SR_PORT, user=SR_USER, password=SR_PASSWORD,
        charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor
    )
    
    with conn.cursor() as cursor:
        cursor.execute("SET CATALOG assembly")
        
        # 构建风险等级分段 CASE 语句
        def risk_bucket_case(field):
            return f"""
            CASE
                WHEN mp.{field} = 0 THEN '0-未知'
                WHEN mp.{field} BETWEEN 1 AND 20 THEN '1-20-低风险'
                WHEN mp.{field} BETWEEN 21 AND 49 THEN '21-49-中低风险'
                WHEN mp.{field} BETWEEN 50 AND 64 THEN '50-64-中风险'
                WHEN mp.{field} BETWEEN 65 AND 79 THEN '65-79-中高风险'
                WHEN mp.{field} BETWEEN 80 AND 100 THEN '80-100-高风险'
                ELSE 'NULL'
            END AS {field}_bucket
            """
        
        risk_bucket_selects = ',\n            '.join([risk_bucket_case(f) for f in RISK_FIELDS])
        
        adv_filter = ','.join(str(x) for x in adv_ids)
        
        # 查询曝光数据
        print("\n查询曝光数据...")
        cursor.execute(f"""
        SELECT 
            i.adv_id,
            COUNT(*) as imp_cnt,
            {risk_bucket_selects}
        FROM dsp.ods_dsp_imp i
        LEFT JOIN dsp_TQ.dsp_tp.media_profile_final mp
            ON CONCAT(COALESCE(i.first_ssp,''), CHAR(2), CAST(i.affiliate_id AS VARCHAR), CHAR(2), COALESCE(i.bundle,'')) = mp.lookupkey
        WHERE i.dt = '{dt}' AND i.adv_id IN ({adv_filter})
        GROUP BY i.adv_id, {', '.join([f'{f}_bucket' for f in RISK_FIELDS])}
        """)
        imp_rows = cursor.fetchall()
        print(f"  曝光记录: {len(imp_rows)} 条")
        
        # 查询点击数据
        print("查询点击数据...")
        cursor.execute(f"""
        SELECT 
            c.adv_id,
            COUNT(*) as click_cnt,
            {risk_bucket_selects}
        FROM dsp.ods_dsp_click c
        LEFT JOIN dsp_TQ.dsp_tp.media_profile_final mp
            ON CONCAT(COALESCE(c.first_ssp,''), CHAR(2), CAST(c.affiliate_id AS VARCHAR), CHAR(2), COALESCE(c.bundle,'')) = mp.lookupkey
        WHERE c.dt = '{dt}' AND c.adv_id IN ({adv_filter})
        GROUP BY c.adv_id, {', '.join([f'{f}_bucket' for f in RISK_FIELDS])}
        """)
        click_rows = cursor.fetchall()
        print(f"  点击记录: {len(click_rows)} 条")

    conn.close()

    # 查询转化数据 - 使用独立连接
    print("查询转化数据...")
    conn2 = pymysql.connect(
        host=SR_HOST, port=SR_PORT, user=SR_USER, password=SR_PASSWORD,
        charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor
    )

    with conn2.cursor() as cursor:
        cursor.execute(f"""
        SELECT
            d.adv_id,
            SUM(d.conversion) as conv_cnt,
            {risk_bucket_selects}
        FROM cdm.dwm_cross_placement_audience_detail_view d
        LEFT JOIN dsp_TQ.dsp_tp.media_profile_final mp
            ON CONCAT(COALESCE(d.first_ssp,''), CHAR(2), CAST(d.affiliate_id AS VARCHAR), CHAR(2), COALESCE(d.bundle_id,'')) = mp.lookupkey
        WHERE d.report_date = '{dt}' AND d.adv_id IN ({adv_filter}) AND d.conversion > 0
        GROUP BY d.adv_id, {', '.join([f'{f}_bucket' for f in RISK_FIELDS])}
        """)
        conv_rows = cursor.fetchall()
        print(f"  转化记录: {len(conv_rows)} 条")

    conn2.close()
    
    # 汇总数据
    from collections import defaultdict
    
    def aggregate_data(rows, metric_key):
        # 总体汇总
        overall = defaultdict(lambda: defaultdict(int))
        # 分广告主汇总
        by_adv = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
        
        for row in rows:
            adv_id = int(row['adv_id'])
            cnt = int(row[metric_key]) if row[metric_key] else 0
            
            for field in RISK_FIELDS:
                bucket = row.get(f'{field}_bucket', 'NULL')
                overall[field][bucket] += cnt
                by_adv[adv_id][field][bucket] += cnt
        
        return overall, by_adv
    
    imp_overall, imp_by_adv = aggregate_data(imp_rows, 'imp_cnt')
    click_overall, click_by_adv = aggregate_data(click_rows, 'click_cnt')
    conv_overall, conv_by_adv = aggregate_data(conv_rows, 'conv_cnt')
    
    # 生成报告
    lines = []
    lines.append(f"# 广告主风险画像分布分析报告\n")
    lines.append(f"**分析日期**: {dt}\n")
    lines.append(f"**广告主**: {', '.join(str(x) for x in adv_ids)}\n")
    lines.append("---\n")
    
    # 计算总量
    total_imp = sum(sum(v.values()) for v in imp_overall.values()) // len(RISK_FIELDS) if imp_overall else 0
    total_click = sum(sum(v.values()) for v in click_overall.values()) // len(RISK_FIELDS) if click_overall else 0
    total_conv = sum(sum(v.values()) for v in conv_overall.values()) // len(RISK_FIELDS) if conv_overall else 0
    
    lines.append("## 整体汇总\n")
    lines.append(f"- **总曝光**: {fmt_num(total_imp)}")
    lines.append(f"- **总点击**: {fmt_num(total_click)}")
    lines.append(f"- **总转化**: {fmt_num(total_conv)}")
    lines.append(f"- **CTR**: {fmt_pct(total_click, total_imp)}")
    lines.append(f"- **CVR**: {fmt_pct(total_conv, total_click)}\n")
    
    # 各风险维度分布
    buckets = ['0-未知', '1-20-低风险', '21-49-中低风险', '50-64-中风险', '65-79-中高风险', '80-100-高风险', 'NULL']

    for metric_name, metric_data, total in [
        ('曝光', imp_overall, total_imp),
        ('点击', click_overall, total_click),
        ('转化', conv_overall, total_conv)
    ]:
        lines.append(f"## {metric_name}在各风险维度的分布\n")
        lines.append("| 风险维度 | 0-未知 | 1-20-低风险 | 21-49-中低风险 | 50-64-中风险 | 65-79-中高风险 | 80-100-高风险 | NULL |")
        lines.append("|---------|--------|-----------|--------------|------------|--------------|--------------|------|")
        
        for field in RISK_FIELDS:
            name = RISK_NAMES.get(field, field)
            dist = metric_data.get(field, {})
            row_data = []
            for bucket in buckets:
                cnt = dist.get(bucket, 0)
                pct = fmt_pct(cnt, total) if total else "0.00%"
                row_data.append(f"{fmt_num(cnt)} ({pct})")
            lines.append(f"| {name} | {' | '.join(row_data)} |")
        lines.append("")
    
    # 分广告主分析
    lines.append("## 分广告主分析\n")
    
    for adv_id in sorted(adv_ids):
        lines.append(f"### 广告主 {adv_id}\n")
        
        adv_imp = sum(sum(v.values()) for v in imp_by_adv.get(adv_id, {}).values()) // len(RISK_FIELDS) if imp_by_adv.get(adv_id) else 0
        adv_click = sum(sum(v.values()) for v in click_by_adv.get(adv_id, {}).values()) // len(RISK_FIELDS) if click_by_adv.get(adv_id) else 0
        adv_conv = sum(sum(v.values()) for v in conv_by_adv.get(adv_id, {}).values()) // len(RISK_FIELDS) if conv_by_adv.get(adv_id) else 0
        
        lines.append(f"- **曝光**: {fmt_num(adv_imp)}")
        lines.append(f"- **点击**: {fmt_num(adv_click)}")
        lines.append(f"- **转化**: {fmt_num(adv_conv)}")
        lines.append(f"- **CTR**: {fmt_pct(adv_click, adv_imp)}")
        lines.append(f"- **CVR**: {fmt_pct(adv_conv, adv_click)}\n")
        
        # 只展示 rl_final 分布
        lines.append("#### 最终风险等级分布\n")
        lines.append("| 指标 | 0-未知 | 1-20-低风险 | 21-49-中低风险 | 50-64-中风险 | 65-79-中高风险 | 80-100-高风险 | NULL |")
        lines.append("|------|--------|-----------|--------------|------------|--------------|--------------|------|")
        
        for metric_name, metric_by_adv, total in [
            ('曝光', imp_by_adv, adv_imp),
            ('点击', click_by_adv, adv_click),
            ('转化', conv_by_adv, adv_conv)
        ]:
            dist = metric_by_adv.get(adv_id, {}).get('rl_final', {})
            row_data = []
            for bucket in buckets:
                cnt = dist.get(bucket, 0)
                pct = fmt_pct(cnt, total) if total else "0.00%"
                row_data.append(f"{fmt_num(cnt)} ({pct})")
            lines.append(f"| {metric_name} | {' | '.join(row_data)} |")
        lines.append("")
    
    # 保存报告
    report = "\n".join(lines)
    output_file = f"{OUTPUT_DIR}/adv_risk_profile_analysis_{dt}.md"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"\n✅ 报告已保存: {output_file}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        print(f"❌ 执行失败: {e}")
        traceback.print_exc()
        sys.exit(1)
