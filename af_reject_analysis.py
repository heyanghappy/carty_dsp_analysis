#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AF/Adjust 拒绝率分析（含媒体画像风险评估）
- af_reject_enabled=1：使用 AppsFlyer PA 数据
- af_reject_enabled=0：使用 Adjust（无 PA 数据，仅实时拒绝）
分析维度：
1. 广告主 (adv_id)
2. Affiliate (affiliate_id)
3. Affiliate + Bundle + Domain (affiliate_id + bundle_id + domain)
"""

import pandas as pd
import pymysql
import sys
from datetime import datetime, timedelta
from feishu_notify import send_to_feishu

SR_HOST = "fe-c-907795efe3201917.starrocks.aliyuncs.com"
SR_PORT = 9030
SR_USER = "sandy"
SR_PASSWORD = "MXeptLkEkoi2$FMX"

OUTPUT_DIR = "/home/node/.openclaw/workspace/repos/carty_dsp_analysis"
MEDIA_PROFILE_TABLE = "dsp_TQ.dsp_tp.media_profile_final"
ANTIFRAUD_CONFIG_TABLE = "dsp_TQ.dsp_tp.adv_antifraud_config"

# 游戏广告主列表 (IAA-Game) - 更新于 2026-04-09
GAME_ADV_IDS = [82, 92, 274, 301, 530, 624, 657, 671, 673, 756, 760, 761, 766, 768, 769, 770, 771, 779, 780, 781, 782, 787, 788, 789, 790, 791, 792, 795, 796, 797, 798, 799, 800, 801, 803, 804, 805, 808, 824, 826, 831, 832, 835, 845]


def load_mmp_config(cursor):
    """从配置表动态读取 AF / Adjust 广告主分组，只取 GAME_ADV_IDS 范围内的"""
    adv_filter = ','.join(str(x) for x in GAME_ADV_IDS)
    cursor.execute(f"""
        SELECT CAST(adv_id AS INT) AS adv_id, af_reject_enabled
        FROM {ANTIFRAUD_CONFIG_TABLE}
        WHERE adv_id IN ({adv_filter})
    """)
    af_adv_ids, adjust_adv_ids = [], []
    for row in cursor.fetchall():
        (af_adv_ids if row['af_reject_enabled'] else adjust_adv_ids).append(row['adv_id'])
    # 配置表里没有的 adv_id 默认归入 adjust（保守处理）
    configured = set(af_adv_ids + adjust_adv_ids)
    for adv_id in GAME_ADV_IDS:
        if adv_id not in configured:
            adjust_adv_ids.append(adv_id)
    return set(af_adv_ids), set(adjust_adv_ids)

ADV_INFO = {
    624: 'Capcut-CPI', 657: 'Tidy Master-CPI', 671: 'Bubble Shooter-CPI',
    673: 'Club Vegas-CPI', 756: 'Satistory', 760: 'Find it all-CPI',
    761: 'Magic Tiles-CPI', 766: 'Fizzo', 768: 'Balls Bounce-CPI',
    769: 'Bus Escape', 770: 'Monsters Gang', 771: 'Mechange',
    780: 'Magic Jigsaw', 781: 'Domino', 787: 'Mahjong Epic',
    789: 'Bricks Legend', 790: 'Vizor Gold', 792: 'Train Miner',
}

# 高拒绝率阈值
HIGH_REJECT_THRESHOLD = 0.10
BUNDLE_DETAIL_LIMIT = 50

RISK_FIELDS = ['rl_final', 'rl_bundle', 'rl_bundle_downloads', 'rl_game_af', 'rl_game_adjust',
               'rl_imp_fraud', 'rl_click_fraud', 'rl_soigame', 'rl_anura',
               'rl_visible_imp', 'rl_unintent_click', 'rl_N_ctr']

SEP = '\x02'

def fmt_num(n):
    if n is None:
        return "0"
    return f"{int(n):,}"

def fmt_pct(n):
    if n is None:
        return "0.00%"
    return f"{float(n)*100:.2f}%"

def risk_label(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return 'N/A'
    v = int(v)
    if v == 0:   return '✅ 无风险'
    if v <= 20:  return '🟡 低风险'
    if v <= 60:  return '🟠 中风险'
    return '🔴 高风险'

def fv(val):
    """格式化风险字段值"""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return 'N/A'
    return str(int(val))

def get_oss_bucket():
    return None

RL_COLS = ['风险', 'bundle', 'bndl_dl', 'af_rej', 'af_pa', 'imp_f', 'clk_f', 'soigame', 'anura', 'vis_imp', 'unint_clk', 'n_ctr']

def md_table(headers, rows):
    """生成标准 Markdown 表格，headers 为列名列表，rows 为每行值的列表"""
    lines = []
    lines.append('| ' + ' | '.join(str(h) for h in headers) + ' |')
    lines.append('|' + '|'.join(['------'] * len(headers)) + '|')
    for row in rows:
        lines.append('| ' + ' | '.join(str(v) for v in row) + ' |')
    return lines

def rl_final_fmt(v):
    """将 rl_final 值格式化为带风险等级 emoji 的字符串，如 100🔴"""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return 'N/A'
    vi = int(v)
    if vi == 0:   return f'{vi}✅'
    if vi <= 20:  return f'{vi}🟡'
    if vi <= 60:  return f'{vi}🟠'
    return f'{vi}🔴'

def rl_vals(row):
    """返回一行中 rl_* 字段的值列表（对应 RL_COLS）"""
    rl = row.get('rl_final')
    return [
        rl_final_fmt(rl),
        fv(row.get('rl_bundle')),
        fv(row.get('rl_bundle_downloads')),
        fv(row.get('rl_game_af')),
        fv(row.get('rl_game_adjust')),
        fv(row.get('rl_imp_fraud')),
        fv(row.get('rl_click_fraud')),
        fv(row.get('rl_soigame')),
        fv(row.get('rl_anura')),
        fv(row.get('rl_visible_imp')),
        fv(row.get('rl_unintent_click')),
        fv(row.get('rl_N_ctr')),
    ]

def query_single_day(cursor, date, adv_filter, rl_fields, af_adv_ids):
    """查询单天数据，返回结构化 dict
    af_adv_ids: set，af_reject_enabled=1 的广告主，PA 数据只关联这些广告主
    """
    date_obj = datetime.strptime(date, '%Y%m%d')
    pa_start_dt = (date_obj - timedelta(days=8)).strftime('%Y%m%d')
    pa_end_dt = date
    cheat_date = date_obj.strftime('%Y-%m-%d')

    # PA 数据只对 AF 广告主有意义
    af_filter = ','.join(str(x) for x in af_adv_ids) if af_adv_ids else '0'

    base_sql = f"""
    WITH
    click_data_with_rank AS (
        SELECT bid_id, adv_id, tag_id, affiliate_id, bundle_id, domain, first_ssp, country,
               dt, hh, time_millis,
               ROW_NUMBER() OVER (PARTITION BY bid_id ORDER BY dt, hh, time_millis) AS rn
        FROM dsp.ods_dsp_click
        WHERE dt = '{date}' AND adv_id IN ({adv_filter})
    ),
    click_data_distinct AS (
        SELECT bid_id, adv_id, tag_id, affiliate_id, bundle_id, domain, first_ssp, country
        FROM click_data_with_rank WHERE rn = 1
    ),
    postback_data AS (
        SELECT DISTINCT pb.bid_id
        FROM dsp.ods_dsp_postback pb
        INNER JOIN click_data_distinct ck ON pb.bid_id = ck.bid_id
        WHERE pb.dt = '{date}'
    ),
    cheat_data AS (
        SELECT DISTINCT ch.bid_id
        FROM dsp.ods_dsp_postback_cheat ch
        INNER JOIN click_data_distinct ck ON ch.bid_id = ck.bid_id
        WHERE ch.dt = '{cheat_date}'
    ),
    pa_data AS (
        -- 只关联 af_reject_enabled=1 的广告主的 click，Adjust 广告主无 PA 数据
        SELECT DISTINCT pa.bid_id
        FROM dsp_TQ.dsp_tp.mmp_appsflyer_report_prod pa
        INNER JOIN click_data_distinct ck ON pa.bid_id = ck.bid_id
        WHERE pa.dt BETWEEN '{pa_start_dt}' AND '{pa_end_dt}'
            AND pa.fraud_reason IS NOT NULL AND pa.fraud_reason != ''
            AND ck.adv_id IN ({af_filter})
    ),
    all_data AS (
        SELECT ck.bid_id, ck.adv_id, ck.affiliate_id, ck.bundle_id, ck.domain, ck.first_ssp,
               CASE WHEN pb.bid_id IS NOT NULL THEN 1 ELSE 0 END AS has_postback,
               CASE WHEN pa.bid_id IS NOT NULL THEN 1 ELSE 0 END AS has_pa,
               CASE WHEN ch.bid_id IS NOT NULL THEN 1 ELSE 0 END AS is_rejected
        FROM click_data_distinct ck
        LEFT JOIN postback_data pb ON ck.bid_id = pb.bid_id
        LEFT JOIN pa_data pa ON ck.bid_id = pa.bid_id
        LEFT JOIN cheat_data ch ON ck.bid_id = ch.bid_id
    )
    """

    result = {'date': date, 'pa_start_dt': pa_start_dt, 'pa_end_dt': pa_end_dt}

    # 整体汇总
    cursor.execute(base_sql + """
    SELECT SUM(has_postback) AS approval_convert,
           SUM(is_rejected)  AS reject_convert,
           SUM(has_pa)       AS pa_convert,
           SUM(has_postback)+SUM(is_rejected) AS total_count,
           ROUND(CASE WHEN SUM(has_postback)+SUM(is_rejected)=0 THEN 0
                      ELSE (SUM(is_rejected)+SUM(has_pa))*1.0/(SUM(has_postback)+SUM(is_rejected))
                 END, 4) AS reject_ratio
    FROM all_data
    """)
    result['summary'] = cursor.fetchone()

    # 广告主维度
    cursor.execute(base_sql + """
    SELECT adv_id,
           SUM(has_postback) AS approval_convert,
           SUM(is_rejected)  AS reject_convert,
           SUM(has_pa)       AS pa_convert,
           SUM(has_postback) + SUM(is_rejected) AS total_count,
           ROUND(CASE WHEN SUM(has_postback)+SUM(is_rejected)=0 THEN 0
                      ELSE (SUM(is_rejected)+SUM(has_pa))*1.0/(SUM(has_postback)+SUM(is_rejected))
                 END, 4) AS reject_ratio
    FROM all_data
    GROUP BY adv_id
    HAVING SUM(has_postback)+SUM(is_rejected) > 0
    ORDER BY reject_ratio DESC, reject_convert DESC, approval_convert DESC
    """)
    result['adv_rows'] = cursor.fetchall()

    # 有拒绝数据广告主的 Bundle+Domain 明细
    reject_advs = [r for r in result['adv_rows'] if r['reject_convert'] > 0 or r['pa_convert'] > 0]
    result['reject_advs'] = reject_advs
    result['bundle_by_adv'] = {}
    if reject_advs:
        reject_adv_filter = ','.join([str(r['adv_id']) for r in reject_advs])
        cursor.execute(base_sql + f"""
        , bundle_agg AS (
            SELECT adv_id, affiliate_id, bundle_id, domain, first_ssp,
                   SUM(has_postback) AS approval_convert,
                   SUM(is_rejected)  AS reject_convert,
                   SUM(has_pa)       AS pa_convert,
                   SUM(has_postback)+SUM(is_rejected) AS total_count,
                   ROUND(CASE WHEN SUM(has_postback)+SUM(is_rejected)=0 THEN 0
                              ELSE (SUM(is_rejected)+SUM(has_pa))*1.0/(SUM(has_postback)+SUM(is_rejected))
                         END, 4) AS reject_ratio
            FROM all_data
            WHERE adv_id IN ({reject_adv_filter})
            GROUP BY adv_id, affiliate_id, bundle_id, domain, first_ssp
            HAVING SUM(has_postback)+SUM(is_rejected) > 0
        )
        SELECT b.*, {rl_fields}
        FROM bundle_agg b
        LEFT JOIN {MEDIA_PROFILE_TABLE} mp_bundle
            ON CONCAT(b.first_ssp, CHAR(2), CAST(b.affiliate_id AS VARCHAR), CHAR(2), b.bundle_id) = mp_bundle.lookupkey
        LEFT JOIN {MEDIA_PROFILE_TABLE} mp_domain
            ON CONCAT(b.first_ssp, CHAR(2), CAST(b.affiliate_id AS VARCHAR), CHAR(2), b.domain) = mp_domain.lookupkey
        ORDER BY b.adv_id, b.reject_ratio DESC, b.reject_convert DESC, b.approval_convert DESC
        """)
        for row in cursor.fetchall():
            adv_id = row['adv_id']
            if adv_id not in result['bundle_by_adv']:
                result['bundle_by_adv'][adv_id] = []
            result['bundle_by_adv'][adv_id].append(row)

    # Affiliate 维度
    cursor.execute(base_sql + f"""
    , aff_agg AS (
        SELECT affiliate_id,
               ANY_VALUE(first_ssp) AS first_ssp,
               SUM(has_postback) AS approval_convert,
               SUM(is_rejected)  AS reject_convert,
               SUM(has_pa)       AS pa_convert,
               SUM(has_postback)+SUM(is_rejected) AS total_count,
               ROUND(CASE WHEN SUM(has_postback)+SUM(is_rejected)=0 THEN 0
                          ELSE (SUM(is_rejected)+SUM(has_pa))*1.0/(SUM(has_postback)+SUM(is_rejected))
                     END, 4) AS reject_ratio
        FROM all_data
        GROUP BY affiliate_id
        HAVING SUM(has_postback)+SUM(is_rejected) > 0
        ORDER BY reject_ratio DESC, reject_convert DESC, approval_convert DESC
        LIMIT 100
    )
    SELECT a.*, {rl_fields}
    FROM aff_agg a
    LEFT JOIN {MEDIA_PROFILE_TABLE} mp_bundle
        ON CONCAT(a.first_ssp, CHAR(2), CAST(a.affiliate_id AS VARCHAR), CHAR(2), '') = mp_bundle.lookupkey
    LEFT JOIN {MEDIA_PROFILE_TABLE} mp_domain ON 1=0
    """)
    result['aff_rows'] = cursor.fetchall()

    # Affiliate + Bundle + Domain 维度
    cursor.execute(base_sql + f"""
    , ab_agg AS (
        SELECT affiliate_id, bundle_id, domain, first_ssp,
               SUM(has_postback) AS approval_convert,
               SUM(is_rejected)  AS reject_convert,
               SUM(has_pa)       AS pa_convert,
               SUM(has_postback)+SUM(is_rejected) AS total_count,
               ROUND(CASE WHEN SUM(has_postback)+SUM(is_rejected)=0 THEN 0
                          ELSE (SUM(is_rejected)+SUM(has_pa))*1.0/(SUM(has_postback)+SUM(is_rejected))
                     END, 4) AS reject_ratio
        FROM all_data
        GROUP BY affiliate_id, bundle_id, domain, first_ssp
        HAVING SUM(has_postback)+SUM(is_rejected) > 0
        ORDER BY reject_ratio DESC, reject_convert DESC, approval_convert DESC
        LIMIT 100
    )
    SELECT a.*, {rl_fields}
    FROM ab_agg a
    LEFT JOIN {MEDIA_PROFILE_TABLE} mp_bundle
        ON CONCAT(a.first_ssp, CHAR(2), CAST(a.affiliate_id AS VARCHAR), CHAR(2), a.bundle_id) = mp_bundle.lookupkey
    LEFT JOIN {MEDIA_PROFILE_TABLE} mp_domain
        ON CONCAT(a.first_ssp, CHAR(2), CAST(a.affiliate_id AS VARCHAR), CHAR(2), a.domain) = mp_domain.lookupkey
    """)
    result['aff_bundle_rows'] = cursor.fetchall()

    return result


def build_single_day_report(day_data, lines, af_adv_ids):
    """将单天查询结果写入 lines（报告片段）"""
    date = day_data['date']
    summary = day_data['summary']
    adv_rows = day_data['adv_rows']
    reject_advs = day_data['reject_advs']
    bundle_by_adv = day_data['bundle_by_adv']
    aff_rows = day_data['aff_rows']
    aff_bundle_rows = day_data['aff_bundle_rows']

    def mmp_label(adv_id):
        return 'AF' if int(adv_id) in af_adv_ids else 'Adjust'

    # 一、整体汇总
    lines.append("## 一、整体汇总\n")
    if summary:
        lines.append("| 指标 | 数值 |")
        lines.append("|------|------|")
        lines.append(f"| 通过转化数 | {fmt_num(summary['approval_convert'])} |")
        lines.append(f"| 拒绝转化数 | {fmt_num(summary['reject_convert'])} |")
        lines.append(f"| PA转化数（仅AF） | {fmt_num(summary['pa_convert'])} |")
        lines.append(f"| 总转化数 | {fmt_num(summary['total_count'])} |")
        lines.append(f"| 整体拒绝率 | {fmt_pct(summary['reject_ratio'])} |")
        lines.append("")

    # 二、分广告主拒绝率
    lines.append("## 二、分广告主拒绝率\n")
    if adv_rows:
        headers = ['ADV ID', '广告主', 'MMP', '通过', '拒绝', 'PA', '总计', '拒绝率']
        rows_data = []
        for row in adv_rows:
            adv_id = int(row['adv_id'])
            name = ADV_INFO.get(adv_id, '-')
            rows_data.append([adv_id, name, mmp_label(adv_id),
                               fmt_num(row['approval_convert']), fmt_num(row['reject_convert']),
                               fmt_num(row['pa_convert']), fmt_num(row['total_count']), fmt_pct(row['reject_ratio'])])
        lines.extend(md_table(headers, rows_data))
        lines.append("")
    else:
        lines.append("无数据\n")

    # 三、有拒绝转化广告主汇总统计
    lines.append("## 三、有拒绝转化广告主汇总统计\n")
    lines.append("> 只统计有拒绝转化（实时拒绝或PA识别）的广告主\n")
    if reject_advs:
        total_approval = sum(r['approval_convert'] or 0 for r in reject_advs)
        total_reject = sum(r['reject_convert'] or 0 for r in reject_advs)
        total_pa = sum(r['pa_convert'] or 0 for r in reject_advs)
        total_count = sum(r['total_count'] or 0 for r in reject_advs)
        ratio = (total_reject + total_pa) / total_count if total_count > 0 else 0
        lines.append("### 3.1 汇总\n")
        lines.append("| 指标 | 数值 |")
        lines.append("|------|------|")
        lines.append(f"| 覆盖广告主数 | {len(reject_advs)} |")
        lines.append(f"| 通过转化数 | {fmt_num(total_approval)} |")
        lines.append(f"| 拒绝转化数 | {fmt_num(total_reject)} |")
        lines.append(f"| PA转化数（仅AF） | {fmt_num(total_pa)} |")
        lines.append(f"| 总转化数 | {fmt_num(total_count)} |")
        lines.append(f"| **拒绝率** | **{fmt_pct(ratio)}** |")
        lines.append("")
        lines.append("### 3.2 明细\n")
        lines.append(f"**共 {len(reject_advs)} 个广告主有拒绝转化**\n")
        headers = ['ADV ID', '广告主', 'MMP', '通过', '拒绝', 'PA', '总计', '拒绝率']
        rows_data = []
        for row in reject_advs:
            adv_id = int(row['adv_id'])
            rows_data.append([adv_id, ADV_INFO.get(adv_id, '-'), mmp_label(adv_id),
                               fmt_num(row['approval_convert']),
                               fmt_num(row['reject_convert']), fmt_num(row['pa_convert']),
                               fmt_num(row['total_count']), fmt_pct(row['reject_ratio'])])
        lines.extend(md_table(headers, rows_data))
        lines.append("")
    else:
        lines.append("无广告主有拒绝转化\n")

    # 四、Bundle+Domain 明细
    lines.append(f"## 四、有拒绝数据广告主的 Bundle+Domain 明细分析\n")
    lines.append(f"> **筛选条件**: 有拒绝转化（实时拒绝或PA识别）的广告主，展示该广告主**所有有转化**的 ssp+aff+bundle+domain 组合（含无拒绝的记录）\n")
    lines.append(f"> **展示条数**: 每个广告主 Top {BUNDLE_DETAIL_LIMIT}（按拒绝率排序）\n")
    lines.append("")
    if reject_advs and bundle_by_adv:
        lines.append(f"**共 {len(reject_advs)} 个广告主有拒绝数据**\n")
        lines.append("")
        for adv_row in reject_advs:
            adv_id = adv_row['adv_id']
            bundle_rows = bundle_by_adv.get(adv_id, [])[:BUNDLE_DETAIL_LIMIT]
            if bundle_rows:
                lines.append(f"### ADV {adv_id} {ADV_INFO.get(int(adv_id), '')} (整体拒绝率: {fmt_pct(adv_row['reject_ratio'])})\n")
                headers = ['Aff', 'Bundle', 'Domain', 'SSP', '通过', '拒绝', 'PA', '总计', '拒绝率'] + RL_COLS
                rows_data = []
                for row in bundle_rows:
                    rows_data.append([row['affiliate_id'], row['bundle_id'], row.get('domain') or '',
                                      row['first_ssp'], fmt_num(row['approval_convert']),
                                      fmt_num(row['reject_convert']), fmt_num(row['pa_convert']),
                                      fmt_num(row['total_count']), fmt_pct(row['reject_ratio'])] + rl_vals(row))
                lines.extend(md_table(headers, rows_data))
                lines.append("")
    else:
        lines.append("无广告主有拒绝数据\n")
        lines.append("")

    # 五、Affiliate 维度
    lines.append("## 五、按 Affiliate 分析\n")
    if aff_rows:
        lines.append("### Top 100 Affiliate（按拒绝率排序）\n")
        headers = ['Aff', '通过', '拒绝', 'PA', '总计', '拒绝率'] + RL_COLS
        rows_data = []
        for row in aff_rows:
            rows_data.append([row['affiliate_id'], fmt_num(row['approval_convert']),
                               fmt_num(row['reject_convert']), fmt_num(row['pa_convert']),
                               fmt_num(row['total_count']), fmt_pct(row['reject_ratio'])] + rl_vals(row))
        lines.extend(md_table(headers, rows_data))
        lines.append("")
    else:
        lines.append("无数据\n")

    # 六、Affiliate + Bundle + Domain 维度
    lines.append("## 六、按 Affiliate + Bundle + Domain 分析\n")
    if aff_bundle_rows:
        lines.append("### Top 100 Affiliate + Bundle + Domain 组合（按拒绝率排序）\n")
        headers = ['Aff', 'Bundle', 'Domain', 'SSP', '通过', '拒绝', 'PA', '总计', '拒绝率'] + RL_COLS
        rows_data = []
        for row in aff_bundle_rows:
            rows_data.append([row['affiliate_id'], row['bundle_id'], row.get('domain') or '',
                               row['first_ssp'], fmt_num(row['approval_convert']),
                               fmt_num(row['reject_convert']), fmt_num(row['pa_convert']),
                               fmt_num(row['total_count']), fmt_pct(row['reject_ratio'])] + rl_vals(row))
        lines.extend(md_table(headers, rows_data))
        lines.append("")
    else:
        lines.append("无数据\n")


def build_multi_day_summary(all_day_data, lines):
    """多天模式：先输出跨天汇总，再逐天明细"""
    lines.append("## 零、多日汇总概览\n")

    # 逐日拒绝率趋势表
    lines.append("### 逐日拒绝率趋势\n")
    headers = ['日期', '通过', '拒绝', 'PA', '总计', '拒绝率']
    rows_data = []
    for d in all_day_data:
        s = d['summary']
        if s:
            rows_data.append([d['date'], fmt_num(s['approval_convert']), fmt_num(s['reject_convert']),
                               fmt_num(s['pa_convert']), fmt_num(s['total_count']), fmt_pct(s['reject_ratio'])])
    lines.extend(md_table(headers, rows_data))
    lines.append("")

    # 跨天广告主汇总（合并所有天）
    adv_agg = {}
    for d in all_day_data:
        for row in d['adv_rows']:
            adv_id = int(row['adv_id'])
            if adv_id not in adv_agg:
                adv_agg[adv_id] = {'approval': 0, 'reject': 0, 'pa': 0, 'total': 0}
            adv_agg[adv_id]['approval'] += row['approval_convert'] or 0
            adv_agg[adv_id]['reject'] += row['reject_convert'] or 0
            adv_agg[adv_id]['pa'] += row['pa_convert'] or 0
            adv_agg[adv_id]['total'] += row['total_count'] or 0

    lines.append("### 广告主汇总（全周期）\n")
    headers = ['ADV ID', '广告主', '通过', '拒绝', 'PA', '总计', '拒绝率']
    rows_data = []
    for adv_id, v in sorted(adv_agg.items(), key=lambda x: -(x[1]['reject']+x[1]['pa'])):
        ratio = (v['reject'] + v['pa']) / v['total'] if v['total'] > 0 else 0
        rows_data.append([adv_id, ADV_INFO.get(adv_id, '-'), fmt_num(v['approval']),
                           fmt_num(v['reject']), fmt_num(v['pa']), fmt_num(v['total']), fmt_pct(ratio)])
    lines.extend(md_table(headers, rows_data))
    lines.append("")


def main():
    if len(sys.argv) > 1:
        dates = sys.argv[1:]
    else:
        dates = [(datetime.now() - timedelta(days=1)).strftime('%Y%m%d')]

    date_label = ', '.join(dates)
    is_multi = len(dates) > 1

    print(f"开始分析 AF/Adjust 拒绝率（含媒体画像风险）")
    print(f"分析日期: {date_label}")
    print(f"媒体画像数据源: {MEDIA_PROFILE_TABLE}")
    print()

    conn = pymysql.connect(
        host=SR_HOST, port=SR_PORT, user=SR_USER, password=SR_PASSWORD,
        charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor
    )

    adv_filter = ','.join([str(x) for x in GAME_ADV_IDS])
    rl_fields = ', '.join(f'COALESCE(mp_bundle.{f}, mp_domain.{f}) AS {f}' for f in RISK_FIELDS)

    all_day_data = []
    with conn.cursor() as cursor:
        cursor.execute("SET CATALOG assembly")
        af_adv_ids, adjust_adv_ids = load_mmp_config(cursor)
        print(f"AF 广告主({len(af_adv_ids)}): {sorted(af_adv_ids)}")
        print(f"Adjust 广告主({len(adjust_adv_ids)}): {sorted(adjust_adv_ids)}")
        for date in dates:
            print(f"\n--- 查询 {date} ---")
            day_data = query_single_day(cursor, date, adv_filter, rl_fields, af_adv_ids)
            all_day_data.append(day_data)
            s = day_data['summary']
            if s:
                print(f"  整体拒绝率: {fmt_pct(s['reject_ratio'])}  总转化: {fmt_num(s['total_count'])}")

    lines = []
    lines.append(f"# AF/Adjust 拒绝率分析报告（含媒体画像风险）\n")
    lines.append(f"**分析日期**: {date_label}\n")
    lines.append(f"**媒体画像数据**: {MEDIA_PROFILE_TABLE}\n")
    lines.append(f"**AF 广告主**: {sorted(af_adv_ids)}\n")
    lines.append(f"**Adjust 广告主**: {sorted(adjust_adv_ids)}\n")
    lines.append("---\n")

    if is_multi:
        build_multi_day_summary(all_day_data, lines)

    for day_data in all_day_data:
        if is_multi:
            lines.append(f"\n---\n\n# {day_data['date']} 明细\n")
            lines.append(f"> PA 数据范围（仅AF）: {day_data['pa_start_dt']} - {day_data['pa_end_dt']}\n")
        else:
            lines.append(f"**PA 数据范围（仅AF）**: {day_data['pa_start_dt']} - {day_data['pa_end_dt']}\n")
        build_single_day_report(day_data, lines, af_adv_ids)

    # 指标说明（只写一次）
    lines.append("## 七、指标说明\n")
    lines.append("| 指标 | 说明 |")
    lines.append("|------|------|")
    lines.append("| MMP | AF=AppsFlyer（有PA数据），Adjust（无PA数据，仅实时拒绝） |")
    lines.append("| 通过转化 | postback 中存在的转化（我们发送给MMP的） |")
    lines.append("| 拒绝转化 | cheat 表中标记的转化（我们实时拒绝的） |")
    lines.append("| PA转化 | AppsFlyer 报告中有 fraud_reason 的转化（AF 离线识别的作弊，仅AF广告主） |")
    lines.append("| 总转化 | 通过转化 + 拒绝转化 |")
    lines.append("| 拒绝率 | (拒绝转化 + PA转化) / 总转化 |")
    lines.append("| rl_final | 媒体综合风险评分 (0=无风险, 100=最高风险) |")
    lines.append("| rl_bundle | Bundle 维度风险评分 |")
    lines.append("| rl_bundle_downloads | Bundle 下载量风险评分 |")
    lines.append("| rl_game_af | AF 拒绝率风险评分 |")
    lines.append("| rl_game_adjust | AF PA 风险评分 |")
    lines.append("| rl_imp_fraud | 曝光欺诈风险评分 |")
    lines.append("| rl_click_fraud | 点击欺诈风险评分 |")
    lines.append("| rl_soigame | SOI Game 风险评分 |")
    lines.append("| rl_anura | Anura 风险评分 |")
    lines.append("| rl_visible_imp | 可见曝光风险评分 |")
    lines.append("| rl_unintent_click | 非意图点击风险评分 |")
    lines.append("| rl_N_ctr | N-CTR 风险评分 |")
    lines.append("")
    lines.append("> **风险等级**: ✅ 无风险(0) / 🟡 低风险(1-20) / 🟠 中风险(21-60) / 🔴 高风险(61-100)")
    lines.append("")

    conn.close()

    report = "\n".join(lines)
    date_suffix = '_'.join(dates)
    output_file = f"{OUTPUT_DIR}/af_reject_analysis_{date_suffix}.md"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(report)

    print(f"✅ 报告已保存: {output_file}")

    # 发送到飞书
    title = f"📊 AF/Adjust 拒绝率日报 {date_suffix}"
    if send_to_feishu(title, report):
        print("✅ 已发送到飞书")
    else:
        print("❌ 飞书发送失败")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        print(f"❌ 执行失败: {e}")
        traceback.print_exc()
        sys.exit(1)
