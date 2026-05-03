#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
游戏广告主媒体分析（aff+ssp+bundle）
分析维度：曝光、点击、转化、拒绝、PA转化、拒绝率、媒体风险等级
"""

import pandas as pd
import pymysql
import sys
from datetime import datetime, timedelta

SR_HOST = "fe-c-907795efe3201917.starrocks.aliyuncs.com"
SR_PORT = 9030
SR_USER = "sandy"
SR_PASSWORD = "MXeptLkEkoi2$FMX"

OUTPUT_DIR = "/home/node/.openclaw/workspace/repos/carty_dsp_analysis"
MEDIA_PROFILE_TABLE = "dsp_TQ.dsp_tp.media_profile_final"

GAME_ADV_IDS = [82, 92, 274, 301, 530, 624, 657, 671, 673, 756, 760, 761, 766, 768, 769,
                770, 771, 779, 780, 781, 782, 787, 788, 789, 790, 791, 792, 795, 796, 797,
                798, 799, 800, 801, 803, 804, 805, 808, 824, 826, 831, 832, 835, 845]

ADV_INFO = {
    624: 'Capcut-CPI', 657: 'Tidy Master-CPI', 671: 'Bubble Shooter-CPI',
    673: 'Club Vegas-CPI', 756: 'Satistory: Tidy up', 760: 'Find it all-CPI',
    761: 'Magic Tiles-CPI', 766: 'Fizzo', 768: 'Balls Bounce-CPI',
    769: 'Bus Escape', 770: 'Monsters Gang', 771: 'Mechange',
    780: 'Magic Jigsaw Puzzles', 781: 'Domino', 787: 'Mahjong Epic',
    789: 'Bricks Legend', 790: 'Vizor Gold Miners', 792: 'Train Miner',
}

RISK_FIELDS = ['rl_final', 'rl_bundle', 'rl_game_af', 'rl_game_adjust',
               'rl_imp_fraud', 'rl_click_fraud', 'rl_anura']

def risk_label(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return 'N/A'
    v = int(v)
    if v == 0:   return '✅ 无风险'
    if v <= 20:  return '🟡 低风险'
    if v <= 60:  return '🟠 中风险'
    return '🔴 高风险'

def query_main_data(dt):
    date_obj = datetime.strptime(dt, '%Y%m%d')
    pa_start = (date_obj - timedelta(days=7)).strftime('%Y%m%d')
    pa_end   = (date_obj + timedelta(days=14)).strftime('%Y%m%d')
    cheat_dt = date_obj.strftime('%Y-%m-%d')
    adv_filter = ','.join(str(x) for x in GAME_ADV_IDS)

    conn = pymysql.connect(
        host=SR_HOST, port=SR_PORT, user=SR_USER, password=SR_PASSWORD,
        charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor
    )

    SEP = '\x02'
    rl_fields = ', '.join(f'mp.{f}' for f in RISK_FIELDS)

    sql = f"""
    WITH
    imp_data AS (
        SELECT affiliate_id, bundle_id, first_ssp, adv_id,
               COUNT(DISTINCT bid_id) AS imp_cnt
        FROM assembly.dsp.ods_dsp_imp
        WHERE dt = '{dt}' AND adv_id IN ({adv_filter})
        GROUP BY affiliate_id, bundle_id, first_ssp, adv_id
    ),
    click_data AS (
        SELECT affiliate_id, bundle_id, first_ssp, adv_id,
               bid_id,
               ROW_NUMBER() OVER (PARTITION BY bid_id ORDER BY dt, hh, time_millis) AS rn
        FROM assembly.dsp.ods_dsp_click
        WHERE dt = '{dt}' AND adv_id IN ({adv_filter})
    ),
    click_dedup AS (
        SELECT affiliate_id, bundle_id, first_ssp, adv_id, bid_id
        FROM click_data WHERE rn = 1
    ),
    click_agg AS (
        SELECT affiliate_id, bundle_id, first_ssp, adv_id,
               COUNT(DISTINCT bid_id) AS click_cnt
        FROM click_dedup
        GROUP BY affiliate_id, bundle_id, first_ssp, adv_id
    ),
    postback_data AS (
        SELECT DISTINCT pb.bid_id
        FROM assembly.dsp.ods_dsp_postback pb
        INNER JOIN assembly.dsp.ods_dsp_click ck
            ON pb.bid_id = ck.bid_id AND ck.dt = '{dt}' AND ck.adv_id IN ({adv_filter})
        WHERE pb.dt = '{dt}'
    ),
    cheat_data AS (
        SELECT DISTINCT ch.bid_id
        FROM assembly.dsp.ods_dsp_postback_cheat ch
        INNER JOIN assembly.dsp.ods_dsp_click ck
            ON ch.bid_id = ck.bid_id AND ck.dt = '{dt}' AND ck.adv_id IN ({adv_filter})
        WHERE ch.dt = '{cheat_dt}'
    ),
    pa_data AS (
        SELECT DISTINCT bid_id
        FROM dsp_TQ.dsp_tp.mmp_appsflyer_report_prod
        WHERE dt BETWEEN '{pa_start}' AND '{pa_end}'
          AND fraud_reason IS NOT NULL AND fraud_reason != ''
    ),
    conv_data AS (
        SELECT ck.adv_id, ck.affiliate_id, ck.bundle_id, ck.first_ssp,
               CASE WHEN pb.bid_id IS NOT NULL THEN 1 ELSE 0 END AS has_postback,
               CASE WHEN pa.bid_id IS NOT NULL THEN 1 ELSE 0 END AS has_pa,
               CASE WHEN ch.bid_id IS NOT NULL THEN 1 ELSE 0 END AS is_rejected
        FROM click_dedup ck
        LEFT JOIN postback_data pb ON ck.bid_id = pb.bid_id
        LEFT JOIN pa_data pa ON ck.bid_id = pa.bid_id
        LEFT JOIN cheat_data ch ON ck.bid_id = ch.bid_id
    ),
    conv_agg AS (
        SELECT adv_id, affiliate_id, bundle_id, first_ssp,
               SUM(has_postback)  AS pass_cnt,
               SUM(is_rejected)   AS reject_cnt,
               SUM(has_pa)        AS pa_cnt,
               SUM(has_postback) + SUM(is_rejected) AS total_conv
        FROM conv_data
        GROUP BY adv_id, affiliate_id, bundle_id, first_ssp
    ),
    dim_base AS (
        SELECT COALESCE(i.adv_id, c.adv_id, cv.adv_id) AS adv_id,
               COALESCE(i.affiliate_id, c.affiliate_id, cv.affiliate_id) AS affiliate_id,
               COALESCE(i.bundle_id, c.bundle_id, cv.bundle_id) AS bundle_id,
               COALESCE(i.first_ssp, c.first_ssp, cv.first_ssp) AS first_ssp,
               COALESCE(i.imp_cnt, 0) AS imp_cnt,
               COALESCE(c.click_cnt, 0) AS click_cnt,
               COALESCE(cv.pass_cnt, 0) AS pass_cnt,
               COALESCE(cv.reject_cnt, 0) AS reject_cnt,
               COALESCE(cv.pa_cnt, 0) AS pa_cnt,
               COALESCE(cv.total_conv, 0) AS total_conv
        FROM imp_data i
        FULL OUTER JOIN click_agg c
            ON i.adv_id = c.adv_id AND i.affiliate_id = c.affiliate_id
            AND i.bundle_id = c.bundle_id AND i.first_ssp = c.first_ssp
        FULL OUTER JOIN conv_agg cv
            ON COALESCE(i.adv_id, c.adv_id) = cv.adv_id
            AND COALESCE(i.affiliate_id, c.affiliate_id) = cv.affiliate_id
            AND COALESCE(i.bundle_id, c.bundle_id) = cv.bundle_id
            AND COALESCE(i.first_ssp, c.first_ssp) = cv.first_ssp
    )
    SELECT d.adv_id, d.affiliate_id, d.bundle_id, d.first_ssp,
           d.imp_cnt, d.click_cnt, d.pass_cnt, d.reject_cnt, d.pa_cnt, d.total_conv,
           ROUND(
               CASE WHEN d.total_conv = 0 THEN 0
                    ELSE (d.reject_cnt + d.pa_cnt) * 1.0 / d.total_conv
               END, 4
           ) AS reject_ratio,
           {rl_fields}
    FROM dim_base d
    LEFT JOIN {MEDIA_PROFILE_TABLE} mp
        ON CONCAT(d.first_ssp, CHAR(2), CAST(d.affiliate_id AS VARCHAR), CHAR(2), d.bundle_id) = mp.lookupkey
    WHERE d.adv_id IS NOT NULL
    ORDER BY d.adv_id, reject_ratio DESC, d.reject_cnt DESC
    """

    print('查询 StarRocks 数据（曝光+点击+转化+拒绝+媒体画像）...')
    with conn.cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()
    conn.close()
    print(f'  共 {len(rows)} 条 adv+aff+bundle+ssp 记录')

    # 统计匹配情况
    matched = sum(1 for r in rows if r.get('rl_final') is not None)
    print(f'  匹配到媒体画像: {matched}/{len(rows)} 条 ({matched/len(rows)*100:.1f}%)')

    return rows, pa_start, pa_end

def main():
    dt = sys.argv[1] if len(sys.argv) > 1 else '20260408'
    print(f'分析日期: {dt}')

    # 1. 查询主数据（含媒体画像）
    rows, pa_start, pa_end = query_main_data(dt)
    if not rows:
        print('❌ 无数据')
        sys.exit(1)

    df = pd.DataFrame(rows)
    for col in ['adv_id']:
        df[col] = df[col].astype(int)
    for col in ['affiliate_id', 'bundle_id', 'first_ssp']:
        df[col] = df[col].astype(str)

    # 2. 生成报告
    lines = []
    lines.append(f'# 游戏广告主媒体风险分析报告（aff+ssp+bundle）\n')
    lines.append(f'**分析日期**: {dt}')
    lines.append(f'**PA数据范围**: {pa_start} ~ {pa_end}')
    lines.append(f'**广告主数量**: {df["adv_id"].nunique()}')
    lines.append(f'**媒体组合数**: {len(df):,}\n')
    lines.append('---\n')

    # 4.1 整体汇总
    lines.append('## 一、整体汇总\n')
    total_imp = df['imp_cnt'].sum()
    total_clk = df['click_cnt'].sum()
    total_pass = df['pass_cnt'].sum()
    total_rej = df['reject_cnt'].sum()
    total_pa = df['pa_cnt'].sum()
    total_conv = df['total_conv'].sum()
    total_reject_ratio = (total_rej + total_pa) / total_conv if total_conv > 0 else 0

    lines.append('| 指标 | 数值 |')
    lines.append('|------|------|')
    lines.append(f'| 总曝光 | {total_imp:,} |')
    lines.append(f'| 总点击 | {total_clk:,} |')
    lines.append(f'| 通过转化 | {total_pass:,} |')
    lines.append(f'| 拒绝转化 | {total_rej:,} |')
    lines.append(f'| PA转化 | {total_pa:,} |')
    lines.append(f'| 总转化 | {total_conv:,} |')
    lines.append(f'| 整体拒绝率 | {total_reject_ratio*100:.2f}% |')
    lines.append('')

    # 4.2 分广告主汇总
    lines.append('## 二、分广告主汇总\n')
    adv_agg = df.groupby('adv_id').agg(
        imp_cnt=('imp_cnt', 'sum'),
        click_cnt=('click_cnt', 'sum'),
        pass_cnt=('pass_cnt', 'sum'),
        reject_cnt=('reject_cnt', 'sum'),
        pa_cnt=('pa_cnt', 'sum'),
        total_conv=('total_conv', 'sum'),
        media_cnt=('bundle_id', 'count'),
    ).reset_index()
    adv_agg['reject_ratio'] = (adv_agg['reject_cnt'] + adv_agg['pa_cnt']) / adv_agg['total_conv'].replace(0, 1)
    adv_agg = adv_agg.sort_values('reject_ratio', ascending=False)

    lines.append('| ADV ID | 广告主 | 媒体数 | 曝光 | 点击 | 通过转化 | 拒绝转化 | PA转化 | 总转化 | 拒绝率 |')
    lines.append('|--------|--------|--------|------|------|----------|----------|--------|--------|--------|')
    for _, r in adv_agg.iterrows():
        adv_id = int(r['adv_id'])
        name = ADV_INFO.get(adv_id, str(adv_id))
        ratio = r['reject_ratio'] if r['total_conv'] > 0 else 0
        lines.append(
            f"| {adv_id} | {name} | {int(r['media_cnt'])} | {int(r['imp_cnt']):,} | {int(r['click_cnt']):,} | "
            f"{int(r['pass_cnt']):,} | {int(r['reject_cnt']):,} | {int(r['pa_cnt']):,} | "
            f"{int(r['total_conv']):,} | {ratio*100:.2f}% |"
        )
    lines.append('')

    # 4.3 分广告主媒体明细
    lines.append('## 三、分广告主媒体明细\n')
    lines.append('> 每个广告主显示 Top 50 媒体（按点击量排序），仅展示有转化或有风险的媒体\n')

    for adv_id in sorted(df['adv_id'].unique()):
        adv_df = df[df['adv_id'] == adv_id].copy()
        name = ADV_INFO.get(int(adv_id), str(adv_id))

        # 只展示有转化或有风险的
        show_df = adv_df[(adv_df['total_conv'] > 0) | (adv_df['rl_final'].fillna(0) >= 70)]
        if show_df.empty:
            continue

        show_df = show_df.sort_values('click_cnt', ascending=False).head(50)

        lines.append(f'### ADV {adv_id} - {name}\n')
        lines.append('| Aff ID | Bundle | SSP | 曝光 | 点击 | 通过 | 拒绝 | PA | 总转化 | 拒绝率 | rl_final | 风险等级 | rl_bundle | rl_game_af | rl_game_adjust | rl_imp_fraud | rl_click_fraud | rl_anura |')
        lines.append('|--------|--------|-----|------|------|------|------|-----|--------|--------|----------|----------|-----------|--------------|----------|--------------|----------------|----------|')

        for _, r in show_df.iterrows():
            rl = r.get('rl_final')
            rl_str = str(int(rl)) if pd.notna(rl) else 'N/A'
            ratio = r['reject_ratio'] if r['total_conv'] > 0 else 0
            def fv(f): v = r.get(f); return str(int(v)) if pd.notna(v) else 'N/A'
            lines.append(
                f"| {r['affiliate_id']} | {r['bundle_id']} | {r['first_ssp']} | "
                f"{int(r['imp_cnt']):,} | {int(r['click_cnt']):,} | "
                f"{int(r['pass_cnt'])} | {int(r['reject_cnt'])} | {int(r['pa_cnt'])} | "
                f"{int(r['total_conv'])} | {ratio*100:.2f}% | {rl_str} | {risk_label(rl)} | "
                f"{fv('rl_bundle')} | {fv('rl_game_af')} | {fv('rl_game_adjust')} | "
                f"{fv('rl_imp_fraud')} | {fv('rl_click_fraud')} | {fv('rl_anura')} |"
            )
        lines.append('')

    # 4.4 高风险媒体汇总（rl_final >= 70）
    lines.append('## 四、高风险媒体（rl_final ≥ 70）\n')
    df_high = df[df['rl_final'].fillna(0) >= 70].sort_values('rl_final', ascending=False)
    if df_high.empty:
        lines.append('> 无 rl_final ≥ 70 的高风险媒体\n')
    else:
        lines.append(f'> 共 {len(df_high)} 条高风险媒体\n')
        lines.append('| ADV | Aff ID | Bundle | SSP | 曝光 | 点击 | 拒绝率 | rl_final | 风险等级 | rl_bundle | rl_game_af | rl_game_adjust | rl_imp_fraud | rl_click_fraud | rl_anura |')
        lines.append('|-----|--------|--------|-----|------|------|--------|----------|----------|-----------|--------------|----------|--------------|----------------|----------|')
        for _, r in df_high.iterrows():
            def fv(f):
                v = r.get(f)
                return str(int(v)) if pd.notna(v) else 'N/A'
            rl = r.get('rl_final')
            ratio = r['reject_ratio'] if r['total_conv'] > 0 else 0
            lines.append(
                f"| {int(r['adv_id'])} | {r['affiliate_id']} | {r['bundle_id']} | {r['first_ssp']} | "
                f"{int(r['imp_cnt']):,} | {int(r['click_cnt']):,} | {ratio*100:.2f}% | "
                f"{fv('rl_final')} | {risk_label(rl)} | {fv('rl_bundle')} | {fv('rl_game_af')} | "
                f"{fv('rl_game_adjust')} | {fv('rl_imp_fraud')} | {fv('rl_click_fraud')} | {fv('rl_anura')} |"
            )
    lines.append('')

    # 4.5 高拒绝率媒体（拒绝率>=30%，有转化）
    lines.append('## 五、高拒绝率媒体（拒绝率 ≥ 30%，有转化）\n')
    df_high_rej = df[(df['reject_ratio'] >= 0.30) & (df['total_conv'] > 0)].sort_values(
        ['reject_ratio', 'reject_cnt'], ascending=[False, False])
    if df_high_rej.empty:
        lines.append('> 无拒绝率 ≥ 30% 的媒体\n')
    else:
        lines.append(f'> 共 {len(df_high_rej)} 条高拒绝率媒体\n')
        lines.append('| ADV | Aff ID | Bundle | SSP | 通过 | 拒绝 | PA | 总转化 | 拒绝率 | rl_final | 风险等级 | rl_bundle | rl_game_af | rl_game_adjust | rl_imp_fraud | rl_click_fraud | rl_anura |')
        lines.append('|-----|--------|--------|-----|------|------|-----|--------|--------|----------|----------|-----------|--------------|----------|--------------|----------------|----------|')
        for _, r in df_high_rej.iterrows():
            rl = r.get('rl_final')
            rl_str = str(int(rl)) if pd.notna(rl) else 'N/A'
            def fv(f): v = r.get(f); return str(int(v)) if pd.notna(v) else 'N/A'
            lines.append(
                f"| {int(r['adv_id'])} | {r['affiliate_id']} | {r['bundle_id']} | {r['first_ssp']} | "
                f"{int(r['pass_cnt'])} | {int(r['reject_cnt'])} | {int(r['pa_cnt'])} | "
                f"{int(r['total_conv'])} | {r['reject_ratio']*100:.2f}% | {rl_str} | {risk_label(rl)} | "
                f"{fv('rl_bundle')} | {fv('rl_game_af')} | {fv('rl_game_adjust')} | "
                f"{fv('rl_imp_fraud')} | {fv('rl_click_fraud')} | {fv('rl_anura')} |"
            )
    lines.append('')

    # 4.6 风险分分级（S1-S5）
    def risk_tier(v):
        if pd.isna(v): return 'S?'
        v = int(v)
        if v <= 20:  return 'S1'
        if v <= 40:  return 'S2'
        if v <= 60:  return 'S3'
        if v <= 80:  return 'S4'
        return 'S5'

    def adv_category(adv_id, name):
        n = name.lower()
        if 'cpi' in n: return 'CPI游戏'
        game_kw = ['train','miner','domino','bus','escape','gold','bricks','legend',
                   'mahjong','mechange','monsters','gang','balls','bounce','find',
                   'satistory','tidy','bubble','shooter','capcut','vizor','magic',
                   'jigsaw','fizzo','club','vegas','tiles']
        if any(k in n for k in game_kw): return '休闲游戏'
        if name == str(adv_id): return '未命名广告主'
        return '其他'

    df['risk_tier'] = df['rl_final'].apply(risk_tier)

    tier_order = ['S1', 'S2', 'S3', 'S4', 'S5', 'S?']
    tier_labels = {'S1': 'S1 ✅', 'S2': 'S2 🟡', 'S3': 'S3 🟠', 'S4': 'S4 🔶', 'S5': 'S5 🔴', 'S?': 'S? ⬜'}

    lines.append('## 六、媒体风险分分级（S1–S5）\n')
    lines.append('> 基于 rl_final 每 20 分一段：S1(0-20) S2(21-40) S3(41-60) S4(61-80) S5(81-100)，无画像为 S?\n')

    # 整体分级分布
    lines.append('### 6.1 整体分级分布\n')
    lines.append('| 风险等级 | 媒体数 | 媒体占比 | 曝光 | 点击 | 通过转化 | 通过占比 | 拒绝转化 | 拒绝占比 | PA转化 | PA占比 | 总转化 | 转化占比 | 拒绝率 |')
    lines.append('|----------|--------|----------|------|------|----------|----------|----------|----------|--------|--------|--------|----------|--------|')
    total_media = len(df)
    total_pass_all = df['pass_cnt'].sum()
    total_rej_all = df['reject_cnt'].sum()
    total_pa_all = df['pa_cnt'].sum()
    total_conv_all = df['total_conv'].sum()
    for tier in tier_order:
        sub = df[df['risk_tier'] == tier]
        if sub.empty: continue
        sub_pass = sub['pass_cnt'].sum()
        sub_rej = sub['reject_cnt'].sum()
        sub_pa = sub['pa_cnt'].sum()
        sub_conv = sub['total_conv'].sum()
        rej_rate = (sub_rej + sub_pa) / sub_conv * 100 if sub_conv > 0 else 0
        lines.append(
            f"| {tier_labels[tier]} | {len(sub)} | {len(sub)/total_media*100:.1f}% | "
            f"{int(sub['imp_cnt'].sum()):,} | {int(sub['click_cnt'].sum()):,} | "
            f"{int(sub_pass):,} | {sub_pass/total_pass_all*100:.1f}% | "
            f"{int(sub_rej):,} | {(sub_rej/total_rej_all*100 if total_rej_all > 0 else 0):.1f}% | "
            f"{int(sub_pa):,} | {(sub_pa/total_pa_all*100 if total_pa_all > 0 else 0):.1f}% | "
            f"{int(sub_conv):,} | {sub_conv/total_conv_all*100:.1f}% | "
            f"{rej_rate:.1f}% |"
        )
    lines.append('')

    # 广告主分类 × 风险分级
    lines.append('### 6.2 广告主分类 × 风险分级\n')
    df['adv_category'] = df.apply(
        lambda r: adv_category(int(r['adv_id']), ADV_INFO.get(int(r['adv_id']), str(int(r['adv_id'])))), axis=1
    )
    lines.append('| 广告主类型 | 广告主数 | 媒体数 | S1✅ | S2🟡 | S3🟠 | S4🔶 | S5🔴 | S4+S5占比 |')
    lines.append('|------------|----------|--------|------|------|------|------|------|-----------|')
    for cat in ['CPI游戏', '休闲游戏', '未命名广告主', '其他']:
        sub = df[df['adv_category'] == cat]
        if sub.empty: continue
        adv_cnt = sub['adv_id'].nunique()
        media_cnt = len(sub)
        tier_counts = {t: len(sub[sub['risk_tier'] == t]) for t in ['S1','S2','S3','S4','S5']}
        high = tier_counts['S4'] + tier_counts['S5']
        lines.append(
            f"| {cat} | {adv_cnt} | {media_cnt} | "
            f"{tier_counts['S1']} | {tier_counts['S2']} | {tier_counts['S3']} | "
            f"{tier_counts['S4']} | {tier_counts['S5']} | {high/media_cnt*100:.1f}% |"
        )
    lines.append('')

    # 分广告主风险分级明细
    lines.append('### 6.3 分广告主风险分级明细\n')
    lines.append('| ADV ID | 广告主 | 类型 | 媒体数 | S1媒体 | S1转化 | S2媒体 | S2转化 | S3媒体 | S3转化 | S4媒体 | S4转化 | S5媒体 | S5转化 | S4+S5占比 | 拒绝率 |')
    lines.append('|--------|--------|------|--------|--------|--------|--------|--------|--------|--------|--------|--------|--------|--------|-----------|--------|')
    adv_risk_rows = []
    for adv_id_val, g in df.groupby('adv_id'):
        adv_id_int = int(adv_id_val)
        total_conv = g['total_conv'].sum()
        tier_conv = {}
        tier_media = {}
        for t in ['S1','S2','S3','S4','S5']:
            sub_t = g[g['risk_tier'] == t]
            tier_media[t] = len(sub_t)
            tier_conv[t] = sub_t['total_conv'].sum()
        adv_risk_rows.append({
            'adv_id': adv_id_int,
            'name': ADV_INFO.get(adv_id_int, str(adv_id_int)),
            'cat': g['adv_category'].iloc[0],
            'media_cnt': len(g),
            'S1_media': tier_media['S1'],
            'S1_conv': tier_conv['S1'],
            'S2_media': tier_media['S2'],
            'S2_conv': tier_conv['S2'],
            'S3_media': tier_media['S3'],
            'S3_conv': tier_conv['S3'],
            'S4_media': tier_media['S4'],
            'S4_conv': tier_conv['S4'],
            'S5_media': tier_media['S5'],
            'S5_conv': tier_conv['S5'],
            'total_conv': total_conv,
            'reject_cnt': g['reject_cnt'].sum(),
            'pa_cnt': g['pa_cnt'].sum(),
        })
    adv_risk = pd.DataFrame(adv_risk_rows)
    adv_risk['high_pct'] = (adv_risk['S4_media'] + adv_risk['S5_media']) / adv_risk['media_cnt'] * 100
    adv_risk['rej_rate'] = (adv_risk['reject_cnt'] + adv_risk['pa_cnt']) / adv_risk['total_conv'].replace(0, 1) * 100
    adv_risk = adv_risk.sort_values('high_pct', ascending=False)
    for _, r in adv_risk.iterrows():
        lines.append(
            f"| {int(r['adv_id'])} | {r['name']} | {r['cat']} | {int(r['media_cnt'])} | "
            f"{int(r['S1_media'])} | {int(r['S1_conv'])} | "
            f"{int(r['S2_media'])} | {int(r['S2_conv'])} | "
            f"{int(r['S3_media'])} | {int(r['S3_conv'])} | "
            f"{int(r['S4_media'])} | {int(r['S4_conv'])} | "
            f"{int(r['S5_media'])} | {int(r['S5_conv'])} | "
            f"{r['high_pct']:.1f}% | {r['rej_rate']:.1f}% |"
        )
    lines.append('')

    # 优先处理清单
    lines.append('### 6.4 优先处理清单\n')
    p0 = adv_risk[(adv_risk['high_pct'] >= 90) & (adv_risk['rej_rate'] >= 50)]
    p1 = adv_risk[(adv_risk['high_pct'] >= 90) & (adv_risk['rej_rate'] < 50)]
    p2 = adv_risk[(adv_risk['high_pct'] >= 70) & (adv_risk['high_pct'] < 90)]
    lines.append('| 优先级 | ADV ID | 广告主 | S4+S5占比 | 拒绝率 | 原因 |')
    lines.append('|--------|--------|--------|-----------|--------|------|')
    for _, r in p0.iterrows():
        lines.append(f"| P0 🔴 | {int(r['adv_id'])} | {r['name']} | {r['high_pct']:.1f}% | {r['rej_rate']:.1f}% | 高风险媒体占比高 + 高拒绝率 |")
    for _, r in p1.iterrows():
        lines.append(f"| P1 🔶 | {int(r['adv_id'])} | {r['name']} | {r['high_pct']:.1f}% | {r['rej_rate']:.1f}% | 全部 S4+S5，无低风险媒体 |")
    for _, r in p2.iterrows():
        lines.append(f"| P2 🟠 | {int(r['adv_id'])} | {r['name']} | {r['high_pct']:.1f}% | {r['rej_rate']:.1f}% | 高风险媒体占比偏高，需监控 |")
    if p0.empty and p1.empty and p2.empty:
        lines.append('> 无需优先处理的广告主\n')
    lines.append('')

    report = '\n'.join(lines)
    output_file = f'{OUTPUT_DIR}/game_media_risk_analysis_{dt}.md'
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(report)
    print(f'\n✅ 报告已保存: {output_file}')

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        import traceback
        print(f'❌ 执行失败: {e}')
        traceback.print_exc()
        sys.exit(1)
