#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
游戏广告主媒体分析（aff+ssp+bundle）
分析维度：曝光、点击、转化、拒绝、PA转化、拒绝率、媒体风险等级
"""

import oss2
import pyarrow.parquet as pq
import pandas as pd
import pymysql
import io
import os
import sys
from datetime import datetime, timedelta

SR_HOST = "fe-c-907795efe3201917.starrocks.aliyuncs.com"
SR_PORT = 9030
SR_USER = "sandy"
SR_PASSWORD = "MXeptLkEkoi2$FMX"

OSS_ENDPOINT = "https://oss-ap-southeast-1.aliyuncs.com"
OSS_BUCKET = "alisg-pacdsp-bucket-lake-tq-prod-01"
OSS_BASE_PATH = "dsp_tq/media_profile"

OUTPUT_DIR = "/Users/gztd-03-01457/Work/claude"

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

RISK_FIELDS = ['rl_final', 'rl_bundle', 'rl_af_reject', 'rl_af_pa',
               'rl_imp_fraud', 'rl_click_fraud', 'rl_anura']

def risk_label(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return 'N/A'
    v = int(v)
    if v == 0:   return '✅ 无风险'
    if v <= 20:  return '🟡 低风险'
    if v <= 60:  return '🟠 中风险'
    return '🔴 高风险'

def get_oss_bucket():
    ak = os.environ.get('OSS_ACCESS_KEY', '')
    sk = os.environ.get('OSS_SECRET_KEY', '')
    if not ak or not sk:
        print('❌ 请设置环境变量 OSS_ACCESS_KEY 和 OSS_SECRET_KEY')
        sys.exit(1)
    auth = oss2.Auth(ak, sk)
    return oss2.Bucket(auth, OSS_ENDPOINT, OSS_BUCKET)

def get_latest_hh(bucket, dt):
    hours = []
    for obj in oss2.ObjectIterator(bucket, prefix=f'{OSS_BASE_PATH}/dt={dt}/', delimiter='/'):
        if obj.is_prefix():
            hours.append(obj.key.split('hh=')[-1].rstrip('/'))
    return sorted(hours)[-1] if hours else None

def load_media_profile(bucket, dt, hh):
    prefix = f'{OSS_BASE_PATH}/dt={dt}/hh={hh}/'
    dfs = []
    count = 0
    for obj in oss2.ObjectIterator(bucket, prefix=prefix):
        if not obj.key.endswith('.parquet'):
            continue
        count += 1
        data = bucket.get_object(obj.key).read()
        table = pq.read_table(io.BytesIO(data))
        df = table.to_pandas()
        keep_cols = ['lookupkey'] + [f for f in RISK_FIELDS if f in df.columns]
        dfs.append(df[keep_cols])
        print(f'  已读取 {count} 个文件，共 {sum(len(d) for d in dfs):,} 条', end='\r')
    print()
    if not dfs:
        return None
    df_all = pd.concat(dfs, ignore_index=True)
    df_all = df_all.sort_values('rl_final', ascending=False).drop_duplicates('lookupkey')
    df_all = df_all.set_index('lookupkey')
    return df_all

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
    SELECT adv_id, affiliate_id, bundle_id, first_ssp,
           imp_cnt, click_cnt, pass_cnt, reject_cnt, pa_cnt, total_conv,
           ROUND(
               CASE WHEN total_conv = 0 THEN 0
                    ELSE (reject_cnt + pa_cnt) * 1.0 / total_conv
               END, 4
           ) AS reject_ratio
    FROM dim_base
    WHERE adv_id IS NOT NULL
    ORDER BY adv_id, reject_ratio DESC, reject_cnt DESC
    """

    print('查询 StarRocks 数据（曝光+点击+转化+拒绝）...')
    with conn.cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()
    conn.close()
    print(f'  共 {len(rows)} 条 adv+aff+bundle+ssp 记录')
    return rows, pa_start, pa_end

def main():
    dt = sys.argv[1] if len(sys.argv) > 1 else '20260408'
    print(f'分析日期: {dt}')

    # 1. 查询主数据
    rows, pa_start, pa_end = query_main_data(dt)
    if not rows:
        print('❌ 无数据')
        sys.exit(1)

    df = pd.DataFrame(rows)
    for col in ['adv_id']:
        df[col] = df[col].astype(int)
    for col in ['affiliate_id', 'bundle_id', 'first_ssp']:
        df[col] = df[col].astype(str)

    # 2. 加载媒体画像
    print('加载 OSS 媒体画像数据...')
    bucket = get_oss_bucket()
    hh = get_latest_hh(bucket, dt)
    if not hh:
        print(f'⚠️  OSS 无 dt={dt} 的数据，跳过风险等级')
        df_profile = None
    else:
        print(f'  使用 dt={dt} hh={hh}')
        df_profile = load_media_profile(bucket, dt, hh)
        if df_profile is not None:
            print(f'  媒体画像共 {len(df_profile):,} 条唯一 lookupkey')

    # 3. join 风险数据
    SEP = '\x02'
    df['lookupkey'] = (df['first_ssp'].astype(str) + SEP +
                       df['affiliate_id'].astype(str) + SEP +
                       df['bundle_id'].astype(str))

    if df_profile is not None:
        for field in RISK_FIELDS:
            if field in df_profile.columns:
                df[field] = df['lookupkey'].map(df_profile[field])
            else:
                df[field] = None
        matched = df['rl_final'].notna().sum()
        print(f'  匹配到风险数据: {matched}/{len(df)} 条 ({matched/len(df)*100:.1f}%)')
    else:
        for field in RISK_FIELDS:
            df[field] = None

    # 4. 生成报告
    lines = []
    lines.append(f'# 游戏广告主媒体分析报告（aff+ssp+bundle）\n')
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
        media_cnt=('lookupkey', 'count'),
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
        lines.append('| Aff ID | Bundle | SSP | 曝光 | 点击 | 通过 | 拒绝 | PA | 总转化 | 拒绝率 | rl_final | 风险等级 | rl_bundle | rl_af_reject | rl_af_pa | rl_imp_fraud | rl_click_fraud | rl_anura |')
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
                f"{fv('rl_bundle')} | {fv('rl_af_reject')} | {fv('rl_af_pa')} | "
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
        lines.append('| ADV | Aff ID | Bundle | SSP | 曝光 | 点击 | 拒绝率 | rl_final | 风险等级 | rl_bundle | rl_af_reject | rl_af_pa | rl_imp_fraud | rl_click_fraud | rl_anura |')
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
                f"{fv('rl_final')} | {risk_label(rl)} | {fv('rl_bundle')} | {fv('rl_af_reject')} | "
                f"{fv('rl_af_pa')} | {fv('rl_imp_fraud')} | {fv('rl_click_fraud')} | {fv('rl_anura')} |"
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
        lines.append('| ADV | Aff ID | Bundle | SSP | 通过 | 拒绝 | PA | 总转化 | 拒绝率 | rl_final | 风险等级 | rl_bundle | rl_af_reject | rl_af_pa | rl_imp_fraud | rl_click_fraud | rl_anura |')
        lines.append('|-----|--------|--------|-----|------|------|-----|--------|--------|----------|----------|-----------|--------------|----------|--------------|----------------|----------|')
        for _, r in df_high_rej.iterrows():
            rl = r.get('rl_final')
            rl_str = str(int(rl)) if pd.notna(rl) else 'N/A'
            def fv(f): v = r.get(f); return str(int(v)) if pd.notna(v) else 'N/A'
            lines.append(
                f"| {int(r['adv_id'])} | {r['affiliate_id']} | {r['bundle_id']} | {r['first_ssp']} | "
                f"{int(r['pass_cnt'])} | {int(r['reject_cnt'])} | {int(r['pa_cnt'])} | "
                f"{int(r['total_conv'])} | {r['reject_ratio']*100:.2f}% | {rl_str} | {risk_label(rl)} | "
                f"{fv('rl_bundle')} | {fv('rl_af_reject')} | {fv('rl_af_pa')} | "
                f"{fv('rl_imp_fraud')} | {fv('rl_click_fraud')} | {fv('rl_anura')} |"
            )
    lines.append('')

    report = '\n'.join(lines)
    output_file = f'{OUTPUT_DIR}/game_media_analysis_{dt}.md'
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
