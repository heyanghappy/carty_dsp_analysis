#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全广告主媒体风险分级分析
基于 aff+bundle+ssp 组合的 rl_final 风险评分，生成 S1-S5 分级报告
报告结构：
  1. 整体汇总（S1-S5 分布）
  2. 分行业汇总（S1-S5 分布）
  3. 分广告主明细（S1-S5 分布）
"""

import pymysql
import pandas as pd
import sys
import os
import io
from datetime import datetime, timedelta
import pyarrow.parquet as pq
import oss2

sys.path.insert(0, '/Users/gztd-03-01457/Work/claude')
from daily_cheat_report import ADV_INFO, ADV_GROUPS

SR_HOST = "fe-c-907795efe3201917.starrocks.aliyuncs.com"
SR_PORT = 9030
SR_USER = "sandy"
SR_PASSWORD = "MXeptLkEkoi2$FMX"

OSS_ENDPOINT = "https://oss-ap-southeast-1.aliyuncs.com"
OSS_BUCKET = "alisg-pacdsp-bucket-lake-tq-prod-01"
OSS_BASE_PATH = "dsp_tq/media_profile/prod_snapshot"
RISK_FIELDS = ['rl_final']

OUTPUT_DIR = "/Users/gztd-03-01457/Work/claude"


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


def risk_tier(v):
    """将 rl_final 分数映射到 S1-S5"""
    if pd.isna(v):
        return 'S?'
    v = int(v)
    if v <= 20:
        return 'S1'
    if v <= 40:
        return 'S2'
    if v <= 60:
        return 'S3'
    if v <= 80:
        return 'S4'
    return 'S5'


def tier_emoji(tier):
    """风险等级 emoji"""
    return {'S1': '✅', 'S2': '🟡', 'S3': '🟠', 'S4': '🔶', 'S5': '🔴', 'S?': '⬜'}.get(tier, '')


def fmt_num(n):
    if pd.isna(n) or n is None:
        return "0"
    return f"{int(n):,}"


def fmt_pct(n):
    if pd.isna(n) or n is None:
        return "0.00%"
    return f"{float(n):.2f}%"


def main():
    if len(sys.argv) >= 3:
        # 多日期汇总模式: start_dt end_dt
        start_dt = datetime.strptime(sys.argv[1], '%Y%m%d')
        end_dt = datetime.strptime(sys.argv[2], '%Y%m%d')
        date_list = []
        d = start_dt
        while d <= end_dt:
            date_list.append(d)
            d += timedelta(days=1)
        dt_str = sys.argv[1]  # 用起始日期作为文件名后缀
        report_label = f"{start_dt.strftime('%Y-%m-%d')} ~ {end_dt.strftime('%Y-%m-%d')}"
        multi_date = True
    elif len(sys.argv) == 2:
        dt = datetime.strptime(sys.argv[1], '%Y%m%d')
        date_list = [dt]
        dt_str = sys.argv[1]
        report_label = dt.strftime('%Y-%m-%d')
        multi_date = False
    else:
        dt = datetime.now() - timedelta(days=1)
        date_list = [dt]
        dt_str = dt.strftime('%Y%m%d')
        report_label = dt.strftime('%Y-%m-%d')
        multi_date = False

    print(f"开始分析: {report_label}")

    # ── 1. 读取 StarRocks 数据 ──────────────────────────────
    print("查询 StarRocks...")
    conn = pymysql.connect(
        host=SR_HOST, port=SR_PORT, user=SR_USER, password=SR_PASSWORD,
        charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor
    )

    date_strs = [d.strftime('%Y-%m-%d') for d in date_list]
    date_in = "', '".join(date_strs)
    sql = f"""
    SELECT
        adv_id,
        first_ssp AS ssp,
        affiliate_id AS aff,
        bundle_id AS bundle,
        SUM(imp) AS imp,
        SUM(click) AS click,
        SUM(conversion) AS conv
    FROM cdm.dwm_cross_placement_audience_detail_view
    WHERE report_date IN ('{date_in}')
        AND imp > 0
    GROUP BY adv_id, first_ssp, affiliate_id, bundle_id
    """

    with conn.cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()
    conn.close()

    if not rows:
        print(f"❌ 暂无数据: {report_label}")
        return

    print(f"✅ 获取 {len(rows)} 条媒体组合数据")

    # ── 2. 读取 OSS 媒体画像风险数据（用最后一天的画像，找不到则往后找）──────────
    print("加载 OSS 媒体画像数据...")
    bucket = get_oss_bucket()
    # 先尝试最后一天，找不到则找最新可用日期
    profile_dt_str = date_list[-1].strftime('%Y%m%d')
    hh = get_latest_hh(bucket, profile_dt_str)
    if not hh:
        # 找 OSS 上最新可用日期
        available_dates = []
        for obj in oss2.ObjectIterator(bucket, prefix=f'{OSS_BASE_PATH}/', delimiter='/'):
            if obj.is_prefix():
                available_dates.append(obj.key.split('dt=')[-1].rstrip('/'))
        if available_dates:
            profile_dt_str = sorted(available_dates)[-1]
            hh = get_latest_hh(bucket, profile_dt_str)
            print(f"  ⚠️  {date_list[-1].strftime('%Y%m%d')} 无画像数据，回退到 {profile_dt_str}")
    if not hh:
        print(f"⚠️  OSS 无可用媒体画像数据，跳过风险等级")
        profile_dict = {}
    else:
        print(f"  使用 dt={profile_dt_str} hh={hh}")
        df_profile = load_media_profile(bucket, profile_dt_str, hh)
        if df_profile is not None:
            print(f"  媒体画像共 {len(df_profile):,} 条唯一 lookupkey")
            profile_dict = df_profile['rl_final'].to_dict()
        else:
            profile_dict = {}

    # ── 3. 合并数据 ──────────────────────────────────────────
    print("合并数据...")
    df = pd.DataFrame(rows)
    SEP = '\x02'
    df['lookupkey'] = (
        df['ssp'].astype(str) + SEP +
        df['aff'].astype(str) + SEP +
        df['bundle'].astype(str)
    )
    df['rl_final'] = df['lookupkey'].map(profile_dict) if profile_dict else None
    df['risk_tier'] = df['rl_final'].apply(risk_tier)

    # 添加广告主信息
    df['adv_id'] = df['adv_id'].astype(int)
    df['adv_name'] = df['adv_id'].apply(lambda x: ADV_INFO.get(x, (str(x), '未知'))[0])
    df['industry'] = df['adv_id'].apply(lambda x: ADV_INFO.get(x, (str(x), '未知'))[1])

    # 构建 adv_id -> 大类 映射
    adv_id_to_group = {}
    for grp, ids in ADV_GROUPS.items():
        for aid in ids:
            adv_id_to_group[aid] = grp
    df['group'] = df['adv_id'].apply(lambda x: adv_id_to_group.get(x, '未知'))

    print(f"✅ 合并完成，共 {len(df)} 条记录")

    # ── 4. 生成报告 ──────────────────────────────────────────
    print("生成报告...")

    lines = []
    lines.append(f"# 全广告主媒体风险分级报告（{report_label}）")
    lines.append("")
    lines.append("> 数据来源: cdm.dwm_cross_placement_audience_detail_view + OSS 媒体画像")
    lines.append("> 风险分级: S1(0-20)✅ / S2(21-40)🟡 / S3(41-60)🟠 / S4(61-80)🔶 / S5(81-100)🔴 / S?(未匹配)⬜")
    lines.append("")

    # ── 4.1 整体汇总 ──────────────────────────────────────
    lines.append("## 一、整体汇总")
    lines.append("")

    overall = df.groupby('risk_tier').agg({
        'imp': 'sum',
        'click': 'sum',
        'conv': 'sum',
        'lookupkey': 'count'
    }).rename(columns={'lookupkey': 'media_cnt'})

    total_imp = overall['imp'].sum()
    total_click = overall['click'].sum()
    total_conv = overall['conv'].sum()
    total_media = overall['media_cnt'].sum()

    lines.append("| 风险等级 | 媒体组合数 | 占比 | 曝光 | 占比 | 点击 | 占比 | 转化 | 占比 |")
    lines.append("|---------|-----------|------|------|------|------|------|------|------|")

    tier_order = ['S1', 'S2', 'S3', 'S4', 'S5', 'S?']
    for tier in tier_order:
        if tier not in overall.index:
            continue
        row = overall.loc[tier]
        emoji = tier_emoji(tier)
        lines.append(
            f"| {tier}{emoji} | {fmt_num(row['media_cnt'])} | "
            f"{fmt_pct(row['media_cnt']/total_media*100)} | "
            f"{fmt_num(row['imp'])} | {fmt_pct(row['imp']/total_imp*100)} | "
            f"{fmt_num(row['click'])} | {fmt_pct(row['click']/total_click*100)} | "
            f"{fmt_num(row['conv'])} | {fmt_pct(row['conv']/total_conv*100)} |"
        )

    lines.append(f"| **合计** | **{fmt_num(total_media)}** | **100.00%** | "
                 f"**{fmt_num(total_imp)}** | **100.00%** | "
                 f"**{fmt_num(total_click)}** | **100.00%** | "
                 f"**{fmt_num(total_conv)}** | **100.00%** |")
    lines.append("")

    # ── 4.2 大类汇总 ──────────────────────────────────────
    lines.append("## 二、大类汇总（DF-APP / DF-web / SKA / IAA-Game）")
    lines.append("")

    group_stats = df.groupby(['group', 'risk_tier']).agg({
        'imp': 'sum',
        'click': 'sum',
        'conv': 'sum',
        'lookupkey': 'count'
    }).rename(columns={'lookupkey': 'media_cnt'})

    group_order = ['SKA', 'DF-APP', 'DF-web', 'IAA-Game', '未知']
    for group in group_order:
        if group not in group_stats.index.get_level_values(0):
            continue

        grp_data = group_stats.loc[group]
        grp_total_media = grp_data['media_cnt'].sum()
        grp_total_imp = grp_data['imp'].sum()
        grp_total_click = grp_data['click'].sum()
        grp_total_conv = grp_data['conv'].sum()

        lines.append(f"### {group}")
        lines.append(f"> 媒体组合: {fmt_num(grp_total_media)} | 曝光: {fmt_num(grp_total_imp)} | "
                     f"点击: {fmt_num(grp_total_click)} | 转化: {fmt_num(grp_total_conv)}")
        lines.append("")
        lines.append("| 风险等级 | 媒体组合数 | 占比 | 曝光 | 占比 | 点击 | 占比 | 转化 | 占比 |")
        lines.append("|---------|-----------|------|------|------|------|------|------|------|")

        for tier in tier_order:
            if tier not in grp_data.index:
                continue
            row = grp_data.loc[tier]
            emoji = tier_emoji(tier)
            conv_pct = fmt_pct(row['conv']/grp_total_conv*100) if grp_total_conv > 0 else "0.00%"
            lines.append(
                f"| {tier}{emoji} | {fmt_num(row['media_cnt'])} | "
                f"{fmt_pct(row['media_cnt']/grp_total_media*100)} | "
                f"{fmt_num(row['imp'])} | {fmt_pct(row['imp']/grp_total_imp*100)} | "
                f"{fmt_num(row['click'])} | {fmt_pct(row['click']/grp_total_click*100)} | "
                f"{fmt_num(row['conv'])} | {conv_pct} |"
            )

        lines.append("")

    # ── 4.3 分行业汇总 ──────────────────────────────────────
    lines.append("## 三、分行业汇总")
    lines.append("")

    # 按 (group, industry) 聚合
    industry_stats = df.groupby(['group', 'industry', 'risk_tier']).agg({
        'imp': 'sum',
        'click': 'sum',
        'conv': 'sum',
        'lookupkey': 'count'
    }).rename(columns={'lookupkey': 'media_cnt'})

    # 按 group 排序输出
    for group in group_order:
        group_industries = []
        if group in industry_stats.index.get_level_values(0):
            group_industries = industry_stats.loc[group].index.get_level_values(0).unique()

        for industry in sorted(group_industries):
            ind_data = industry_stats.loc[(group, industry)]
            ind_total_media = ind_data['media_cnt'].sum()
            ind_total_imp = ind_data['imp'].sum()
            ind_total_click = ind_data['click'].sum()
            ind_total_conv = ind_data['conv'].sum()

            lines.append(f"### {group} - {industry}")
            lines.append(f"> 媒体组合: {fmt_num(ind_total_media)} | 曝光: {fmt_num(ind_total_imp)} | "
                         f"点击: {fmt_num(ind_total_click)} | 转化: {fmt_num(ind_total_conv)}")
            lines.append("")
            lines.append("| 风险等级 | 媒体组合数 | 占比 | 曝光 | 占比 | 点击 | 占比 | 转化 | 占比 |")
            lines.append("|---------|-----------|------|------|------|------|------|------|------|")

            for tier in tier_order:
                if tier not in ind_data.index:
                    continue
                row = ind_data.loc[tier]
                emoji = tier_emoji(tier)
                conv_pct = fmt_pct(row['conv']/ind_total_conv*100) if ind_total_conv > 0 else "0.00%"
                lines.append(
                    f"| {tier}{emoji} | {fmt_num(row['media_cnt'])} | "
                    f"{fmt_pct(row['media_cnt']/ind_total_media*100)} | "
                    f"{fmt_num(row['imp'])} | {fmt_pct(row['imp']/ind_total_imp*100)} | "
                    f"{fmt_num(row['click'])} | {fmt_pct(row['click']/ind_total_click*100)} | "
                    f"{fmt_num(row['conv'])} | {conv_pct} |"
                )

            lines.append("")

    # ── 4.4 分广告主明细 ──────────────────────────────────────
    lines.append("## 四、分广告主明细")
    lines.append("")

    adv_stats = df.groupby(['group', 'industry', 'adv_id', 'adv_name', 'risk_tier']).agg({
        'imp': 'sum',
        'click': 'sum',
        'conv': 'sum',
        'lookupkey': 'count'
    }).rename(columns={'lookupkey': 'media_cnt'})

    for group in group_order:
        if group not in adv_stats.index.get_level_values(0):
            continue
        group_industries = adv_stats.loc[group].index.get_level_values(0).unique()

        for industry in sorted(group_industries):
            lines.append(f"### {group} - {industry}")
            lines.append("")
            lines.append("| 广告主 | adv_id | 风险等级 | 媒体组合数 | 曝光 | 点击 | 转化 |")
            lines.append("|--------|-------:|---------|-----------|------|------|------|")

            ind_advs = df[(df['group'] == group) & (df['industry'] == industry)]['adv_id'].unique()
            for adv_id in sorted(ind_advs):
                adv_name = df[df['adv_id'] == adv_id]['adv_name'].iloc[0]

                try:
                    adv_data = adv_stats.loc[(group, industry, adv_id, adv_name)]
                except KeyError:
                    continue

                adv_total_media = adv_data['media_cnt'].sum()
                adv_total_imp = adv_data['imp'].sum()
                adv_total_click = adv_data['click'].sum()
                adv_total_conv = adv_data['conv'].sum()

                lines.append(
                    f"| **{adv_name}** | **{adv_id}** | **合计** | "
                    f"**{fmt_num(adv_total_media)}** | **{fmt_num(adv_total_imp)}** | "
                    f"**{fmt_num(adv_total_click)}** | **{fmt_num(adv_total_conv)}** |"
                )

                for tier in tier_order:
                    if tier not in adv_data.index:
                        continue
                    row = adv_data.loc[tier]
                    emoji = tier_emoji(tier)
                    lines.append(
                        f"| {adv_name} | {adv_id} | {tier}{emoji} | "
                        f"{fmt_num(row['media_cnt'])} | {fmt_num(row['imp'])} | "
                        f"{fmt_num(row['click'])} | {fmt_num(row['conv'])} |"
                    )

            lines.append("")

    # ── 5. 保存报告 ──────────────────────────────────────────
    output_file = f"{OUTPUT_DIR}/all_adv_media_risk_analysis_{dt_str}{'_' + date_list[-1].strftime('%Y%m%d') if multi_date else ''}.md"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))

    print(f"✅ 报告已保存: {output_file}")


if __name__ == '__main__':
    main()
