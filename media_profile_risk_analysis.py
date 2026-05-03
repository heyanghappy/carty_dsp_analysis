#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
媒体画像风险分数分析
读取 OSS 上最新一小时的 Parquet 数据，分析 s_* 和 rl_* 字段分布
每天早上 10:30 定时运行
"""

import oss2
import pyarrow.parquet as pq
import pandas as pd
import io
import os
import sys
from datetime import datetime, timedelta

OUTPUT_DIR = "/home/node/.openclaw/workspace/repos/carty_dsp_analysis"
OSS_ENDPOINT = "https://oss-ap-southeast-1.aliyuncs.com"
OSS_BUCKET = "alisg-pacdsp-bucket-lake-tq-prod-01"
OSS_BASE_PATH = "dsp_tq/media_profile/prod_snapshot"

# 原始指标字段说明（实际存在的字段）
METRIC_FIELDS = {
    'curr_3d_imp_cnt':              '近3天曝光数',
    'curr_3d_imp_cheat_cnt':        '近3天曝光作弊数',
    'curr_3d_imp_fraud_cnt':        '近3天曝光欺诈数',
    'curr_3d_click_cnt':            '近3天点击数',
    'curr_3d_click_cheat_cnt':      '近3天点击作弊数',
    'curr_3d_click_fraud_cnt':      '近3天点击欺诈数',
    'curr_3d_anura_bad_cnt':        '近3天Anura bad数',
    'curr_3d_af_reject_cnt':        '近3天AF拒绝数',
    'curr_3d_af_total_cnt':         '近3天AF总数',
    'curr_7d_cost':                 '近7天成本',
    'curr_7d_revenue':              '近7天收入',
    'hist_max_af_14d_reject_ratio': '历史14天最大AF拒绝率',
    'hist_max_afPA_7d_reject_ratio':'历史7天最大AF PA拒绝率',
    'hist_max_anura_3d_bad_ratio':  '历史3天最大Anura bad率',
}

# rl_* 字段说明
RISK_LEVEL_FIELDS = {
    'rl_final':            '最终风险等级',
    'rl_final_beta':       '最终风险等级(Beta)',
    'rl_imp_fraud':        '展示作弊风险',
    'rl_click_fraud':      '点击作弊风险',
    'rl_af_reject':        'AF拒绝风险',
    'rl_afPA_reject':      'AF PA拒绝风险',
    'rl_bundle_af_reject': 'Bundle AF拒绝风险',
    'rl_bundle':           'Bundle风险',
    'rl_bundle_downloads': 'Bundle下载量风险',
    'rl_soigame':          'SOI Game风险',
    'rl_anura':            'Anura风险',
    'rl_visible_imp':      '可见展示风险',
    'rl_unintent_click':   '非意图点击风险',
    'rl_N_ctr':            'N CTR风险',
    'rl_video_ctr':        '视频CTR风险',
    'rl_roi':              'ROI风险',
    'rl_new_device':       '新设备风险',
    'rl_vendor_abnormal':  '厂商异常风险',
    'rl_game_adjust':      '游戏Adjust风险',
    'rl_game_af':          '游戏AF风险',
}

def get_oss_bucket():
    ak = os.environ.get('OSS_ACCESS_KEY', '')
    sk = os.environ.get('OSS_SECRET_KEY', '')
    if not ak or not sk:
        print("❌ 请设置环境变量 OSS_ACCESS_KEY 和 OSS_SECRET_KEY")
        sys.exit(1)
    auth = oss2.Auth(ak, sk)
    return oss2.Bucket(auth, OSS_ENDPOINT, OSS_BUCKET)

def get_latest_dt_hh(bucket):
    """获取最���的 dt 和 hh"""
    # 获取最新日期
    dates = []
    for obj in oss2.ObjectIterator(bucket, prefix=f'{OSS_BASE_PATH}/', delimiter='/'):
        if obj.is_prefix():
            dt = obj.key.split('dt=')[-1].rstrip('/')
            dates.append(dt)
    if not dates:
        return None, None
    latest_dt = sorted(dates)[-1]

    # 获取该日期最新小时
    hours = []
    for obj in oss2.ObjectIterator(bucket, prefix=f'{OSS_BASE_PATH}/dt={latest_dt}/', delimiter='/'):
        if obj.is_prefix():
            hh = obj.key.split('hh=')[-1].rstrip('/')
            hours.append(hh)
    if not hours:
        return latest_dt, None
    latest_hh = sorted(hours)[-1]

    return latest_dt, latest_hh

def read_parquet_dir(bucket, dt, hh):
    """读取某个目录下所有 parquet 文件"""
    prefix = f'{OSS_BASE_PATH}/dt={dt}/hh={hh}/'
    dfs = []
    count = 0
    for obj in oss2.ObjectIterator(bucket, prefix=prefix):
        if not obj.key.endswith('.parquet'):
            continue
        count += 1
        data = bucket.get_object(obj.key).read()
        table = pq.read_table(io.BytesIO(data))
        dfs.append(table.to_pandas())
        print(f"  已读取 {count} 个文件，当前共 {sum(len(d) for d in dfs):,} 条记录", end='\r')

    print()
    if not dfs:
        return None
    return pd.concat(dfs, ignore_index=True)

def fmt_num(n):
    return f"{int(n):,}"

def fmt_pct(n, total):
    if not total:
        return "0.00%"
    return f"{n/total*100:.2f}%"

def analyze_distribution(series, total):
    """统计值分布，返回 markdown 表格行"""
    vc = series.value_counts(dropna=False).reset_index()
    vc.columns = ['value', 'cnt']
    vc = vc.sort_values('value')
    rows = []
    for _, row in vc.iterrows():
        val = str(int(row['value'])) if pd.notna(row['value']) else 'NULL'
        rows.append(f"| {val} | {fmt_num(row['cnt'])} | {fmt_pct(row['cnt'], total)} |")
    return rows

def analyze_numeric_stats(series):
    """统计数值字段的百分位分布"""
    s = series.dropna()
    if len(s) == 0:
        return []
    lines = []
    lines.append(f"- min={s.min():.0f}, max={s.max():.0f}, mean={s.mean():.1f}, "
                 f"p25={s.quantile(0.25):.0f}, p50={s.quantile(0.5):.0f}, "
                 f"p75={s.quantile(0.75):.0f}, p90={s.quantile(0.9):.0f}, p99={s.quantile(0.99):.0f}")
    return lines

def analyze_risk_level_summary(df, total):
    """生成风险等级汇总表格，返回markdown行列表"""
    lines = []

    # 添加汇总表格标题
    lines.append("## 风险等级汇总")
    lines.append("")
    lines.append("| 字段名 | 中文说明 | 风险等级分布概要 | 无风险(0)占比 | 低风险(10-20)占比 | 中风险(30-60)占比 | 高风险(70-100)占比 |")
    lines.append("|--------|----------|------------------|---------------|-------------------|-------------------|-------------------|")

    for field, desc in RISK_LEVEL_FIELDS.items():
        if field not in df.columns:
            continue

        series = df[field]
        value_counts = series.value_counts(dropna=False)

        # 计算各风险等级范围的占比
        zero_pct = 0.0
        low_pct = 0.0      # 10-20
        medium_pct = 0.0   # 30-60
        high_pct = 0.0     # 70-100

        # 收集主要非零等级（占比>0.1%）
        main_levels = []

        for level, count in value_counts.items():
            if pd.isna(level):
                continue

            level_int = int(level)
            pct = count / total * 100

            if level_int == 0:
                zero_pct = pct
            elif 10 <= level_int <= 20:
                low_pct += pct
            elif 30 <= level_int <= 60:
                medium_pct += pct
            elif 70 <= level_int <= 100:
                high_pct += pct

            # 记录占比>0.1%的非零等级
            if level_int > 0 and pct > 0.1:
                main_levels.append((level_int, pct))

        # 生成分布概要字符串
        if zero_pct == 100.0:
            summary = "全部为0风险"
        elif not main_levels:
            # 如果没有占比>0.1%的非零等级，显示前3个主要等级（按占比降序）
            sorted_levels = []
            for level, count in value_counts.items():
                if pd.isna(level):
                    continue
                level_int = int(level)
                if level_int > 0:
                    pct = count / total * 100
                    sorted_levels.append((level_int, pct))

            sorted_levels.sort(key=lambda x: x[1], reverse=True)
            summary_parts = []
            for level_int, pct in sorted_levels[:3]:
                if pct > 0.01:  # 至少0.01%才显示
                    summary_parts.append(f"{level_int}({pct:.2f}%)")
            summary = ", ".join(summary_parts) if summary_parts else "-"
        else:
            # 显示占比>0.1%的主要等级（最多3个）
            main_levels.sort(key=lambda x: x[1], reverse=True)
            summary_parts = [f"{level}({pct:.2f}%)" for level, pct in main_levels[:3]]
            summary = ", ".join(summary_parts)

        # 添加表格行
        lines.append(f"| {field} | {desc} | {summary} | {zero_pct:.2f}% | {low_pct:.2f}% | {medium_pct:.2f}% | {high_pct:.2f}% |")

    lines.append("")
    lines.append("**说明**")
    lines.append("1. **无风险(0)**: 风险等级为0，表示无风险")
    lines.append("2. **低风险(10-20)**: 风险等级10-20，轻微风险")
    lines.append("3. **中风险(30-60)**: 风险等级30-60，中等风险")
    lines.append("4. **高风险(70-100)**: 风险等级70-100，高风险")
    lines.append("5. **分布概要**: 显示占比>0.1%的非零风险等级及其占比，最多显示3个主要等级")
    lines.append("")

    return lines

def analyze_risk_level_detail_distribution(df, total):
    """生成风险等级详细分布表格（实际出现的值），返回markdown行列表"""
    lines = []

    # 收集所有字段的实际值分布
    field_data = {}
    field_descriptions = {}
    all_levels = set()

    for field, desc in RISK_LEVEL_FIELDS.items():
        if field not in df.columns:
            continue

        series = df[field]
        value_counts = series.value_counts(dropna=False)

        level_pcts = {}
        for level, count in value_counts.items():
            if pd.isna(level):
                continue
            level_int = int(level)
            pct = count / total * 100
            level_pcts[level_int] = pct
            all_levels.add(level_int)

        field_data[field] = level_pcts
        field_descriptions[field] = desc

    if not field_data:
        return lines

    # 只保留至少一个字段有非零占比的等级值，排序
    levels = sorted(all_levels)

    lines.append("## 风险等级详细分布（百分比）")
    lines.append("")
    lines.append("> 仅显示数据中实际出现的等级值，空白表示占比为 0%")
    lines.append("")

    header_cols = ["字段", "说明"] + [str(l) for l in levels]
    lines.append("| " + " | ".join(header_cols) + " |")
    lines.append("|" + "|".join(["------"] * len(header_cols)) + "|")

    for field, level_pcts in field_data.items():
        desc = field_descriptions[field]
        pct_strs = []
        for l in levels:
            pct = level_pcts.get(l, 0.0)
            pct_strs.append(f"{pct:.2f}%" if pct > 0 else "")
        row_data = [field, desc] + pct_strs
        lines.append("| " + " | ".join(row_data) + " |")

    lines.append("")
    lines.append("**说明**: 表格显示各风险等级字段在实际出现的等级值上的百分比分布。等级值为0表示无风险，100表示最高风险。")
    lines.append("")

    return lines

def main():
    if len(sys.argv) > 1:
        dt = sys.argv[1]
        hh = sys.argv[2].zfill(2) if len(sys.argv) > 2 else None
    else:
        dt, hh = None, None

    bucket = get_oss_bucket()

    if dt is None:
        dt, hh = get_latest_dt_hh(bucket)
        print(f"自动检测最新数据: {dt} {hh}:00")
    else:
        if hh is None:
            # 找该日期最新小时
            hours = []
            for obj in oss2.ObjectIterator(bucket, prefix=f'{OSS_BASE_PATH}/dt={dt}/', delimiter='/'):
                if obj.is_prefix():
                    hours.append(obj.key.split('hh=')[-1].rstrip('/'))
            hh = sorted(hours)[-1] if hours else '23'
        print(f"分析日期: {dt} {hh}:00")

    print(f"读取路径: oss://{OSS_BUCKET}/{OSS_BASE_PATH}/dt={dt}/hh={hh}/")
    df = read_parquet_dir(bucket, dt, hh)
    if df is None:
        print("❌ 无数据")
        sys.exit(1)

    total = len(df)
    print(f"共 {total:,} 条记录，{len(df.columns)} 个字段")
    print(f"字段列表: {', '.join(df.columns.tolist())}\n")

    lines = []
    lines.append(f"# 媒体画像风险分数分析报告\n")
    lines.append(f"**数据时间**: {dt} {hh}:00\n")
    lines.append(f"**总记录数**: {fmt_num(total)}\n")
    lines.append(f"**数据路径**: oss://{OSS_BUCKET}/{OSS_BASE_PATH}/dt={dt}/hh={hh}/\n")
    lines.append("---\n")

    # ── 一、rl_* 风险等级字段分布 ──────────────────────────────────
    lines.append("## 一、风险等级（rl_*）字段分布\n")
    for field, desc in RISK_LEVEL_FIELDS.items():
        if field not in df.columns:
            continue
        print(f"分析 {field}...")
        lines.append(f"### {field}（{desc}）\n")
        lines.append("| 等级值 | 数量 | 占比 |")
        lines.append("|--------|----------|--------|")
        lines.extend(analyze_distribution(df[field], total))
        lines.append("")

    # ── 风险等级汇总表格 ───────────────────────────────────────────
    print("生成风险等级汇总表格...")
    lines.extend(analyze_risk_level_summary(df, total))

    # ── 风险等级详细分布表格 ───────────────────────────────────────
    print("生成风险等级详细分布表格...")
    lines.extend(analyze_risk_level_detail_distribution(df, total))

    # ── 二、原始指标字段统计 ──────────────────────────────────────
    lines.append("## 二、原始指标字段统计\n")
    lines.append("| 字段 | 说明 | min | max | mean | p25 | p50 | p75 | p90 | p99 |")
    lines.append("|------|------|-----|-----|------|-----|-----|-----|-----|-----|")
    for field, desc in METRIC_FIELDS.items():
        if field not in df.columns:
            continue
        print(f"分析 {field}...")
        s = df[field].dropna()
        if len(s) == 0:
            lines.append(f"| {field} | {desc} | - | - | - | - | - | - | - | - |")
        else:
            lines.append(
                f"| {field} | {desc} "
                f"| {s.min():.0f} | {s.max():.0f} | {s.mean():.1f} "
                f"| {s.quantile(0.25):.0f} | {s.quantile(0.5):.0f} | {s.quantile(0.75):.0f} "
                f"| {s.quantile(0.9):.0f} | {s.quantile(0.99):.0f} |"
            )
    lines.append("")

    # ── 四、bundle / domain 字段分析 ─────────────────────────────
    for field in ['bundle_id', 'domain']:
        if field not in df.columns:
            continue
        print(f"分析 {field}...")
        lines.append(f"## 四、{field} 字段分析\n" if field == 'bundle_id' else f"## 五、{field} 字段分析\n")

        series = df[field].dropna().astype(str)
        unique_cnt = series.nunique()
        lines.append(f"- **唯一值数量**: {fmt_num(unique_cnt)}")
        lines.append(f"- **非空记录数**: {fmt_num(len(series))}")
        lines.append("")

        # Top 20 出现频次
        lines.append(f"### Top 20 {field}（出现频次）\n")
        lines.append(f"| {field} | 数量 | 占比 |")
        lines.append("|--------|----------|--------|")
        top20 = series.value_counts().head(20)
        for val, cnt in top20.items():
            lines.append(f"| {val} | {fmt_num(cnt)} | {fmt_pct(cnt, total)} |")
        lines.append("")

        # 高风险 bundle/domain（rl_final >= 70）
        if 'rl_final' in df.columns:
            high_risk = df[df['rl_final'] >= 70][[field, 'rl_final']].dropna(subset=[field])
            high_risk[field] = high_risk[field].astype(str)
            if len(high_risk) > 0:
                lines.append(f"### 高风险 {field}（rl_final ≥ 70）\n")
                lines.append(f"- **高风险记录数**: {fmt_num(len(high_risk))}（占总量 {fmt_pct(len(high_risk), total)}）")
                lines.append(f"- **涉及唯一 {field} 数**: {fmt_num(high_risk[field].nunique())}")
                lines.append("")

                # 按 rl_final 均值排序，取 Top 20
                agg = high_risk.groupby(field).agg(
                    记录数=(field, 'count'),
                    rl_final_max=('rl_final', 'max'),
                    rl_final_mean=('rl_final', 'mean'),
                ).sort_values('记录数', ascending=False).head(20)

                lines.append(f"| {field} | 记录数 | rl_final最大值 | rl_final均值 |")
                lines.append("|--------|----------|----------------|--------------|")
                for val, row in agg.iterrows():
                    lines.append(f"| {val} | {fmt_num(row['记录数'])} | {int(row['rl_final_max'])} | {row['rl_final_mean']:.1f} |")
                lines.append("")
            else:
                lines.append(f"### 高风险 {field}（rl_final ≥ 70）\n")
                lines.append("无高风险记录。\n")

    report = "\n".join(lines)
    output_file = f"{OUTPUT_DIR}/media_profile_risk_analysis_{dt}_{hh}.md"
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
