#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日 Anura 检测报告 v2.1
每天北京时间 11:10 执行，分析前一天的 anura_check_result 数据
v2.0 升级：
  - bundle 明细升级为 aff + ssp + bundle（JOIN imp 表）
  - 拼接媒体风险画像（rl_final 等级 + 细项打分）
  - 高危广告主（bad率 ≥ 10%）新增 aff+ssp+bundle 维度明细
v2.1 优化（避免大查询超限）：
  - 移除高危广告主明细展开
  - 移除 aff+bundle 明细的 ssp 和媒体风险画像（不再 JOIN imp 和 media_profile）
  - 保留核心汇总表供快速巡检
"""

import pymysql
import sys
from datetime import datetime, timedelta

sys.path.insert(0, '/Users/gztd-03-01457/Work/claude')
from daily_cheat_report import ADV_INFO

SR_HOST = "fe-c-907795efe3201917.starrocks.aliyuncs.com"
SR_PORT = 9030
SR_USER = "sandy"
SR_PASSWORD = "MXeptLkEkoi2$FMX"

OUTPUT_DIR = "/Users/gztd-03-01457/Work/claude"
MEDIA_PROFILE_TABLE = "dsp_TQ.dsp_tp.media_profile_final"

HIGH_BAD_THRESHOLD = 10.0  # 高危广告主 bad 率阈值（%）

RISK_FIELDS = ['rl_final', 'rl_bundle', 'rl_bundle_downloads', 'rl_af_reject', 'rl_afPA_reject',
               'rl_imp_fraud', 'rl_click_fraud', 'rl_soigame', 'rl_anura',
               'rl_visible_imp', 'rl_unintent_click', 'rl_N_ctr']

RL_COLS = ['风险', 'bundle', 'bndl_dl', 'af_rej', 'af_pa', 'imp_f', 'clk_f', 'soigame', 'anura', 'vis_imp', 'unint_clk', 'n_ctr']


def fv(val):
    if val is None:
        return 'N/A'
    try:
        return str(int(float(val)))
    except Exception:
        return 'N/A'


def rl_final_fmt(v):
    if v is None:
        return 'N/A'
    try:
        vi = int(float(v))
    except Exception:
        return 'N/A'
    if vi == 0:   return f'{vi}✅'
    if vi <= 20:  return f'{vi}🟡'
    if vi <= 60:  return f'{vi}🟠'
    return f'{vi}🔴'


def rl_vals(row):
    return [
        rl_final_fmt(row.get('rl_final')),
        fv(row.get('rl_bundle')),
        fv(row.get('rl_bundle_downloads')),
        fv(row.get('rl_af_reject')),
        fv(row.get('rl_afPA_reject')),
        fv(row.get('rl_imp_fraud')),
        fv(row.get('rl_click_fraud')),
        fv(row.get('rl_soigame')),
        fv(row.get('rl_anura')),
        fv(row.get('rl_visible_imp')),
        fv(row.get('rl_unintent_click')),
        fv(row.get('rl_N_ctr')),
    ]


def md_table(headers, rows):
    lines = []
    lines.append('| ' + ' | '.join(str(h) for h in headers) + ' |')
    lines.append('|' + '|'.join(['------'] * len(headers)) + '|')
    for row in rows:
        lines.append('| ' + ' | '.join(str(v) for v in row) + ' |')
    return lines


def main():
    if len(sys.argv) > 1:
        dt = sys.argv[1]
        print(f"使用指定日期: {dt}")
    else:
        dt = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

    print(f"开始分析: {dt}")

    conn = pymysql.connect(host=SR_HOST, port=SR_PORT, user=SR_USER, password=SR_PASSWORD,
                           charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

    rl_select = ', '.join(f'mp.{f}' for f in RISK_FIELDS)

    with conn.cursor() as cursor:
        cursor.execute("SET CATALOG dsp_TQ")

        # ── 整体汇总 ──────────────────────────────────────────
        cursor.execute(f"""
        SELECT COUNT(*) AS total,
            SUM(CASE WHEN anura_result = 'bad' THEN 1 ELSE 0 END) AS bad,
            SUM(CASE WHEN anura_result = 'warn' THEN 1 ELSE 0 END) AS warn,
            SUM(CASE WHEN anura_result = 'good' THEN 1 ELSE 0 END) AS good,
            ROUND(SUM(CASE WHEN anura_result = 'bad' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS bad_rate
        FROM dsp_tp.anura_check_result WHERE dt = '{dt}'
        """)
        overall = cursor.fetchone()

        if not overall or overall['total'] == 0:
            print(f"❌ 暂无数据: {dt}")
            conn.close()
            return

        # ── 按 Affiliate 汇总 ─────────────────────────────────
        cursor.execute(f"""
        SELECT affiliate_id, COUNT(*) AS total,
            SUM(CASE WHEN anura_result = 'bad' THEN 1 ELSE 0 END) AS bad,
            SUM(CASE WHEN anura_result = 'warn' THEN 1 ELSE 0 END) AS warn,
            SUM(CASE WHEN anura_result = 'good' THEN 1 ELSE 0 END) AS good,
            ROUND(SUM(CASE WHEN anura_result = 'bad' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS bad_rate
        FROM dsp_tp.anura_check_result WHERE dt = '{dt}'
        GROUP BY affiliate_id ORDER BY bad DESC
        """)
        aff_rows = cursor.fetchall()

        # ── 按广告主汇总 ──────────────────────────────────────
        cursor.execute(f"""
        SELECT adv_id, COUNT(*) AS total,
            SUM(CASE WHEN anura_result = 'bad' THEN 1 ELSE 0 END) AS bad,
            SUM(CASE WHEN anura_result = 'warn' THEN 1 ELSE 0 END) AS warn,
            SUM(CASE WHEN anura_result = 'good' THEN 1 ELSE 0 END) AS good,
            ROUND(SUM(CASE WHEN anura_result = 'bad' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS bad_rate
        FROM dsp_tp.anura_check_result WHERE dt = '{dt}'
        GROUP BY adv_id ORDER BY bad DESC
        """)
        adv_rows = cursor.fetchall()

        # ── aff + bundle 明细（不 JOIN imp，避免大查询超限） ──
        print("查询 aff+bundle 明细（Top 200）...")
        cursor.execute("SET CATALOG dsp_TQ")
        cursor.execute(f"""
        SELECT
            affiliate_id,
            bundle,
            COUNT(*) AS total,
            SUM(CASE WHEN anura_result = 'bad'  THEN 1 ELSE 0 END) AS bad,
            SUM(CASE WHEN anura_result = 'warn' THEN 1 ELSE 0 END) AS warn,
            SUM(CASE WHEN anura_result = 'good' THEN 1 ELSE 0 END) AS good,
            ROUND(SUM(CASE WHEN anura_result = 'bad' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS bad_rate
        FROM dsp_tp.anura_check_result
        WHERE dt = '{dt}'
        GROUP BY affiliate_id, bundle
        HAVING SUM(CASE WHEN anura_result = 'bad' THEN 1 ELSE 0 END) > 0
        ORDER BY bad DESC
        LIMIT 200
        """)
        aff_bundle_rows = cursor.fetchall()

        # ── 高危广告主汇总（不再查询明细，避免大查询超限） ───────
        high_bad_advs = [r for r in adv_rows if r['bad_rate'] is not None and float(r['bad_rate']) >= HIGH_BAD_THRESHOLD]

    conn.close()

    # ── 生成报告 ──────────────────────────────────────────────
    lines = []
    lines.append(f"# Anura 检测报告 v2.1 — {dt}\n")

    lines.append("## 整体汇总\n")
    lines.append("| 指标 | 数值 |")
    lines.append("|------|------|")
    lines.append(f"| 总检测量 | {overall['total']:,} |")
    lines.append(f"| bad | {overall['bad']:,} |")
    lines.append(f"| warn | {overall['warn']:,} |")
    lines.append(f"| good | {overall['good']:,} |")
    lines.append(f"| bad 率 | {overall['bad_rate']}% |")
    lines.append("")

    # ── 按 Affiliate 汇总 ─────────────────────────────────────
    lines.append("## 按 Affiliate 汇总\n")
    lines.append("| affiliate_id | 总量 | bad | warn | good | bad率 |")
    lines.append("|-------------|-----:|----:|-----:|-----:|-------|")
    for r in aff_rows:
        flag = " **!!**" if r['bad_rate'] and float(r['bad_rate']) >= 30 else ""
        lines.append(f"| {r['affiliate_id']} | {r['total']:,} | {r['bad']:,} | {r['warn']:,} | {r['good']:,} | {r['bad_rate']}%{flag} |")
    lines.append("")

    # ── 按广告主汇总（行业分组） ──────────────────────────────
    industry_advs = {}
    for r in adv_rows:
        aid = int(r['adv_id']) if r['adv_id'] else 0
        info = ADV_INFO.get(aid, (str(aid), '未知'))
        name, industry = info if isinstance(info, tuple) else (info, '未知')
        if industry not in industry_advs:
            industry_advs[industry] = []
        industry_advs[industry].append({
            'aid': aid, 'name': name,
            'total': r['total'], 'bad': r['bad'], 'warn': r['warn'], 'good': r['good'],
            'bad_rate': r['bad_rate']
        })

    industry_summary = {}
    for ind, advs in industry_advs.items():
        total = sum(a['total'] for a in advs)
        bad   = sum(a['bad']   for a in advs)
        warn  = sum(a['warn']  for a in advs)
        good  = sum(a['good']  for a in advs)
        industry_summary[ind] = {
            'total': total, 'bad': bad, 'warn': warn, 'good': good,
            'bad_rate': round(bad / total * 100, 2) if total > 0 else 0
        }

    sorted_industries = sorted(industry_summary.items(), key=lambda x: x[1]['bad'], reverse=True)

    lines.append("## 按广告主汇总\n")
    lines.append("### 行业汇总\n")
    lines.append("| 行业 | 总量 | bad | warn | good | bad率 |")
    lines.append("|------|-----:|----:|-----:|-----:|-------|")
    for ind, s in sorted_industries:
        flag = " **!!**" if s['bad_rate'] >= 30 else ""
        lines.append(f"| {ind} | {s['total']:,} | {s['bad']:,} | {s['warn']:,} | {s['good']:,} | {s['bad_rate']}%{flag} |")
    lines.append("")

    for ind, _ in sorted_industries:
        advs = industry_advs[ind]
        lines.append(f"### {ind}\n")
        lines.append("| adv_id | 广告主 | 总量 | bad | warn | good | bad率 |")
        lines.append("|--------|--------|-----:|----:|-----:|-----:|-------|")
        for a in advs:
            flag = " **!!**" if a['bad_rate'] and float(a['bad_rate']) >= 30 else ""
            lines.append(f"| {a['aid']} | {a['name']} | {a['total']:,} | {a['bad']:,} | {a['warn']:,} | {a['good']:,} | {a['bad_rate']}%{flag} |")
        lines.append("")

    # ── aff + bundle 明细（Top 200） ──────────────────────
    lines.append("## Aff + Bundle 明细（bad > 0，Top 200）\n")
    if aff_bundle_rows:
        headers = ['aff', 'bundle', '总量', 'bad', 'warn', 'good', 'bad率']
        rows_data = []
        for r in aff_bundle_rows:
            rows_data.append([
                r['affiliate_id'], r['bundle'] or '-',
                f"{r['total']:,}", f"{r['bad']:,}", f"{r['warn']:,}", f"{r['good']:,}",
                f"{r['bad_rate']}%"
            ])
        lines.extend(md_table(headers, rows_data))
    else:
        lines.append("无数据")
    lines.append("")

    # ── 高危广告主汇总（bad率 ≥ 10%）──────────────────────
    lines.append(f"## 高危广告主汇总（bad率 ≥ {HIGH_BAD_THRESHOLD:.0f}%）\n")
    if high_bad_advs:
        lines.append(f"**共 {len(high_bad_advs)} 个广告主 bad 率 ≥ {HIGH_BAD_THRESHOLD:.0f}%**\n")
        lines.append("| adv_id | 广告主 | 总量 | bad | warn | good | bad率 |")
        lines.append("|--------|--------|-----:|----:|-----:|-----:|-------|")
        for r in high_bad_advs:
            aid = int(r['adv_id']) if r['adv_id'] else 0
            info = ADV_INFO.get(aid, (str(aid), '未知'))
            name = info[0] if isinstance(info, tuple) else info
            flag = " **!!**" if r['bad_rate'] >= 30 else ""
            lines.append(f"| {aid} | {name} | {r['total']:,} | {r['bad']:,} | {r['warn']:,} | {r['good']:,} | {r['bad_rate']}%{flag} |")
        lines.append("")
    else:
        lines.append(f"无广告主 bad 率 ≥ {HIGH_BAD_THRESHOLD:.0f}%\n")

    # ── 指标说明 ──────────────────────────────────────────────
    lines.append("## 指标说明\n")
    lines.append("> **高危阈值**: bad率 ≥ 10% 进入高危广告主汇总")
    lines.append("> **优化说明**: v2.1 移除大查询（不再 JOIN imp/media_profile），保留核心汇总表供快速巡检")
    lines.append("")

    report = "\n".join(lines)
    output_file = f"{OUTPUT_DIR}/anura_report_{dt}.md"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(report)

    print(f"✅ 报告已保存: {output_file}")
    print(f"   总检测量: {overall['total']:,}, bad率: {overall['bad_rate']}%, bundle: {len(aff_bundle_rows)}, aff: {len(aff_rows)}, adv: {len(adv_rows)}")
    print(f"   高危广告主: {len(high_bad_advs)} 个（bad率 ≥ {HIGH_BAD_THRESHOLD:.0f}%）")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        print(f"❌ 执行失败: {e}")
        traceback.print_exc()
        sys.exit(1)
