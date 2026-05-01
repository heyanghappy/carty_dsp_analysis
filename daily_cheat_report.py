#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日自动作弊分析报告
每天早上10点执行，分析最近7天的广告主作弊情况，生成报告并发送到飞书
"""

import pymysql
import pandas as pd
from datetime import datetime, timedelta
import requests
import json
import sys

# 广告主分组配置（来源：DSP客户分小组标签.xlsx）
ADV_GROUPS = {
    'SKA': [12, 40, 52, 62, 68, 152, 260, 341, 556, 583, 611, 626, 628, 653, 692, 699, 710, 715, 732, 754, 765, 785, 825, 851],
    'DF-APP': [585, 607, 623, 635, 636, 637, 638, 640, 644, 645, 646, 656, 660, 662, 664, 667, 681, 684, 686, 687, 691, 693, 694, 695, 697, 702, 703, 704, 705, 706, 707, 708, 709, 711, 712, 714, 720, 724, 725, 726, 728, 731, 734, 735, 736, 737, 738, 740, 741, 743, 744, 745, 746, 747, 748, 749, 752, 757, 773, 774, 775, 776, 777, 778, 786, 830, 834, 847, 848, 850, 852],
    'DF-web': [2, 587, 588, 591, 600, 609, 612, 632, 633, 647, 651, 654, 661, 688, 689, 700, 701, 722, 723, 727, 729, 730, 750, 751, 755, 758, 759, 762, 763, 764, 767, 783, 794, 806, 829, 833],
    'IAA-Game': [82, 92, 274, 301, 530, 624, 657, 671, 673, 756, 760, 761, 766, 768, 769, 770, 771, 779, 780, 781, 782, 787, 788, 789, 790, 791, 792, 795, 796, 797, 798, 799, 800, 801, 803, 804, 805, 808, 824, 826, 831, 832, 835, 845],
}

# 广告主行业映射（adv_id -> (adv_name, industry)）
ADV_INFO = {
    # SKA
    12: ('lazada-CPS', '头部电商'), 40: ('Trip-CPS', '头部电商'), 52: ('shopee-CPS', '头部电商'),
    62: ('TTS-CPS', '头部电商'), 68: ('Shein-CPS', '头部电商'), 152: ('SheinRTA', '头部电商'),
    260: ('Opay-RTA', '金融'), 341: ('sportmaster-RT-RU', '头部电商'), 556: ('Adeliver Opay RTA', '金融'),
    583: ('Shopee-RTA-RTG', '头部电商'), 611: ('TTS-Local-CPS', '头部电商'), 626: ('Noon', '头部电商'),
    628: ('DHgate-CPO-RT', '头部电商'), 653: ('Lazada-Weave-CPS', '头部电商'), 692: ('hifami-RT', '未知'),
    699: ('coupang-CPS', '平台电商'), 710: ('tokoRT', '头部电商'), 715: ('PalmPay-RT', '未知'),
    732: ('Shahid-RT', '未知'), 754: ('Tokopedia-CPS', '头部电商'), 765: ('Trendyol-RT', '头部电商'),
    785: ('Hunting Sniper-RT-US', '游戏'), 825: ('Lazada-RTA-CPS', '头部电商'), 851: ('HiggsDomino-IDN-RT', '游戏'),
    # DF-APP
    585: ('上海极邑网络科技有限公司-DF-Nutra', 'nutra'), 607: ('CMY', 'hot sku'), 623: ('Blitzads-DF-Nutra', 'nutra'),
    635: ('趣瑞-杀毒', '杀毒'), 636: ('趣瑞-nutra', 'nutra'), 637: ('尔多账户2(备用户)', 'hot sku'),
    638: ('尔多(备用户)', 'hot sku'), 640: ('Adeliver-PAC代投', 'soi,杀毒（campaign1555）'), 644: ('趣瑞-soi', 'soi'),
    645: ('趣瑞-auto', '保险'), 646: ('Green Bamboo 3 - APP Traffic', '未知'), 656: ('尔多2', 'hot sku'),
    660: ('Surmobi-DF-Ecom-APP', 'hot sku'), 662: ('泽昊天辰-APP', '未知'), 664: ('FrontStory-APP', '未知'),
    667: ('北京吾游', 'soi'), 681: ('趣瑞-richhitdaily.top', '杀毒'), 684: ('mobplus-m.m2888.net', 'soi'),
    686: ('Pushads', 'soi'), 687: ('尔多-alen(备用户)', 'hot sku'), 691: ('Yoyu-SOI-track.goldwinnerprizes', 'soi'),
    693: ('CMY-WH&ML-0002', '未知'), 694: ('ADZ-SOI-trendndaily_CP', 'soi'), 695: ('CMY-WH&ML-0002 (洁牙)', 'hot sku'),
    697: ('cmy-WH&ML-0004 （备用户）', 'hot sku'), 702: ('ntpop', 'soi'), 703: ('total AV-作废', '杀毒'),
    704: ('Nutra Lead_CPL_adeliver', 'nutra'), 705: ('Webvork_CPL_adeliver', 'nutra'), 706: ('Pushnami_CPL_adeliver', 'soi'),
    707: ('兴宇-nutra', 'nutra'), 708: ('MeviusAds-SOI', 'soi'), 709: ('Flatiron_CPL_adeliver', 'soi'),
    711: ('Proleagion_CPL_adeliver', 'soi'), 712: ('Fluent_CPL_adeliver', 'soi'), 714: ('BlitzAds-GLP-1_CPL', 'GLP'),
    720: ('MeviusAds-Nutra', 'nutra'), 724: ('Yoyu-杀毒-自投_CPL', '杀毒'), 725: ('Yoyu-SOI-自投_CPL', 'soi'),
    726: ('LeadgenX-SOI_CPL', 'soi'), 728: ('Sinrong-Nutra-CPL', 'nutra'), 731: ('Total Security', '杀毒'),
    734: ('ILS_CPL_adeliver', '保险'), 735: ('趣瑞-GLP-1_CPL', 'GLP'), 736: ('mobplus-杀毒_CPL', '杀毒'),
    737: ('Yoyu-装修-自投_CPL', '保险'), 738: ('Yoyu-保险-自投_CPL', '保险'), 740: ('mobplus-保险_CPL', '保险'),
    741: ('Medvi_CPL_adeliver', '保险'), 743: ('Yoyu-SOI-自投2_CPL', 'soi'), 744: ('mobplus-SOI_CPL-自投1', 'soi'),
    745: ('mobplus-SOI_CPL-自投2', 'soi'), 746: ('mobplus-杀毒_CPL-自投1', '杀毒'), 747: ('mobplus-杀毒_CPL-自投2', '杀毒'),
    748: ('LeadgenX-保险_CPL', '保险'), 749: ('Yoyu-保险-自投2_CPL', '保险'), 752: ('Yoyu-PPCall-自投_CPL', '保险'),
    757: ('TU-test-SOI-CPL', 'soi'), 773: ('大魔投-杀毒-自投_CPL', '杀毒'), 774: ('大魔投-保险-自投_CPL', '保险'),
    775: ('大魔投-减肥-自投_CPL', 'nutra'), 776: ('大魔投-SOI-自投_CPL', 'soi'), 777: ('IGOR-test-SOI-CPL', 'soi'),
    778: ('FOX-test-SOI-CPL', 'soi'), 786: ('Yoyu-债务-自投1_CPL', '保险'), 830: ('Yoyu-mmp-自投1_CPL', '保险'),
    834: ('ADZ-保险_CPL', '保险'), 847: ('Martell-PPC-Medicare', '保险'), 848: ('Adsing-ppc-FE', '保险'),
    850: ('Spektrafin-ppc-FE', '保险'), 852: ('Kuyami', '保险'),
    # DF-web
    2: ('AE-RTA-Selfdsp', '平台电商'), 587: ('创象1', '金融'), 588: ('创象2', '金融'),
    591: ('Green Bamboo', '金融'), 600: ('Surmobi-DF-Ecom', '白牌电商'), 609: ('PY Digital', '代理中间页'),
    612: ('FrontStory', '新闻资讯'), 632: ('景观-hyy', '白牌电商'), 633: ('景观-dyx', '白牌电商'),
    647: ('Green Bamboo 4 - Nerve', '电商'), 651: ('景观-dyx02', '白牌电商'), 654: ('Hale-J', '金融'),
    661: ('泽昊天辰-Web', '白牌电商'), 688: ('xinze-web', '白牌电商'), 689: ('xinze-app', '否'),
    700: ('xinze-web2', '广告代理商'), 701: ('xinze-app2', '是'), 722: ('xinze03 web+app', '白牌电商'),
    723: ('xinze06-二线', '白牌电商'), 727: ('アンカー株式会社', '白牌电商'), 729: ('Smart Asset', '金融'),
    730: ('Gameaddik', '游戏'), 750: ('Peak Performance', '金融'), 751: ('Version Two', '金融'),
    755: ('xinze-Google', '电商'), 758: ('xinze-Google-02', '电商'), 759: ('xinze-Google-03', '电商'),
    762: ('original nutrition', '电商'), 763: ('wonderbody', '电商'), 764: ('BlitzAds - Carty', '电商'),
    767: ('BeDigital', '未知'), 783: ('BlitzAds - Carty-02', '电商'), 794: ('Gameaddik-CPA', '游戏'),
    806: ('RM42_Carty', '游戏'), 829: ('web it', '金融'), 833: ('云际智推', '电商'),
    # IAA-Game
    82: ('82', '未知'), 92: ('92', '未知'), 274: ('274', '未知'), 301: ('301', '未知'), 530: ('530', '未知'),
    624: ('Capcut-CPI', 'APP'), 657: ('Tidy Master-CPI', 'Game'), 671: ('Bubble Shooter-CPI', 'Game'),
    673: ('Club Vegas-CPI', 'Game'), 756: ('Satistory: Tidy up', 'Game'), 760: ('Find it all-CPI', 'Game'),
    761: ('Magic Tiles-CPI', 'Game'), 766: ('Fizzo', 'APP'), 768: ('Balls Bounce-CPI', 'Game'),
    769: ('Bus Escape', 'Game'), 770: ('Monsters Gang', 'Game'), 771: ('Mechange', 'Game'),
    779: ('779', '未知'), 780: ('Magic Jigsaw Puzzles', 'Game'), 781: ('Domino', 'Game'),
    782: ('782', '未知'), 787: ('Mahjong Epic', 'Game'), 788: ('788', '未知'),
    789: ('Bricks Legend', 'Game'), 790: ('Vizor Gold Miners', 'Game'), 791: ('791', '未知'),
    792: ('Train Miner', 'Game'), 795: ('795', '未知'), 796: ('796', '未知'), 797: ('797', '未知'),
    798: ('798', '未知'), 799: ('799', '未知'), 800: ('800', '未知'), 801: ('801', '未知'),
    803: ('803', '未知'), 804: ('804', '未知'), 805: ('805', '未知'), 808: ('808', '未知'),
    824: ('824', '未知'), 826: ('826', '未知'), 831: ('831', '未知'), 832: ('832', '未知'), 835: ('835', '未知'),
    845: ('845', '未知'),
}

# StarRocks配置
SR_HOST = "fe-c-907795efe3201917.starrocks.aliyuncs.com"
SR_PORT = 9030
SR_USER = "sandy"
SR_PASSWORD = "MXeptLkEkoi2$FMX"

# 飞书配置
FEISHU_APP_ID = "cli_a912ec6b53f8dcc1"
FEISHU_APP_SECRET = "WhxP812QPaXW5HoVepuLUdkeb7ETzBoC"
FEISHU_OPEN_ID = "ou_02adad263d6f1d91a66e00367d3b8567"


def query_cheat(conn, table, start_dt, end_dt, adv_ids, batch_size=3):
    """查询作弊统计，分批查询避免超时，包含总日志数、唯一bid数、作弊bid数"""
    all_rows = []
    
    # 按天查询，减少单次查询数据量
    from datetime import datetime, timedelta
    start_date = datetime.strptime(start_dt, '%Y%m%d')
    end_date = datetime.strptime(end_dt, '%Y%m%d')
    
    current_date = start_date
    while current_date <= end_date:
        dt_str = current_date.strftime('%Y%m%d')
        
        for i in range(0, len(adv_ids), batch_size):
            batch = adv_ids[i:i+batch_size]
            placeholders = ','.join([f"'{x}'" for x in batch])
            sql = f"""
            SELECT adv_id,
                COUNT(*) AS total_logs,
                COUNT(DISTINCT bid_id) AS distinct_bids,
                COUNT(DISTINCT CASE WHEN cheat = 'true' THEN bid_id END) AS cheat_bids
            FROM {table}
            WHERE dt = '{dt_str}' 
              AND adv_id IN ({placeholders})
              AND (cheat = 'true' OR cheat = 'false')  -- 只查询有cheat标记的记录
            GROUP BY adv_id
            """
            try:
                with conn.cursor() as cursor:
                    cursor.execute(sql)
                    rows = cursor.fetchall()
                if rows:
                    all_rows.extend(rows)
                    print(f"  ✓ {table} dt={dt_str} advs={batch[0]}..{batch[-1]}: {len(rows)}条")
            except Exception as e:
                print(f"  ✗ 查询 {table} dt={dt_str} advs={batch[0]}..{batch[-1]} 失败: {e}")
                # 继续查询其他批次
        
        current_date += timedelta(days=1)
    
    if not all_rows:
        return pd.DataFrame(columns=['adv_id','total_logs','distinct_bids','cheat_bids'])
    
    df = pd.DataFrame(all_rows)
    # 按adv_id聚合多天数据
    df = df.groupby('adv_id', as_index=False).sum()
    return df


def send_to_feishu(title, content):
    """发送消息到飞书，内容过长时分多条发送"""
    # 获取token
    token_resp = requests.post(
        "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
        json={"app_id": FEISHU_APP_ID, "app_secret": FEISHU_APP_SECRET}
    )
    token = token_resp.json()["tenant_access_token"]

    # 按段落拆分，每段不超过2800字符
    chunks = []
    current = ""
    for line in content.split('\n'):
        if len(current) + len(line) + 1 > 2800:
            chunks.append(current)
            current = line
        else:
            current = current + '\n' + line if current else line
    if current:
        chunks.append(current)

    success = True
    for i, chunk in enumerate(chunks):
        part = f" ({i+1}/{len(chunks)})" if len(chunks) > 1 else ""
        card_content = {
            "config": {"wide_screen_mode": True},
            "header": {"title": {"tag": "plain_text", "content": f"{title}{part}"}, "template": "blue"},
            "elements": [{"tag": "markdown", "content": f"```\n{chunk}\n```"}]
        }

        resp = requests.post(
            "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=open_id",
            headers={"Authorization": f"Bearer {token}"},
            json={"receive_id": FEISHU_OPEN_ID, "msg_type": "interactive",
                  "content": json.dumps(card_content)}
        )
        if resp.json().get("code") != 0:
            success = False
    return success


def main():
    # 计算日期范围（只查询昨天，避免CPU超限）
    if len(sys.argv) > 1:
        # 命令行指定日期
        target_dt = sys.argv[1]
        start_dt = target_dt
        end_dt = target_dt
        print(f"开始分析: {start_dt} (指定日期)")
    else:
        # 默认只查询昨天
        end_date = datetime.now() - timedelta(days=1)
        start_date = end_date  # 只查一天
        start_dt = start_date.strftime('%Y%m%d')
        end_dt = end_date.strftime('%Y%m%d')
        print(f"开始分析: {start_dt} (昨天)")

    # 连接StarRocks
    conn = pymysql.connect(host=SR_HOST, port=SR_PORT, user=SR_USER, password=SR_PASSWORD,
                          charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

    summary_list = []
    all_data = {}

    # 分组统计
    for group, adv_ids in ADV_GROUPS.items():
        adv_ids_str = [str(i) for i in adv_ids]
        imp_df = query_cheat(conn, 'assembly.dsp.ods_dsp_imp', start_dt, end_dt, adv_ids_str)
        click_df = query_cheat(conn, 'assembly.dsp.ods_dsp_click', start_dt, end_dt, adv_ids_str)

        merged = pd.merge(imp_df, click_df, on='adv_id', how='outer', suffixes=('_imp','_click')).fillna(0)

        # 曝光统计
        merged['imp_total_logs'] = merged['total_logs_imp'].astype(int)
        merged['imp_distinct_bids'] = merged['distinct_bids_imp'].astype(int)
        merged['imp_cheat_bids'] = merged['cheat_bids_imp'].astype(int)
        merged['imp_cheat_rate_percent'] = (merged['imp_cheat_bids'] / merged['imp_distinct_bids'].replace(0,1) * 100).round(2)

        # 点击统计
        merged['click_total_logs'] = merged['total_logs_click'].astype(int)
        merged['click_distinct_bids'] = merged['distinct_bids_click'].astype(int)
        merged['click_cheat_bids'] = merged['cheat_bids_click'].astype(int)
        merged['click_cheat_rate_percent'] = (merged['click_cheat_bids'] / merged['click_distinct_bids'].replace(0,1) * 100).round(2)

        # 总体统计
        merged['total_logs'] = merged['imp_total_logs'] + merged['click_total_logs']
        merged['total_distinct_bids'] = merged['imp_distinct_bids'] + merged['click_distinct_bids']
        merged['total_cheat_bids'] = merged['imp_cheat_bids'] + merged['click_cheat_bids']
        merged['total_cheat_rate_percent'] = (merged['total_cheat_bids'] / merged['total_distinct_bids'].replace(0,1) * 100).round(2)

        all_data[group] = merged

        # 汇总统计
        s_imp_logs = merged['imp_total_logs'].sum()
        s_imp_bids = merged['imp_distinct_bids'].sum()
        s_imp_cheat = merged['imp_cheat_bids'].sum()
        s_clk_logs = merged['click_total_logs'].sum()
        s_clk_bids = merged['click_distinct_bids'].sum()
        s_clk_cheat = merged['click_cheat_bids'].sum()
        s_total_logs = s_imp_logs + s_clk_logs
        s_total_bids = s_imp_bids + s_clk_bids
        s_total_cheat = s_imp_cheat + s_clk_cheat

        summary_list.append({
            'group': group,
            'imp_total_logs': s_imp_logs,
            'imp_distinct_bids': s_imp_bids,
            'imp_cheat_bids': s_imp_cheat,
            'imp_cheat_rate': round(s_imp_cheat/max(s_imp_bids,1)*100, 2),
            'click_total_logs': s_clk_logs,
            'click_distinct_bids': s_clk_bids,
            'click_cheat_bids': s_clk_cheat,
            'click_cheat_rate': round(s_clk_cheat/max(s_clk_bids,1)*100, 2),
            'total_logs': s_total_logs,
            'total_distinct_bids': s_total_bids,
            'total_cheat_bids': s_total_cheat,
            'total_cheat_rate': round(s_total_cheat/max(s_total_bids,1)*100, 2),
        })

    conn.close()

    # 生成报告（Markdown格式）
    report_lines = []
    report_lines.append(f"# 广告主作弊分析日报 {start_dt}~{end_dt}\n")

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
    filename = f"/Users/gztd-03-01457/Work/claude/data_log/daily_cheat_report_{end_dt}.md"
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(report)
    print(f"✅ 报告已保存: {filename}")

    # 发送到飞书
    if send_to_feishu(f"📊 作弊分析日报 {start_dt}~{end_dt}", report):
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
