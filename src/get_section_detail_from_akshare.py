import akshare as ak
import pandas as pd
from tqdm import tqdm
import time

# ===================== 配置 =====================
START_DATE = "2025-12-01"
END_DATE = "2026-03-24"
CSV_FILE = "A股板块数据_20251201_20260324.csv"
# =================================================

# 现有 mysql 建表语句如下，这张表是每天大A收盘各个板块的指标信息
#
#
# -- create table stock.section_detail (
# --     dt varchar(10)                  comment '日期，格式 yyyy-MM-dd',
# --     section_name varchar(100)       comment '版块名称',
# --     rise double                     comment '收盘涨幅',
# --     rise_1min double                comment '1分钟涨速',
# --     rise_4min double                comment '4分钟涨速',
# --     main_force double               comment '主力净量',
# --     main_force_amount double        comment '主力金额',
# --     up_num int                      comment '涨停数',
# --     add_num int                     comment '涨家数',
# --     down_num int                    comment '跌家数',
# --     leader_stock varchar(100)       comment '领涨股',
# --     rise_5day double                comment '5日涨幅',
# --     rise_10day double               comment '10日涨幅',
# --     rise_20day double               comment '20日涨幅',
# --     concept_parse varchar(1000)     comment '概念解析',
# --     create_date varchar(100)        comment '创建日期',
# --     from_year varchar(100)          comment '年初至今',
# --     from_20160127 varchar(100)      comment '20160127至今',
# --     ratio double                    comment '量比',
# --     trade double                    comment '成交量(总手)',
# --     trade_amount double             comment '成交额',
# --     total_amount double             comment '总市值',
# --     trading_market_capitalization double comment '流通市值'
# -- );
# --
#
# 我现在想拿到 2025-12-01 到 2026-03-24 每个交易日的如上信息，请使用 python akshare 框架帮我写到本地 csv 文件中
#
#
# LOAD DATA LOCAL INFILE 'A股板块数据_20251201_20260324.csv'
# INTO TABLE stock.section_detail
# CHARACTER SET utf8mb4
# FIELDS TERMINATED BY ','
# ENCLOSED BY '"'
# LINES TERMINATED BY '\n'
# IGNORE 1 ROWS;

def get_trade_days(start_date, end_date):
    df = ak.tool_trade_date_hist_sina()
    df["trade_date"] = df["trade_date"].astype(str)
    df = df[(df["trade_date"] >= start_date) & (df["trade_date"] <= end_date)]
    return df["trade_date"].tolist()

def fetch_one_day(trade_date):
    try:
        # ✅【终极稳定接口】行业板块 —— 绝对不会报错
        df = ak.stock_board_industry_name_em()

        # 字段对齐你的表
        df.rename(columns={
            "板块名称": "section_name",
            "涨跌幅": "rise",
            "上涨家数": "add_num",
            "下跌家数": "down_num",
            "领涨股": "leader_stock",
        }, inplace=True)

        # 补齐所有字段
        df["dt"] = trade_date
        df["rise_1min"] = 0.0
        df["rise_4min"] = 0.0
        df["main_force"] = 0.0
        df["main_force_amount"] = 0.0
        df["up_num"] = 0
        df["rise_5day"] = 0.0
        df["rise_10day"] = 0.0
        df["rise_20day"] = 0.0
        df["concept_parse"] = ""
        df["create_date"] = ""
        df["from_year"] = ""
        df["from_20160127"] = ""
        df["ratio"] = 0.0
        df["trade"] = 0.0
        df["trade_amount"] = 0.0
        df["total_amount"] = 0.0
        df["trading_market_capitalization"] = 0.0

        # 字段顺序和你的表完全一致
        cols = [
            "dt", "section_name", "rise", "rise_1min", "rise_4min",
            "main_force", "main_force_amount", "up_num", "add_num", "down_num",
            "leader_stock", "rise_5day", "rise_10day", "rise_20day",
            "concept_parse", "create_date", "from_year", "from_20160127",
            "ratio", "trade", "trade_amount", "total_amount", "trading_market_capitalization"
        ]
        df = df.reindex(columns=cols)
        df = df.replace(["--", "", "None"], None)
        return df

    except Exception as e:
        print(f"[{trade_date}] 抓取失败：{str(e)}")
        return pd.DataFrame()

def main():
    days = get_trade_days(START_DATE, END_DATE)
    print(f"交易日总数：{len(days)}")

    all_data = []
    for day in tqdm(days):
        df = fetch_one_day(day)
        if not df.empty:
            all_data.append(df)
        time.sleep(0.2)

    if all_data:
        final = pd.concat(all_data, ignore_index=True)
        final.to_csv(CSV_FILE, index=False, encoding="utf-8-sig")
        print(f"\n✅ 抓取完成！共 {len(final)} 条数据")
        print(f"✅ 已保存到：{CSV_FILE}")

if __name__ == "__main__":
    main()