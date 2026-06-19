from datetime import datetime

import time
import pandas as pd
import mplfinance as mpf
import matplotlib.pyplot as plt
import warnings
from io import BytesIO
import base64
from sqlalchemy import create_engine, text
from functools import partial
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor

from src.utils import constants

# ====================== 【只改这里】MySQL 配置 ======================
MYSQL_HOST = constants.db_config['host']
MYSQL_USER = constants.db_config['user']
MYSQL_PASSWORD = constants.db_config['password']
MYSQL_DB = constants.db_config['database']
# ===================================================================

# -------------------- 屏蔽警告 + 加速配置 --------------------
warnings.filterwarnings("ignore")
plt.set_loglevel("error")
plt.rcParams['figure.max_open_warning'] = 0
plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']
plt.rcParams['axes.unicode_minus'] = False

# 数据库引擎
engine = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DB}?charset=utf8mb4"
)


# -------------------- 一次性加载所有K线数据 --------------------
def load_all_data():

    print("📥 加载所有 K 线数据...")
    # 表名替换为 stock_1days_down
    df_up = pd.read_sql("select * from stock_1days_down", engine)
    all_codes = df_up["code"].unique().tolist()
    placeholders = ",".join([f"'{c}'" for c in all_codes])

    sql_all_k = f"""
        select dt, code, price_open as Open, price_close as Close,
               price_highest as High, price_lowest as Low, trade_amount as Volume, rise
        from stock_detail
        where code in ({placeholders})
        order by code, dt asc
    """
    df_k_all = pd.read_sql(sql_all_k, engine)
    df_k_all["dt"] = pd.to_datetime(df_k_all["dt"])

    # 只保留最近 3 个月
    last_date = df_k_all["dt"].max()
    start_date = last_date - pd.DateOffset(months=3)
    df_k_all = df_k_all[df_k_all["dt"] >= start_date].copy()

    # 平盘 Open == Close → 变红
    df_k_all.loc[df_k_all["Close"] == df_k_all["Open"], "Close"] += 0.0001

    # 获取每只股票 最新收盘价 + 最新涨幅
    last_data = df_k_all.sort_values("dt").groupby("code").last()[["Close", "rise"]]

    price_map = last_data["Close"].round(2).to_dict()
    rise_map = (last_data["rise"]).round(2).to_dict()

    return df_up, df_k_all, price_map, rise_map


# -------------------- A股风格：涨红跌绿 --------------------
mc = mpf.make_marketcolors(
    up='r',
    down='g',
    edge='inherit',
    wick='inherit',
    volume='inherit'
)
s_style = mpf.make_mpf_style(marketcolors=mc, gridstyle='')


# -------------------- 极快绘图 --------------------
def fast_plot(df):
    try:
        fig, ax = mpf.plot(
            df, type="candle", volume=True, style=s_style,
            figratio=(10, 5), figscale=0.7,
            returnfig=True
        )
        buf = BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight", dpi=80)
        buf.seek(0)
        img = base64.b64encode(buf.read()).decode()
        plt.close(fig)
        return f"data:image/png;base64,{img}"
    except:
        return ""

def process_one(code, df_k_all):
    df = df_k_all[df_k_all["code"] == code].copy()
    if len(df) < 5:
        return code, ""
    df.set_index("dt", inplace=True)
    return code, fast_plot(df)

# -------------------- 并行绘图 --------------------
def generate_html(rise_10):

    df_up, df_k_all, price_map, rise_map = load_all_data()

    # 行业按股票数量从多到少排序
    industry_count = df_up["industry"].value_counts().sort_values(ascending=False)
    industries = industry_count.index.tolist()

    stock_image_map = {}
    print("🖼️ 开始批量绘图...")

    with ProcessPoolExecutor(max_workers=14) as executor:
        fn = partial(process_one, df_k_all=df_k_all)
        results = list(executor.map(fn, df_up["code"].unique()))

    for code, img in results:
        stock_image_map[code] = img

    print("🌍 生成HTML...")

    html = """
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <title>近 10 日涨幅居高且连续至少 1 天下跌股票 K 线图</title>
        <style>
            *{box-sizing:border-box;margin:0;padding:0;font-family:Microsoft YaHei}
            body{background:#f5f7fa;padding:20px}
            .container{max-width:1900px;margin:0 auto}
            .title{text-align:center;margin-bottom:20px}

            .col-switch{display:flex;gap:8px;justify-content:center;margin-bottom:20px}
            .col-btn{padding:10px 20px;border:none;border-radius:6px;background:#e3e6ed;cursor:pointer}
            .col-btn.active{background:#2f80ed;color:white}

            .tab-wrap{background:white;padding:15px;border-radius:10px;margin-bottom:20px}
            .tabs{display:flex;gap:8px;flex-wrap:wrap}
            .tab{padding:8px 16px;background:#f1f3f5;border:0;border-radius:6px;cursor:pointer}
            .tab.active{background:#2f80ed;color:white}

            .tab-content{display:none;grid-template-columns:repeat(3,1fr);gap:16px}
            .tab-content.active{display:grid}

            .card{background:white;padding:12px;border-radius:12px}
            .card img{width:100%;border-radius:8px;margin-top:10px}
            .stock-title{font-weight:bold}
            .price{color:#e63946;font-size:14px;margin-left:6px}
            .rise-green{color:#28a745;font-size:14px;margin-left:4px}
            .rise-red{color:#e63946;font-size:14px;margin-left:4px}
            .sub{font-size:12px;color:#888;margin-top:4px}
        </style>
    </head>
    <body>
        <div class="container">
            <h1 class="title">📉 近 10 日涨幅居高 + 连续至少 1 天下跌股票 K 线看板</h1>

            <div class="col-switch">
                <button class="col-btn" onclick="changeColumns(2)">2列</button>
                <button class="col-btn active" onclick="changeColumns(3)">3列</button>
                <button class="col-btn" onclick="changeColumns(4)">4列</button>
                <button class="col-btn" onclick="changeColumns(5)">5列</button>
            </div>

            <div class="tab-wrap">
                <div class="tabs">
    """

    # 行业 TAB（带数量）
    for i, ind in enumerate(industries):
        count = industry_count[ind]
        active = "active" if i == 0 else ""
        html += f'<button class="tab {active}" onclick="setTab({i})">{ind}({count})</button>'

    html += '</div></div>'

    # 行业内容
    for i, ind in enumerate(industries):
        show = "active" if i == 0 else ""
        html += f'<div class="tab-content {show}">'
        for _, r in df_up[df_up["industry"] == ind].iterrows():
            code = r["code"]
            img = stock_image_map.get(code, "")
            if not img: continue

            price = price_map.get(code, "")
            rise_val = rise_map.get(code, 0.00)

            price_str = f"({price} 元)" if price else ""
            rise_cls = "rise-red" if rise_val >= 0 else "rise-green"
            rise_str = f'<span class="{rise_cls}">{rise_val:+.2f}%</span>'

            title_html = f'{code} {r["stock_name"]}<span class="price">{price_str}</span>{rise_str}'

            html += f'''
            <div class="card">
                <div class="stock-title">{title_html}</div>
                <div class="sub">{r["industry_detail"]} | 连续{r["number_of_consecutive_days"]}天收阴，近10日涨幅超{rise_10 * 100}%</div>
                <img src="{img}">
            </div>
            '''
        html += "</div>"

    html += '''
        <script>
            function changeColumns(col) {
                let grids = document.querySelectorAll('.tab-content');
                grids.forEach(g => {
                    g.style.gridTemplateColumns = `repeat(${col}, 1fr)`;
                });
                document.querySelectorAll('.col-btn').forEach((btn,i) => {
                    btn.classList.toggle('active', parseInt(btn.innerText[0]) === col);
                });
            }

            function setTab(i){
                document.querySelectorAll('.tab-content').forEach((e,j)=>{
                    e.classList.toggle('active',j==i)
                    document.querySelectorAll('.tab')[j].classList.toggle('active',j==i)
                });
            }
        </script>
    </body></html>
    '''

    # 输出文件名同步修改
    with open(f"""../html/{datetime.now().strftime("%Y-%m-%d")}_近 10 日涨幅超 {rise_10 * 100}% 连续 1 天下跌股票 K 线图.html""", "w", encoding="utf-8") as f:
        f.write(html)

    print(f"✅ 完成！文件已生成：近10日涨幅超 {rise_10 * 100} % 连续 1 天下跌股票 K 线图.html")

def update_stock_1days_down(today, rise_10):

    # 1. 清空表 stock_1days_down
    sql_truncate = "truncate table stock_1days_down;"

    # 2. 核心筛选SQL：近10日涨幅>rise_10 + 连续 ≥1 根阴线且连续到今日
    sql_insert = f'''
    insert into stock.stock_1days_down
    -- 第一层：源头过滤，剔除收盘价/开盘价任意为空的无效行情
    with all_k_rn as (
        select
            dt,
            code,
            stock_name,
            price_close,
            price_open,
            row_number() over(partition by code order by dt desc) as rn_desc,
            case when price_close < price_open then 1 else 0 end as is_down
        from stock_detail
        where dt>='2026-03-02'
          and code not like '688%'
          -- 关键：过滤价格为空的残缺K线 
          and price_close is not null
          and price_open is not null
    ),
    latest_10 as (
        select * from all_k_rn where rn_desc <= 10
    ),
    -- 第二层：校验每只票必须凑齐 10 根完整有效K线，不足 10 根直接丢弃
    stock_10rise as (
        select
            code,
            max(stock_name) stock_name,
            count(*) as valid_line_cnt,
            max(case when rn_desc=1 then price_close end) close_now,
            max(case when rn_desc=10 then price_close end) close_10ago,
            max(case when rn_desc<=3 then price_close end) high_recent3
        from latest_10
        group by code
        having close_10ago > 0
           -- 必须刚好 10 根完整有效行情，有空行被过滤后不足 10 根的票直接淘汰
           and valid_line_cnt = 10
    ),
    rise_qualified as (
        select
            *,
            close_now / close_10ago - 1 as rise_10day,
            close_now / high_recent3 as pullback_rate
        from stock_10rise
        where 
            -- 10 日涨幅阈值
            (close_now / close_10ago - 1) > {rise_10}
            -- 现价高于10日前起点，整体区间上涨
            and close_now > close_10ago
    ),
    stock_all_line as (
        select * from all_k_rn
        where code in (select code from rise_qualified)
    ),
    step2 as (
        select
            *,
            row_number() over (partition by code order by dt) as rn,
            row_number() over (partition by code, is_down order by dt) as rn_down
        from stock_all_line
    ),
    step3 as (
        select
            code,
            max(stock_name) as stock_name,
            count(*) as number_of_consecutive_days,
            max(dt) as end_dt
        from step2
        where is_down = 1
        group by code, rn - rn_down
        having count(*) >= 1
           and max(dt) = '{today}'
    ),
    final_result as (
        select
            s3.code,
            s3.stock_name,
            s3.number_of_consecutive_days,
            dst.industry,
            dst.industry_detail
        from step3 s3
        left join dim_stock_tag dst
            on s3.code = replace(replace(lower(dst.code), 'sz', ''), 'sh', '')
    )
    select * from final_result
    order by number_of_consecutive_days desc;
    '''

    # 执行SQL
    with engine.connect() as conn:
        print("正在清空表 stock_1days_down...")
        conn.execute(text(sql_truncate))

        print(f"正在插入【近10日涨幅超 {rise_10 * 100}%+ 连续至少 1 天收阴】股票数据...")
        conn.execute(text(sql_insert))

        conn.commit()

    print("✅ 两条SQL执行完成！")

if __name__ == "__main__":

    start_time = time.time()

    # today = datetime.now().strftime("%Y-%m-%d")
    today = '2026-06-18'

    # rise_10 表示最近 10 个交易日涨幅
    rise_10 = 0.1
    update_stock_1days_down(today, rise_10)

    generate_html(rise_10)

    end_time = time.time()
    cost_time = end_time - start_time

    print(f"程序总耗时：{cost_time:.2f} 秒")