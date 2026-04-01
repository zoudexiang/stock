from datetime import datetime

import pandas as pd
import mplfinance as mpf
import matplotlib.pyplot as plt
import warnings
from io import BytesIO
import base64
from sqlalchemy import create_engine, text
from concurrent.futures import ThreadPoolExecutor

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
    print("📥 加载所有K线数据...")
    df_up = pd.read_sql("SELECT * FROM stock_3days_up", engine)
    all_codes = df_up["code"].unique().tolist()
    placeholders = ",".join([f"'{c}'" for c in all_codes])

    sql_all_k = f"""
        SELECT dt, code, price_open AS Open, price_close AS Close,
               price_highest AS High, price_lowest AS Low, trade_amount AS Volume, rise
        FROM stock_detail
        WHERE code IN ({placeholders})
        ORDER BY code, dt ASC
    """
    df_k_all = pd.read_sql(sql_all_k, engine)
    df_k_all["dt"] = pd.to_datetime(df_k_all["dt"])

    # 只保留最近 2 个月
    last_date = df_k_all["dt"].max()
    start_date = last_date - pd.DateOffset(months=3)
    df_k_all = df_k_all[df_k_all["dt"] >= start_date].copy()

    # ====================== ✅ 强制修复：平盘 Open == Close → 变红 ======================
    df_k_all.loc[df_k_all["Close"] == df_k_all["Open"], "Close"] += 0.0001

    # 获取每只股票 最新收盘价 + 最新涨幅
    last_data = df_k_all.sort_values("dt").groupby("code").last()[["Close", "rise"]]

    # ✅ 正确计算涨幅：rise * 100 转为百分比，并保留2位小数
    price_map = last_data["Close"].round(2).to_dict()
    rise_map = (last_data["rise"] * 100).round(2).to_dict()

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


# -------------------- 并行绘图 --------------------
def generate_html():
    df_up, df_k_all, price_map, rise_map = load_all_data()

    # 行业按股票数量从多到少排序
    industry_count = df_up["industry"].value_counts().sort_values(ascending=False)
    industries = industry_count.index.tolist()

    stock_image_map = {}
    print("🖼️ 开始批量绘图...")

    def process_one(code):
        df = df_k_all[df_k_all["code"] == code].copy()
        if len(df) < 5:
            return code, ""
        df.set_index("dt", inplace=True)
        return code, fast_plot(df)

    with ThreadPoolExecutor(max_workers=6) as executor:
        results = list(executor.map(process_one, df_up["code"].unique()))

    for code, img in results:
        stock_image_map[code] = img

    print("🌍 生成HTML...")

    html = '''
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <title>连续3天上涨股票 K线图</title>
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
            <h1 class="title">📈 连续3天上涨股票 K线看板</h1>

            <div class="col-switch">
                <button class="col-btn" onclick="changeColumns(2)">2列</button>
                <button class="col-btn active" onclick="changeColumns(3)">3列</button>
                <button class="col-btn" onclick="changeColumns(4)">4列</button>
                <button class="col-btn" onclick="changeColumns(5)">5列</button>
            </div>

            <div class="tab-wrap">
                <div class="tabs">
    '''

    # 行业TAB（带数量）
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

            # ====================== ✅ 最终正确显示：价格 + 涨幅 ======================
            price = price_map.get(code, "")
            rise_val = rise_map.get(code, 0.00)

            price_str = f"({price} 元)" if price else ""
            rise_cls = "rise-red" if rise_val >= 0 else "rise-green"
            rise_str = f'<span class="{rise_cls}">{rise_val:+.2f}%</span>'

            title_html = f'{code} {r["stock_name"]}<span class="price">{price_str}</span>{rise_str}'

            html += f'''
            <div class="card">
                <div class="stock-title">{title_html}</div>
                <div class="sub">{r["industry_detail"]} | 连续{r["number_of_consecutive_days"]}天</div>
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

    with open(f"""../html/{datetime.now().strftime("%Y-%m-%d")}_连续3天上涨股票K线图.html""", "w", encoding="utf-8") as f:
        f.write(html)

    print("✅ 完成！文件已生成：连续3天上涨股票K线图.html")


def update_stock_3days_up(today):
    # 1. 清空表
    sql_truncate = "TRUNCATE TABLE stock_3days_up;"

    # 2. 插入数据（你的完整SQL）
    sql_insert = f"""
    insert into stock.stock_3days_up
    with step1 as (
        select
            dt,
            code,
            stock_name,
            price_close,
            price_open,
            case when price_close >= price_open then 1 else 0 end as is_up
        from stock_detail
        where dt>='2026-03-02'
            and code not like '688%%'
            and upper(stock_name) not like 'ST'
    ),
    step2 as (
        select
            *,
            row_number() over (partition by code order by dt) as rn,
            row_number() over (partition by code, is_up order by dt) as rn_up
        from step1
    ),
    step3 as (
        select
            code,
            max(stock_name) as stock_name,
            count(*) as number_of_consecutive_days,
            max(dt) as end_dt
        from step2
        where is_up = 1
        group by code, rn - rn_up
        having count(*) >= 3
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
    """

    # ========== 执行 SQL ==========
    with engine.connect() as conn:
        print("正在清空表 stock_3days_up...")
        conn.execute(text(sql_truncate))

        print("正在插入连续3天上涨股票数据...")
        conn.execute(text(sql_insert))

        conn.commit()  # 提交事务（必须加！）

    print("✅ 两条SQL执行完成！")

if __name__ == "__main__":

    today = datetime.now().strftime("%Y-%m-%d")

    update_stock_3days_up(today)

    generate_html()