from datetime import datetime
import time
import pandas as pd
import mplfinance as mpf
import matplotlib.pyplot as plt
import warnings
from io import BytesIO
import base64
from sqlalchemy import create_engine, text
from concurrent.futures import ThreadPoolExecutor

from src.utils import constants

# ====================== MySQL 配置 ======================
MYSQL_HOST = constants.db_config['host']
MYSQL_USER = constants.db_config['user']
MYSQL_PASSWORD = constants.db_config['password']
MYSQL_DB = constants.db_config['database']
# =======================================================

warnings.filterwarnings("ignore")
plt.set_loglevel("error")
plt.rcParams['figure.max_open_warning'] = 0
plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']
plt.rcParams['axes.unicode_minus'] = False

engine = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DB}?charset=utf8mb4"
)

# A股 涨红跌绿样式
mc = mpf.make_marketcolors(
    up='r',
    down='g',
    edge='inherit',
    wick='inherit',
    volume='inherit'
)
s_style = mpf.make_mpf_style(marketcolors=mc, gridstyle='')


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
    except Exception:
        return ""


def generate_near_10low_html():
    """
    规则：最近10个交易日最低点，与最新收盘价对比，价格差在±3%以内
    板块内部：价格差绝对值越接近0，排序越靠前
    """
    print("📥 加载10日低点与最新收盘价价差±3%以内个股数据...")
    last_dt_df = pd.read_sql("SELECT MAX(dt) AS dt FROM stock_detail", engine)
    last_dt = last_dt_df.iloc[0]["dt"]

    # 获取最近10个交易日每只股票的最低价
    sql_10low = f"""
    SELECT code, MIN(price_lowest) as low_10d
    FROM stock_detail
    WHERE dt >= DATE_SUB('{last_dt}', INTERVAL 10 DAY)
    GROUP BY code
    """
    df_10low = pd.read_sql(sql_10low, engine)

    # 获取最新交易日基础数据，关联行业
    sql_latest = f"""
    SELECT
        s.code,
        s.stock_name,
        s.price_close,
        s.rise,
        dst.industry,
        dst.industry_detail
    FROM stock_detail s
    LEFT JOIN dim_stock_tag dst
        ON REPLACE(REPLACE(LOWER(dst.code), 'sz', ''), 'sh', '') = s.code
    WHERE s.dt = '{last_dt}'
      AND s.code NOT LIKE '688%%'
      AND UPPER(s.stock_name) NOT LIKE '%%ST%%'
    """
    df_latest = pd.read_sql(sql_latest, engine)

    df_merge = pd.merge(df_latest, df_10low, on="code", how="inner")
    # 价差百分比：(最新收盘价‑10日低点)/10日低点 *100
    df_merge["diff_pct"] = (df_merge["price_close"] - df_merge["low_10d"]) / df_merge["low_10d"] * 100
    # 筛选：价差在 -3 ~ +3% 区间
    df = df_merge[(df_merge["diff_pct"] >= -3) & (df_merge["diff_pct"] <= 3)].copy()

    if df.empty:
        print("❌ 暂无10日低点与最新收盘价价差在3%以内的股票")
        return

    df["industry"] = df["industry"].fillna("未分类")
    # 板块内排序：按价差绝对值abs(diff_pct)升序 → 越靠近0越靠前
    df = df.sort_values(by=["industry", "diff_pct"], key=lambda x: x.abs() if x.name=="diff_pct" else x, ascending=[True, True]).reset_index(drop=True)

    codes = df["code"].unique().tolist()
    ph = ",".join([f"'{c}'" for c in codes])

    # 加载K线，最近3个月
    k_sql = f"""
        SELECT dt, code,
               price_open AS Open, price_close AS Close,
               price_highest AS High, price_lowest AS Low,
               trade_amount AS Volume, rise
        FROM stock_detail
        WHERE code IN ({ph})
        ORDER BY code, dt
    """
    df_k = pd.read_sql(k_sql, engine)
    df_k["dt"] = pd.to_datetime(df_k["dt"])
    end_dt = df_k["dt"].max()
    start_dt = end_dt - pd.DateOffset(months=3)
    df_k = df_k[df_k["dt"] >= start_dt].copy()
    # 平盘K线修复
    df_k.loc[df_k["Close"] == df_k["Open"], "Close"] += 0.0001

    last_df = df_k.sort_values("dt").groupby("code").last()[["Close", "rise"]]
    price_map = last_df["Close"].round(2).to_dict()
    rise_map = last_df["rise"].round(2).to_dict()

    print("🖼️ 开始绘制K线...")
    img_map = {}

    def plot_one(code):
        d = df_k[df_k["code"] == code].copy()
        if len(d) < 5:
            return code, ""
        d.set_index("dt", inplace=True)
        return code, fast_plot(d)

    with ThreadPoolExecutor(max_workers=6) as executor:
        res = list(executor.map(plot_one, codes))
    for c, i in res:
        img_map[c] = i

    ind_cnt = df["industry"].value_counts().sort_values(ascending=False)
    industries = ind_cnt.index.tolist()

    print("🌍 生成HTML...")
    html = '''
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <title>10日低点附近(±3%)个股K线看板</title>
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
            .near-text{color:#2f80ed;font-weight:bold;font-size:13px;}
        </style>
    </head>
    <body>
        <div class="container">
            <h1 class="title">📍 10日低点附近(±3%)个股K线看板</h1>
            <div class="col-switch">
                <button class="col-btn" onclick="changeColumns(2)">2列</button>
                <button class="col-btn active" onclick="changeColumns(3)">3列</button>
                <button class="col-btn" onclick="changeColumns(4)">4列</button>
                <button class="col-btn" onclick="changeColumns(5)">5列</button>
            </div>
            <div class="tab-wrap">
                <div class="tabs">
    '''
    for i, ind in enumerate(industries):
        active = "active" if i == 0 else ""
        html += f'<button class="tab {active}" onclick="setTab({i})">{ind}({ind_cnt[ind]})</button>'
    html += '</div></div>'

    for i, ind in enumerate(industries):
        active = "active" if i == 0 else ""
        html += f'<div class="tab-content {active}">'
        sub_df = df[df["industry"] == ind]
        for _, r in sub_df.iterrows():
            code = r["code"]
            img = img_map.get(code, "")
            if not img:
                continue
            price = price_map.get(code, "")
            rise_val = rise_map.get(code, 0)
            diff_pct_val = round(r["diff_pct"], 2)
            low_10 = round(r["low_10d"], 2)
            price_str = f"({price}元)" if price else ""
            rise_cls = "rise-red" if rise_val >= 0 else "rise-green"
            rise_str = f'<span class="{rise_cls}">{rise_val:+.2f}%</span>'

            html += f'''
            <div class="card">
                <div class="stock-title">{code} {r["stock_name"]}<span class="price">{price_str}</span>{rise_str}</div>
                <div class="sub near-text">
                    10日最低:{low_10}｜相对低点价差:{diff_pct_val:+.2f}%
                </div>
                <div class="sub">{r["industry_detail"]}</div>
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
    filename = f"../html/{datetime.now().strftime('%Y-%m-%d')}_10日低点附近3%股票.html"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"✅ 完成！文件已生成：{filename}")


if __name__ == "__main__":
    start_time = time.time()
    generate_near_10low_html()
    end_time = time.time()
    cost_time = end_time - start_time
    print(f"程序总耗时：{cost_time:.2f} 秒")
