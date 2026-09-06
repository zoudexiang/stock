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


def generate_high30_drop_html():
    """
    规则：最近30个交易日高点相对最新收盘价回撤大于15%
    按板块tab分组，板块内部按回撤幅度降序（跌幅越大越靠前）
    """
    print("📥 加载30日高点回撤超15%个股数据...")
    last_dt_df = pd.read_sql("SELECT MAX(dt) AS dt FROM stock_detail", engine)
    last_dt = last_dt_df.iloc[0]["dt"]

    # 取最近30个交易日最高价格
    sql_30high = f"""
    SELECT code, MAX(price_highest) as high_30d
    FROM stock_detail
    WHERE dt >= DATE_SUB('{last_dt}', INTERVAL 30 DAY)
    GROUP BY code
    """
    df_30high = pd.read_sql(sql_30high, engine)

    # 获取最新交易日个股基础数据，关联行业标签
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

    # 合并30日高点数据
    df_merge = pd.merge(df_latest, df_30high, on="code", how="inner")
    # 计算回撤比例 (高点‑收盘价)/高点 *100；筛选回撤>15%
    df_merge["drop_rate"] = (df_merge["high_30d"] - df_merge["price_close"]) / df_merge["high_30d"] * 100
    df = df_merge[df_merge["drop_rate"] > 15].copy()

    if df.empty:
        print("❌ 暂无30日高点回撤超过15%的股票")
        return

    # 板块内：按回撤幅度drop_rate降序，跌幅越大排在越前面
    df["industry"] = df["industry"].fillna("未分类")
    df = df.sort_values(by=["industry", "drop_rate"], ascending=[True, False]).reset_index(drop=True)

    codes = df["code"].unique().tolist()
    ph = ",".join([f"'{c}'" for c in codes])

    # 加载最近3个月K线数据
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
    # 平盘K线颜色修复
    df_k.loc[df_k["Close"] == df_k["Open"], "Close"] += 0.0001

    # 最新价格、涨跌幅映射
    last_df = df_k.sort_values("dt").groupby("code").last()[["Close", "rise"]]
    price_map = last_df["Close"].round(2).to_dict()
    rise_map = last_df["rise"].round(2).to_dict()

    # 多线程绘制K线
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

    # 板块tab按个股数量降序
    ind_cnt = df["industry"].value_counts().sort_values(ascending=False)
    industries = ind_cnt.index.tolist()

    print("🌍 生成HTML...")
    html = '''
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <title>30日高点回撤超15%个股K线看板</title>
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
            .drop-text{color:#c92a2a;font-weight:bold;font-size:13px;}
        </style>
    </head>
    <body>
        <div class="container">
            <h1 class="title">📉 30日区间高点回撤超15%个股K线看板</h1>
            <div class="col-switch">
                <button class="col-btn" onclick="changeColumns(2)">2列</button>
                <button class="col-btn active" onclick="changeColumns(3)">3列</button>
                <button class="col-btn" onclick="changeColumns(4)">4列</button>
                <button class="col-btn" onclick="changeColumns(5)">5列</button>
            </div>
            <div class="tab-wrap">
                <div class="tabs">
    '''
    # 渲染板块tab按钮
    for i, ind in enumerate(industries):
        active = "active" if i == 0 else ""
        html += f'<button class="tab {active}" onclick="setTab({i})">{ind}({ind_cnt[ind]})</button>'
    html += '</div></div>'

    # 渲染板块卡片，板块内部已经按drop_rate降序
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
            drop_rate_val = round(r["drop_rate"], 2)
            high_30 = round(r["high_30d"], 2)
            price_str = f"({price}元)" if price else ""
            rise_cls = "rise-red" if rise_val >= 0 else "rise-green"
            rise_str = f'<span class="{rise_cls}">{rise_val:+.2f}%</span>'

            html += f'''
            <div class="card">
                <div class="stock-title">{code} {r["stock_name"]}<span class="price">{price_str}</span>{rise_str}</div>
                <div class="sub drop-text">
                    30日最高:{high_30}｜高点回撤:{drop_rate_val:.2f}%
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
    filename = f"../html/{datetime.now().strftime('%Y-%m-%d')}_30日高点回撤超15%股票.html"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"✅ 完成！文件已生成：{filename}")


if __name__ == "__main__":
    start_time = time.time()
    generate_high30_drop_html()
    end_time = time.time()
    cost_time = end_time - start_time
    print(f"程序总耗时：{cost_time:.2f} 秒")
