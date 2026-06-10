from datetime import datetime
import time
import pandas as pd
import mplfinance as mpf
import matplotlib.pyplot as plt
import warnings
from io import BytesIO
import base64
from sqlalchemy import create_engine
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

# ======================================================================================
# 同花顺热榜TOP个股 HTML 生成
# ======================================================================================
def generate_hotstock_html():
    print("📥 加载同花顺热榜TOP个股数据...")
    # 获取最新交易日期
    last_dt = pd.read_sql("SELECT MAX(dt) AS dt FROM stock_detail", engine).iloc[0]["dt"]

    # 关联 dim_stock_tag 取出行业、细分行业
    sql = f"""
    select
        a.seq,
        a.stock_code,
        b.stock_name,
        c.industry,
        c.industry_detail
    from dim_stock_hot a
    left join stock_detail b on a.stock_code = b.code and b.dt = '{last_dt}'
    left join dim_stock_tag c on REPLACE(REPLACE(LOWER(c.code), 'sz', ''), 'sh', '') = a.stock_code
    where b.stock_name not like '%%ST%%'
    order by a.seq;
    """
    df_hot = pd.read_sql(sql, engine)
    if df_hot.empty:
        print("❌ 热榜暂无有效个股数据")
        return

    # 空值填充
    df_hot["industry"] = df_hot["industry"].fillna("未知行业")
    df_hot["industry_detail"] = df_hot["industry_detail"].fillna("未知细分")

    # 提取代码，批量查询K线
    codes = df_hot["stock_code"].unique().tolist()
    ph = ",".join([f"'{c}'" for c in codes])
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

    # 保留最近3个月K线
    end_dt = df_k["dt"].max()
    start_dt = end_dt - pd.DateOffset(months=3)
    df_k = df_k[df_k["dt"] >= start_dt].copy()

    # 平盘K线强制变红
    df_k.loc[df_k["Close"] == df_k["Open"], "Close"] += 0.0001

    # 组装最新价格、当日涨幅映射
    last_df = df_k.sort_values("dt").groupby("code").last()[["Close", "rise"]]
    price_map = last_df["Close"].round(2).to_dict()
    rise_map = last_df["rise"].round(2).to_dict()

    # 多线程绘图
    print("🖼️ 开始绘制热榜个股K线...")
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

    print("🌍 生成热榜HTML页面...")
    html = '''
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <title>同花顺热榜TOP个股</title>
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
            <h1 class="title">🔥 同花顺热榜TOP个股 K 线看板</h1>
            <div class="col-switch">
                <button class="col-btn" onclick="changeColumns(2)">2列</button>
                <button class="col-btn active" onclick="changeColumns(3)">3列</button>
                <button class="col-btn" onclick="changeColumns(4)">4列</button>
                <button class="col-btn" onclick="changeColumns(5)">5列</button>
            </div>
            <div class="tab-wrap">
                <div class="tabs">
                    <button class="tab active" onclick="setTab(0)">全部热榜个股</button>
                </div>
            </div>
            <div class="tab-content active">
    '''

    # 循环生成卡片，替换为行业+细分行业
    for _, r in df_hot.iterrows():
        code = r["stock_code"]
        seq = r["seq"]
        stock_name = r["stock_name"]
        industry = r["industry"]
        industry_detail = r["industry_detail"]
        img = img_map.get(code, "")
        if not img:
            continue

        price = price_map.get(code, "")
        rise_val = rise_map.get(code, 0)
        price_str = f"({price}元)" if price else ""
        rise_cls = "rise-red" if rise_val >= 0 else "rise-green"
        rise_str = f'<span class="{rise_cls}">{rise_val:+.2f}%</span>'

        # 原固定文字替换为 行业 | 细分行业
        html += f'''
        <div class="card">
            <div class="stock-title">排名{seq} | {code} {stock_name}<span class="price">{price_str}</span>{rise_str}</div>
            <div class="sub">{industry} | {industry_detail}</div>
            <img src="{img}">
        </div>
        '''

    html += '''
            </div>
        </div>
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

    # 输出HTML文件
    filename = f"../html/{datetime.now().strftime('%Y-%m-%d')}_同花顺热榜TOP个股.html"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"✅ 热榜看板完成！文件已生成：{filename}")


if __name__ == "__main__":
    start_time = time.time()
    generate_hotstock_html()
    end_time = time.time()
    print(f"程序总耗时：{end_time - start_time:.2f} 秒")