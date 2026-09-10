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
import requests
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
    except Exception as e:
        print(f"绘图异常: {e}")
        return ""

# -------------------- 获取同花顺热榜TOP100 --------------------
def get_ths_hot_top100():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    url = "https://dq.10jqka.com.cn/fuyao/hot_list_data/out/hot_list/v1/stock"
    params = {
        "stock_type": "a",
        "type": "hour",  # day=24小时热榜；hour=小时热榜
        "list_type": "normal"
    }
    resp = requests.get(url, params=params, headers=headers, timeout=15)
    json_data = resp.json()
    stock_list = json_data.get("data", {}).get("stock_list", [])
    res = []
    for item in stock_list[:100]:
        row = {
            "排名": item.get("order"),
            "代码": item.get("code"),
            "名称": item.get("name"),
            "涨跌幅": item.get("change_rate"),
            "现价": item.get("price")
        }
        res.append(row)
    df = pd.DataFrame(res)
    return df

# ======================================================================================
# ✅ 同花顺热榜TOP100 K线看板（无板块Tab，严格按热榜排名顺序）
# ======================================================================================
def generate_hot100_html():
    print("📥 获取同花顺热榜TOP100...")
    df_hot = get_ths_hot_top100()
    if df_hot.empty:
        print("❌ 获取热榜数据失败！")
        return
    # 保持热榜原有顺序
    code_list = df_hot["代码"].tolist()
    print(f"✅ 共获取热榜个股数量：{len(code_list)}")

    # 查询这批股票K线数据
    ph = ",".join([f"'{c}'" for c in code_list])
    k_sql = f"""
        SELECT dt, code,
               price_open AS Open, price_close AS Close,
               price_highest AS High, price_lowest AS Low,
               trade_amount AS Volume, rise
        FROM stock_detail
        WHERE code IN ({ph})
        ORDER BY code, dt
    """
    print("📥 加载K线数据...")
    df_k = pd.read_sql(k_sql, engine)
    df_k["dt"] = pd.to_datetime(df_k["dt"])
    # 保留最近3个月K线
    end_dt = df_k["dt"].max()
    start_dt = end_dt - pd.DateOffset(months=3)
    df_k = df_k[df_k["dt"] >= start_dt].copy()
    # 平盘K线变红（和你原有代码保持一致）
    df_k.loc[df_k["Close"] == df_k["Open"], "Close"] += 0.0001

    # 多线程绘图
    print("🖼️ 开始绘制K线...")
    img_map = {}
    def plot_one(code):
        d = df_k[df_k["code"] == code].copy()
        if len(d) < 5:
            return code, ""
        d.set_index("dt", inplace=True)
        return code, fast_plot(d)
    with ThreadPoolExecutor(max_workers=6) as executor:
        res = list(executor.map(plot_one, code_list))
    for c, i in res:
        img_map[c] = i

    # 生成HTML页面，去掉板块Tab，按热榜排名顺序渲染卡片
    print("🌍 生成HTML...")
    html = '''
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <title>同花顺热榜TOP100 K线看板</title>
        <style>
            *{box-sizing:border-box;margin:0;padding:0;font-family:Microsoft YaHei}
            body{background:#f5f7fa;padding:20px}
            .container{max-width:1900px;margin:0 auto}
            .title{text-align:center;margin-bottom:20px}
            .col-switch{display:flex;gap:8px;justify-content:center;margin-bottom:20px}
            .col-btn{padding:10px 20px;border:none;border-radius:6px;background:#e3e6ed;cursor:pointer}
            .col-btn.active{background:#2f80ed;color:white}
            .content-wrap{display:grid;grid-template-columns:repeat(3,1fr);gap:16px}
            .card{background:white;padding:12px;border-radius:12px}
            .card img{width:100%;border-radius:8px;margin-top:10px}
            .stock-title{font-weight:bold}
            .rank{background:#2f80ed;color:white;padding:2px 6px;border-radius:4px;margin-right:6px;font-size:13px}
            .price{color:#e63946;font-size:14px;margin-left:6px}
            .rise-green{color:#28a745;font-size:14px;margin-left:4px}
            .rise-red{color:#e63946;font-size:14px;margin-left:4px}
            .sub{font-size:12px;color:#888;margin-top:4px}
        </style>
    </head>
    <body>
        <div class="container">
            <h1 class="title">🔥 同花顺热榜TOP100 K线看板（按热榜排名顺序）</h1>
            <div class="col-switch">
                <button class="col-btn" onclick="changeColumns(2)">2列</button>
                <button class="col-btn active" onclick="changeColumns(3)">3列</button>
                <button class="col-btn" onclick="changeColumns(4)">4列</button>
                <button class="col-btn" onclick="changeColumns(5)">5列</button>
            </div>
            <div class="content-wrap">
    '''
    # 循环：严格按照热榜排名顺序输出卡片
    for _, row_hot in df_hot.iterrows():
        code = row_hot["代码"]
        img = img_map.get(code, "")
        rank = row_hot["排名"]
        name = row_hot["名称"]
        price = row_hot["现价"]
        chg_rate = row_hot["涨跌幅"]
        if not img:
            continue
        # ==========【修复空值】==========
        if chg_rate is None:
            chg_rate = 0
        chg_rate_val = float(chg_rate)
        rise_cls = "rise-red" if chg_rate_val >= 0 else "rise-green"
        # 现价也增加兜底，防止price为None报错
        price_str = f"{price}元" if price is not None else "-"
        html += f'''
        <div class="card">
            <div class="stock-title">
                <span class="rank">{rank}</span>{code} {name}
                <span class="price">{price_str}</span>
                <span class="{rise_cls}">{chg_rate_val:.2f}%</span>
            </div>
            <img src="{img}">
        </div>
        '''
    html += '''
            </div>
        </div>
        <script>
            function changeColumns(col) {
                let wrap = document.querySelector('.content-wrap');
                wrap.style.gridTemplateColumns = `repeat(${col}, 1fr)`;
                document.querySelectorAll('.col-btn').forEach((btn,i) => {
                    btn.classList.toggle('active', parseInt(btn.innerText[0]) === col);
                });
            }
        </script>
    </body></html>
    '''
    filename = f"../html/{datetime.now().strftime('%Y-%m-%d')}_同花顺热榜TOP100.html"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"✅ 完成！文件已生成：{filename}")

if __name__ == "__main__":
    start_time = time.time()
    generate_hot100_html()
    end_time = time.time()
    cost_time = end_time - start_time
    print(f"程序总耗时：{cost_time:.2f} 秒")
