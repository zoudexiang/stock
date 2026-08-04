from datetime import datetime
import time
import pandas as pd
import numpy as np
import mplfinance as mpf
import matplotlib.pyplot as plt
import warnings
from io import BytesIO
import base64
from sqlalchemy import create_engine
from concurrent.futures import ThreadPoolExecutor
from scipy.signal import find_peaks

from src.utils import constants

# ====================== MySQL 配置 ======================
MYSQL_HOST = constants.db_config['host']
MYSQL_USER = constants.db_config['user']
MYSQL_PASSWORD = constants.db_config['password']
MYSQL_DB = constants.db_config['database']
# ========================================================

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
    except Exception:
        return ""


# ======================================================================================
# 🔍【N 颈线位专属算法】锁定近 3 天刚好处于 M 顶中间 N 颈线支撑区的股票
# ======================================================================================
def detect_n_neckline_pattern(df_single):
    """
    判断单个股票是否在【最近 3 天内】股价刚好测试/处于 M 形态的 N 颈线位
    返回: (bool, left_peak_price, right_peak_price, neckline_price)
    """
    if len(df_single) < 60:
        return False, 0, 0, 0

    df = df_single.sort_values("dt").reset_index(drop=True)
    highs = df["High"].values
    lows = df["Low"].values
    volumes = df["Volume"].values
    closes = df["Close"].values
    total_len = len(df)

    # 1. 寻峰 (6个月数据下，间隔 7 天，显著性 1.5%)
    peaks, _ = find_peaks(highs, distance=7, prominence=highs.max() * 0.015)

    if len(peaks) < 2:
        return False, 0, 0, 0

    # 取最后两个显著波峰 (T1: 左顶, T2: 右顶)
    p1_idx, p2_idx = peaks[-2], peaks[-1]

    # 规则 1: 右顶 (T2) 必须是最近形成的 (发生在最后 7 个交易日内)
    if (total_len - 1 - p2_idx) > 7:
        return False, 0, 0, 0

    # 规则 2: 两顶时间间隔合适 (8 ~ 90 个交易日)
    peak_interval = p2_idx - p1_idx
    if not (8 <= peak_interval <= 90):
        return False, 0, 0, 0

    t1_high = highs[p1_idx]
    t2_high = highs[p2_idx]

    # 规则 3: 两顶空间高度接近 (价差 3% 以内)
    if abs(t1_high - t2_high) / max(t1_high, t2_high) > 0.03:
        return False, 0, 0, 0

    # 规则 4: 计算 N 颈线位 (两顶之间的最低价格)
    valley_slice = lows[p1_idx:p2_idx]
    neckline = np.min(valley_slice)

    # ------------------------------------------------------------------
    # 🎯【核心定位】：锁定最近 3 天股价【正好处于 N 颈线位附近】
    # 满足：近 3 天至少有一天的收盘价或最低价在 [颈线 * 0.98, 颈线 * 1.03]
    # ------------------------------------------------------------------
    recent_3_closes = closes[-3:]
    recent_3_lows = lows[-3:]

    at_neckline = any(
        (0.98 * neckline <= c <= 1.03 * neckline) or (0.98 * neckline <= l <= 1.03 * neckline)
        for c, l in zip(recent_3_closes, recent_3_lows)
    )

    if at_neckline:
        return True, round(t1_high, 2), round(t2_high, 2), round(neckline, 2)

    return False, 0, 0, 0


# ======================================================================================
# ✅ N 颈线位股票筛选 + 5 TAB 板块 HTML 看板生成
# ======================================================================================
def generate_n_neckline_html():
    print("📥 加载全市场近 6 个月数据，筛选【最近 3 天刚好处于 N 颈线位】的股票...")

    # 1. 获取最新日期，取近 6 个月的数据
    last_dt = pd.read_sql("SELECT MAX(dt) AS dt FROM stock_detail", engine).iloc[0]["dt"]

    sql_k = f"""
        SELECT 
            s.dt, s.code, s.stock_name,
            s.price_open AS Open, s.price_close AS Close,
            s.price_highest AS High, s.price_lowest AS Low,
            s.trade_amount AS Volume, s.rise,
            dst.industry, dst.industry_detail
        FROM stock_detail s
        LEFT JOIN dim_stock_tag dst 
            ON REPLACE(REPLACE(LOWER(dst.code), 'sz', ''), 'sh', '') = s.code
        WHERE s.dt >= DATE_SUB('{last_dt}', INTERVAL 6 MONTH)
          AND UPPER(s.stock_name) NOT LIKE '%%ST%%'
        ORDER BY s.code, s.dt
    """
    df_all = pd.read_sql(sql_k, engine)
    if df_all.empty:
        print("❌ 未查到股票 K 线数据")
        return

    df_all["dt"] = pd.to_datetime(df_all["dt"])
    df_all.loc[df_all["Close"] == df_all["Open"], "Close"] += 0.0001

    # 2. 遍历筛选处于 N 颈线位的个股
    print("🔎 执行 N 颈线位检测算法...")
    codes = df_all["code"].unique().tolist()
    n_stocks = []

    for code in codes:
        sub_df = df_all[df_all["code"] == code]
        is_n, t1, t2, neck = detect_n_neckline_pattern(sub_df)
        if is_n:
            last_row = sub_df.iloc[-1]

            # 板块归类
            board = "主板"
            if code.startswith("300") or code.startswith("301"):
                board = "创业板"
            elif code.startswith("688"):
                board = "科创板"

            n_stocks.append({
                "code": code,
                "stock_name": last_row["stock_name"],
                "board": board,
                "price": round(last_row["Close"], 2),
                "rise": round(last_row["rise"], 2),
                "industry": last_row["industry"] if last_row["industry"] else "未分类",
                "industry_detail": last_row["industry_detail"] if last_row["industry_detail"] else "",
                "t1_price": t1,
                "t2_price": t2,
                "neck_price": neck
            })

    df_n = pd.DataFrame(n_stocks)
    if df_n.empty:
        print("❌ 最近 3 天暂无正好处于 N 颈线位的股票")
        return

    print(f"🎯 成功捕捉到 {len(df_n)} 只【近 3 天刚好处于 N 颈线位】的标的！")

    # 3. 多线程并行绘图
    print("🖼️ 开始绘制 K 线图...")
    img_map = {}
    n_codes = df_n["code"].tolist()

    def plot_one(code):
        d = df_all[df_all["code"] == code].copy()
        d.set_index("dt", inplace=True)
        return code, fast_plot(d)

    with ThreadPoolExecutor(max_workers=6) as executor:
        res = list(executor.map(plot_one, n_codes))
    for c, img in res:
        img_map[c] = img

    # 4. 板块 TAB 数据划分
    tabs = [
        {"id": "all", "name": "所有", "df": df_n},
        {"id": "cyb", "name": "仅创业板", "df": df_n[df_n["board"] == "创业板"]},
        {"id": "zb", "name": "仅主板", "df": df_n[df_n["board"] == "主板"]},
        {"id": "kcb", "name": "仅科创板", "df": df_n[df_n["board"] == "科创板"]},
        {"id": "zb_cyb", "name": "主板+创业板", "df": df_n[df_n["board"].isin(["主板", "创业板"])].copy()}
    ]

    # 5. 生成 HTML 页面
    print("🌍 正在构建 HTML 页面...")
    html = '''
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <title>M 形态 - N 颈线支撑位临界股票看板</title>
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
            .tab{padding:8px 16px;background:#f1f3f5;border:0;border-radius:6px;cursor:pointer;font-weight:bold}
            .tab.active{background:#2f80ed;color:white}
            .tab-content{display:none;grid-template-columns:repeat(3,1fr);gap:16px}
            .tab-content.active{display:grid}
            .card{background:white;padding:12px;border-radius:12px;border:1px solid #e1e4e8}
            .card img{width:100%;border-radius:8px;margin-top:10px}
            .stock-title{font-weight:bold;font-size:16px}
            .price{color:#333;font-size:14px;margin-left:6px}
            .rise-green{color:#28a745;font-size:14px;margin-left:4px}
            .rise-red{color:#e63946;font-size:14px;margin-left:4px}
            .sub{font-size:12px;color:#666;margin-top:4px}
            .n-info{color:#0077b6;font-weight:bold;font-size:13px;background:#edf6f9;padding:4px 8px;border-radius:4px;margin-top:6px}

            .rule-wrap{
                text-align:center;
                margin-bottom:16px;
                font-size:16px;
                font-weight:bold;
                line-height:1.7;
                animation: colorLoop 4s infinite linear;
            }
            @keyframes colorLoop {
                0%{color:#ff2222;}
                25%{color:#00aa22;}
                50%{color:#ddbb00;}
                75%{color:#9922bb;}
                100%{color:#ff2222;}
            }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="rule-wrap">
                M 形态【N 颈线支撑位】观察原则：<br/>
                1、跨度：基于过去 6 个月数据提取 $T_1$ 和 $T_2$ 双顶<br/>
                2、位置：股价最近 3 天运行至中间 $N$ 颈线支撑位附近<br/>
                3、博弈：观察颈线处能否企稳反弹或是否破位下杀
            </div>
            <h1 class="title">📍 M 形态 - N 颈线支撑位 K 线看板</h1>
            <div class="col-switch">
                <button class="col-btn" onclick="changeColumns(2)">2列</button>
                <button class="col-btn active" onclick="changeColumns(3)">3列</button>
                <button class="col-btn" onclick="changeColumns(4)">4列</button>
                <button class="col-btn" onclick="changeColumns(5)">5列</button>
            </div>
            <div class="tab-wrap">
                <div class="tabs">
    '''

    # 生成 TAB 按钮
    for i, t in enumerate(tabs):
        active = "active" if i == 0 else ""
        count = len(t["df"])
        html += f'<button class="tab {active}" onclick="setTab({i})">{t["name"]} ({count})</button>'
    html += '</div></div>'

    # 生成各 TAB 对应的个股卡片
    for i, t in enumerate(tabs):
        active = "active" if i == 0 else ""
        html += f'<div class="tab-content {active}">'
        sub_df = t["df"]

        for _, r in sub_df.iterrows():
            code = r["code"]
            img = img_map.get(code, "")
            if not img:
                continue

            price_str = f"({r['price']}元)"
            rise_val = r["rise"]
            rise_cls = "rise-red" if rise_val >= 0 else "rise-green"
            rise_str = f'<span class="{rise_cls}">{rise_val:+.2f}%</span>'

            html += f'''
            <div class="card">
                <div class="stock-title">{code} {r["stock_name"]}<span class="price">{price_str}</span>{rise_str}</div>
                <div class="sub">板块: {r["board"]} ｜ 行业: {r["industry"]}</div>
                <div class="n-info">
                    M顶关键点: 左顶 {r["t1_price"]} 元 ｜ 右顶 {r["t2_price"]} 元 ｜ 🎯 颈线位 N: {r["neck_price"]} 元
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
                document.querySelectorAll('.col-btn').forEach((btn) => {
                    btn.classList.toggle('active', parseInt(btn.innerText[0]) === col);
                });
            }
            function setTab(i){
                document.querySelectorAll('.tab-content').forEach((e,j)=>{
                    e.classList.toggle('active', j==i);
                    document.querySelectorAll('.tab')[j].classList.toggle('active', j==i);
                });
            }
        </script>
    </body></html>
    '''

    filename = f"../html/{datetime.now().strftime('%Y-%m-%d')}_N颈线位股票看板.html"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"✅ 完成！文件已生成：{filename}")


if __name__ == "__main__":
    start_time = time.time()

    generate_n_neckline_html()

    cost_time = time.time() - start_time
    print(f"程序总耗时：{cost_time:.2f} 秒")