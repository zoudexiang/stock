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
# 🔍【优化版】M 顶识别：支持 6 个月跨度 + 锁定最近 3 天右侧触发
# ======================================================================================
def detect_m_pattern(df_single):
    """
    判断单个股票是否在【最近 3 天内】刚刚到达/跌破 M 顶右侧颈线低位
    返回: (bool, left_peak_price, right_peak_price, neckline_price)
    """
    if len(df_single) < 60:
        return False, 0, 0, 0

    df = df_single.sort_values("dt").reset_index(drop=True)
    highs = df["High"].values
    volumes = df["Volume"].values
    closes = df["Close"].values
    total_len = len(df)

    # 1. 寻峰 (6个月数据下，最小间距设为 7 天，显著性 1.5%)
    peaks, _ = find_peaks(highs, distance=7, prominence=highs.max() * 0.015)

    if len(peaks) < 2:
        return False, 0, 0, 0

    # 取最后两个显著波峰 (T1: 左顶, T2: 右顶)
    p1_idx, p2_idx = peaks[-2], peaks[-1]

    # ------------------------------------------------------------------
    # 🎯【核心条件 1】右顶 (T2) 必须是最近形成的（发生在最后 7 个交易日内）
    # ------------------------------------------------------------------
    if (total_len - 1 - p2_idx) > 7:
        return False, 0, 0, 0

    # 🎯【核心条件 2】两顶时间间隔合适（8 ~ 90 个交易日，支持大级别 M 顶）
    peak_interval = p2_idx - p1_idx
    if not (8 <= peak_interval <= 90):
        return False, 0, 0, 0

    t1_high = highs[p1_idx]
    t2_high = highs[p2_idx]

    # 🎯【核心条件 3】两顶高度接近（价差在 3% 以内）
    if abs(t1_high - t2_high) / max(t1_high, t2_high) > 0.03:
        return False, 0, 0, 0

    # 🎯【核心条件 4】计算颈线位（两顶之间的最低点）
    valley_slice = highs[p1_idx:p2_idx]
    neckline = np.min(valley_slice)

    # 🎯【核心条件 5】量价背离（右顶成交量不能明显超越左顶）
    v1 = volumes[p1_idx]
    v2 = volumes[p2_idx]
    if v2 > v1 * 1.15:
        return False, 0, 0, 0

    # ------------------------------------------------------------------
    # 🎯【核心条件 6】锁定“新鲜度”：仅限【最近 3 天内】到达/下破颈线位置
    # ------------------------------------------------------------------
    recent_3_closes = closes[-3:]

    # 最近 3 天至少有 1 天收盘价处于 [颈线 * 0.92, 颈线 * 1.01] 临界区
    is_recently_triggered = any(0.92 * neckline <= c <= 1.01 * neckline for c in recent_3_closes)

    if is_recently_triggered:
        return True, round(t1_high, 2), round(t2_high, 2), round(neckline, 2)

    return False, 0, 0, 0


# ======================================================================================
# ✅ M 顶形态选股 + 按板块 Tab 分类生成 HTML
# ======================================================================================
def generate_m_pattern_html(today):

    print("📥 加载全市场近 6 个月数据，筛选【最近 3 天】触发 M 顶的股票...")

    sql_k = f"""
    select 
        dt, 
        s.code, 
        stock_name,
        Open, 
        Close,
        High, 
        Low,
        Volume, 
        rise,
        industry, 
        industry_detail
    from (
        select 
            dt, 
            code, 
            stock_name,
            price_open as Open, 
            price_close as Close,
            price_highest as High, 
            price_lowest as Low,
            trade_amount as Volume, 
            rise
        from stock_detail
        where dt >= date_sub('{today}', interval 6 month) and upper(stock_name) not like '%%ST%%'
    ) s left join (
        select 
            replace(replace(lower(code), 'sz', ''), 'sh', '') as code, 
            industry, 
            industry_detail
        from dim_stock_tag
    ) dst on dst.code = s.code
    order by s.code, dt;

    """
    df_all = pd.read_sql(sql_k, engine)
    if df_all.empty:
        print("❌ 未查到股票 K 线数据")
        return

    df_all["dt"] = pd.to_datetime(df_all["dt"])
    df_all.loc[df_all["Close"] == df_all["Open"], "Close"] += 0.0001

    # 2. 遍历筛选符合 M 形态的个股
    print("🔎 执行 6 个月跨度 & 最近3天临界 M 形态检测算法...")
    codes = df_all["code"].unique().tolist()
    m_stocks = []

    for code in codes:
        sub_df = df_all[df_all["code"] == code]
        is_m, t1, t2, neck = detect_m_pattern(sub_df)
        if is_m:
            last_row = sub_df.iloc[-1]

            # 板块归类
            board = "主板"
            if code.startswith("300") or code.startswith("301"):
                board = "创业板"
            elif code.startswith("688"):
                board = "科创板"

            m_stocks.append({
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

    df_m = pd.DataFrame(m_stocks)
    if df_m.empty:
        print("❌ 近 3 天内暂无刚刚触发 M 顶形态的股票")
        return

    print(f"🎯 成功捕捉到 {len(df_m)} 只【近 3 天】触发 M 顶临界点的个股！")

    # 3. 多线程并行绘图
    print("🖼️ 开始绘制 K 线图...")
    img_map = {}
    m_codes = df_m["code"].tolist()

    def plot_one(code):
        d = df_all[df_all["code"] == code].copy()
        d.set_index("dt", inplace=True)
        return code, fast_plot(d)

    with ThreadPoolExecutor(max_workers=6) as executor:
        res = list(executor.map(plot_one, m_codes))
    for c, img in res:
        img_map[c] = img

    # 4. 板块 TAB 数据划分
    tabs = [
        {"id": "all", "name": "所有", "df": df_m},
        {"id": "cyb", "name": "仅创业板", "df": df_m[df_m["board"] == "创业板"]},
        {"id": "zb", "name": "仅主板", "df": df_m[df_m["board"] == "主板"]},
        {"id": "kcb", "name": "仅科创板", "df": df_m[df_m["board"] == "科创板"]},
        {"id": "zb_cyb", "name": "主板+创业板", "df": df_m[df_m["board"].isin(["主板", "创业板"])].copy()}
    ]

    # 5. 生成 HTML 页面
    print("🌍 正在构建 HTML 页面...")
    html = '''
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <title>M 顶 (近 3 天临界触发) 看跌预警看板</title>
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
            .m-info{color:#d90429;font-weight:bold;font-size:13px;background:#fdf0f0;padding:4px 8px;border-radius:4px;margin-top:6px}

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
                M 顶形态【近 3 天右侧临界】预警：<br/>
                1、跨度：基于过去 6 个月历史 K 线识别<br/>
                2、右顶：近 7 个交易日内刚形成右顶<br/>
                3、突破：近 3 个交易日收盘价首次跌破/逼近颈线位置
            </div>
            <h1 class="title">📉 M 顶 (近 3 天右侧触及/破位) K 线看板</h1>
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
                <div class="m-info">
                    M 顶结构: 左顶 {r["t1_price"]} 元 ｜ 右顶 {r["t2_price"]} 元 ｜ 颈线 {r["neck_price"]} 元
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

    filename = f"../html/{datetime.now().strftime('%Y-%m-%d')}_M顶近3天触发股票看板.html"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"✅ 完成！文件已生成：{filename}")


if __name__ == "__main__":

    start_time = time.time()
    # today = datetime.now().strftime("%Y-%m-%d")
    today = '2026-08-03'
    generate_m_pattern_html(today)

    cost_time = time.time() - start_time
    print(f"程序总耗时：{cost_time:.2f} 秒")