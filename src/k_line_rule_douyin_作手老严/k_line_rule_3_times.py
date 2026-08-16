from datetime import datetime, timedelta
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

def process_one(code, df_k_all):
    df = df_k_all[df_k_all["code"] == code].copy()
    if len(df) < 5:
        return code, ""
    df.set_index("dt", inplace=True)
    return code, fast_plot(df)

# ======================================================================================
# ✅ 新功能：90天内首次成交量3倍放量个股 HTML K线看板
# 规则：当日成交量 >= 前5日均量*3，取90天区间内第一次满足条件日期
# ======================================================================================
def generate_first_volume_break_html():
    print("📥 开始查询90天内首次放量（量能≥前5日均量3倍）股票...")

    # 1. 确定时间区间
    end_dt = pd.read_sql("SELECT MAX(dt) AS dt FROM stock_detail", engine).iloc[0]["dt"]
    start_90d = (pd.to_datetime(end_dt) - timedelta(days=90)).strftime("%Y-%m-%d")

    # 2. 拉取90天内所有非ST股票K线数据
    sql_k_raw = f"""
        SELECT dt, code, stock_name, trade_amount
        FROM stock_detail
        WHERE dt >= '{start_90d}'
          AND UPPER(stock_name) NOT LIKE '%%ST%%'
        ORDER BY code, dt ASC
    """
    df_raw = pd.read_sql(sql_k_raw, engine)
    df_raw["dt"] = pd.to_datetime(df_raw["dt"])

    if df_raw.empty:
        print("❌ 暂无符合条件股票")
        return

    # 3. 分组计算前5日均量，标记放量信号
    result_list = []
    grouped = df_raw.groupby("code")

    for code, g in grouped:
        g = g.sort_values("dt").reset_index(drop=True)
        # 滚动5日均量
        g["vol_ma5"] = g["trade_amount"].rolling(window=5).mean()
        # 判断放量条件：成交量 >= 5日均量 *3
        g["is_break"] = g["trade_amount"] >= g["vol_ma5"] * 3

        # 筛选满足条件的行，取最早一条（90天内第一次放量）
        break_df = g[g["is_break"] == True]
        if break_df.empty:
            continue
        first_break_row = break_df.iloc[0]
        result_list.append({
            "code": code,
            "stock_name": first_break_row["stock_name"],
            "first_break_dt": first_break_row["dt"].strftime("%Y-%m-%d")
        })

    df_target = pd.DataFrame(result_list)
    if df_target.empty:
        print("❌ 没有找到90天内首次放量个股")
        return

    target_codes = df_target["code"].unique().tolist()
    ph = ",".join([f"'{c}'" for c in target_codes])

    # 4. 关联行业标签
    sql_tag = f"""
        SELECT DISTINCT
            code,
            industry,
            industry_detail
        FROM dim_stock_tag
        WHERE REPLACE(REPLACE(LOWER(code), 'sz', ''), 'sh', '') IN ({ph})
    """
    df_tag = pd.read_sql(sql_tag, engine)
    # df_tag["code"] = df_tag["code"].apply(lambda x: replace(replace(str(x).lower(), "sz", ""), "sh", ""))
    def clean_code(s):
        s = str(s).lower()
        s = s.replace("sz", "").replace("sh", "")
        return s

    df_tag["code"] = df_tag["code"].apply(clean_code)

    df_merge = pd.merge(
        df_target,
        df_tag,
        on="code",
        how="left"
    )
    df_merge["industry"] = df_merge["industry"].fillna("未分类")

    # 5. 加载K线绘图数据（近3个月，用于绘制K线图）
    k_end = pd.to_datetime(end_dt)
    k_start = k_end - timedelta(days=90)
    sql_plot_k = f"""
        SELECT dt, code,
               price_open AS Open, price_close AS Close,
               price_highest AS High, price_lowest AS Low,
               trade_amount AS Volume, rise
        FROM stock_detail
        WHERE code IN ({ph})
          AND dt >= '{k_start.strftime("%Y-%m-%d")}'
        ORDER BY code, dt ASC
    """
    df_k_plot = pd.read_sql(sql_plot_k, engine)
    df_k_plot["dt"] = pd.to_datetime(df_k_plot["dt"])
    # 平盘K线变红
    df_k_plot.loc[df_k_plot["Close"] == df_k_plot["Open"], "Close"] += 0.0001

    # 获取最新价格、当日涨幅
    last_k = df_k_plot.sort_values("dt").groupby("code").last()[["Close", "rise"]]
    price_map = last_k["Close"].round(2).to_dict()
    rise_map = last_k["rise"].round(2).to_dict()

    # 6. 多线程批量绘图
    print("🖼️ 开始绘制K线图...")
    img_map = {}

    def plot_task(code):
        sub = df_k_plot[df_k_plot["code"] == code].copy()
        if len(sub) < 5:
            return code, ""
        sub.set_index("dt", inplace=True)
        return code, fast_plot(sub)

    with ThreadPoolExecutor(max_workers=6) as executor:
        res = list(executor.map(plot_task, target_codes))
    for c, img in res:
        img_map[c] = img

    # 7. 按行业分组，行业数量排序
    ind_cnt = df_merge["industry"].value_counts().sort_values(ascending=False)
    industries = ind_cnt.index.tolist()

    # 8. HTML页面生成
    print("🌍 生成HTML看板...")
    html = '''
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <title>90天内首次3倍放量个股K线看板</title>
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
            .break-date{
                color:#ff2222;
                font-weight:bold;
                font-size:14px;
                margin-bottom:6px;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1 class="title">📊 90天内首次成交量3倍放量个股 K线看板</h1>
            <div class="col-switch">
                <button class="col-btn" onclick="changeColumns(2)">2列</button>
                <button class="col-btn active" onclick="changeColumns(3)">3列</button>
                <button class="col-btn" onclick="changeColumns(4)">4列</button>
                <button class="col-btn" onclick="changeColumns(5)">5列</button>
            </div>
            <div class="tab-wrap">
                <div class="tabs">
    '''

    # 构建行业Tab按钮
    for i, ind in enumerate(industries):
        active = "active" if i == 0 else ""
        html += f'<button class="tab {active}" onclick="setTab({i})">{ind}({ind_cnt[ind]})</button>'
    html += '</div></div>'

    # 填充每个行业卡片内容
    for i, ind in enumerate(industries):
        active_cls = "active" if i == 0 else ""
        html += f'<div class="tab-content {active_cls}">'
        sub_data = df_merge[df_merge["industry"] == ind]
        for _, row in sub_data.iterrows():
            code = row["code"]
            img = img_map.get(code, "")
            if not img:
                continue

            price = price_map.get(code, "")
            rise_val = rise_map.get(code, 0.00)
            break_day = row["first_break_dt"]

            price_str = f"({price}元)" if price else ""
            rise_css = "rise-red" if rise_val >= 0 else "rise-green"
            rise_html = f'<span class="{rise_css}">{rise_val:+.2f}%</span>'

            html += f'''
            <div class="card">
                <div class="break-date">🔥首次放量日期：{break_day}</div>
                <div class="stock-title">{code} {row["stock_name"]}<span class="price">{price_str}</span>{rise_html}</div>
                <div class="sub">{row["industry_detail"]}</div>
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

    save_name = f"../html/{datetime.now().strftime('%Y-%m-%d')}_90天首次3倍放量股票.html"
    with open(save_name, "w", encoding="utf-8") as fw:
        fw.write(html)

    print(f"✅ 文件生成完成！路径：{save_name}")


if __name__ == "__main__":
    start_time = time.time()

    # 执行放量选股看板
    generate_first_volume_break_html()

    end_time = time.time()
    print(f"程序总耗时：{end_time - start_time:.2f} 秒")