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
# ✅【一日持股法选股看板】
# ======================================================================================
def generate_html(dt):

    print("📥 加载【一日持股法】选股数据...")
    # 1. 获取最新一天日期
    if dt is None:
        dt = pd.read_sql("SELECT MAX(dt) AS dt FROM stock_detail", engine).iloc[0]["dt"]
    # 2. SQL 使用 text 参数化，不再用f-string拼接！% 正常单写，不会冲突
    sql = text("""
        select 
            a.dt, 
            a.code, 
            a.price_open as Open, 
            a.price_close as Close,
            a.price_highest as High, 
            a.price_lowest as Low, 
            a.trade_amount as Volume, 
            a.rise,
            a.turnover_rate,
            a.stock_name,
            a.rise_5,
            a.rise_10,
            a.rise_15,
            a.ratio,
            dst.industry,
            dst.industry_detail
        from (
            -- step1 基础过滤
            select
                dt, 
                code, 
                stock_name,
                price_open, 
                price_close,
                price_highest, 
                price_lowest, 
                trade_amount, 
                rise,
                turnover_rate,
                rise_5,
                rise_10,
                rise_15,
                ratio
            from stock_detail
            where dt=:cur_dt 
                -- 筛选涨幅 3% ~ 5% 之间的
                and rise>=3 and rise<=5
                -- 筛选换手 5 ~ 10 之间的
                and turnover_rate>=5 and turnover_rate<=10
                -- 筛选市值 50 ~ 200 亿之间的
                and (total_market_capitalization / 100000000)>=50 
                and (total_market_capitalization / 100000000)<=200 
                -- 筛选量比 >=1 的
                and ratio>=1
        ) a join (
            -- step2 近一个月有涨停标的
            select distinct code
            from stock_detail
            where dt>=date_sub(:cur_dt, interval 1 month)
                and upper(stock_name) not like '%ST%'
                and code not like '688%'
        ) b on a.code=b.code
        left join dim_stock_tag dst on replace(replace(lower(dst.code), 'sz', ''), 'sh', '') = a.code;
    """)
    # 参数传入
    df = pd.read_sql(sql, engine, params={"cur_dt": str(dt)})
    if df.empty:
        print("❌ 暂无符合【一日持股法】条件股票")
        return
    # 3. 加载这些股票的K线
    codes = df["code"].unique().tolist()
    ph = ",".join([f"'{c}'" for c in codes])
    k_sql = f"""
        select 
            dt, 
            code,
            price_open AS Open, 
            price_close AS Close,
            price_highest AS High, 
            price_lowest AS Low,
            trade_amount AS Volume, 
            rise
        from stock_detail
        where code in ({ph})
        order by code, dt
    """
    df_k = pd.read_sql(k_sql, engine)
    df_k["dt"] = pd.to_datetime(df_k["dt"])
    # 保留最近3个月K线
    end_dt = df_k["dt"].max()
    start_dt = end_dt - pd.DateOffset(months=3)
    df_k = df_k[df_k["dt"] >= start_dt].copy()
    # 平盘K线变红
    df_k.loc[df_k["Close"] == df_k["Open"], "Close"] += 0.0001
    # 最新价 & 涨幅映射
    last_df = df_k.sort_values("dt").groupby("code").last()[["Close", "rise"]]
    price_map = last_df["Close"].round(2).to_dict()
    rise_map = last_df["rise"].round(2).to_dict()
    # 4. 多线程绘图
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
    # 5. 按行业分组（按股票数从多到少）
    df["industry"] = df["industry"].fillna("未分类")
    ind_cnt = df["industry"].value_counts().sort_values(ascending=False)
    industries = ind_cnt.index.tolist()
    # 6. 生成HTML
    print("🌍 生成HTML...")
    html = '''
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <title>一日持股法</title>
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
                一日持股法核心原则：<br/>
                尾盘买入，次日早盘择机卖出；严格执行止损止盈
            </div>
            <h1 class="title">📌 一日持股法 K线看板</h1>
            <div class="col-switch">
                <button class="col-btn" onclick="changeColumns(2)">2列</button>
                <button class="col-btn active" onclick="changeColumns(3)">3列</button>
                <button class="col-btn" onclick="changeColumns(4)">4列</button>
                <button class="col-btn" onclick="changeColumns(5)">5列</button>
            </div>
            <div class="tab-wrap">
                <div class="tabs">
    '''
    # 行业TAB
    for i, ind in enumerate(industries):
        active = "active" if i == 0 else ""
        html += f'<button class="tab {active}" onclick="setTab({i})">{ind}({ind_cnt[ind]})</button>'
    html += '</div></div>'
    # 行业卡片
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
            price_str = f"({price}元)" if price else ""
            rise_cls = "rise-red" if rise_val >= 0 else "rise-green"
            rise_str = f'<span class="{rise_cls}">{rise_val:+.2f}%</span>'
            turnover_str = f'<span style="color:red;font-weight:bold;margin-left:6px;">换手:{r["turnover_rate"]:.2f}%</span>'
            ratio_str = f'<span style="color:red;font-weight:bold;margin-left:6px;">量比:{r["ratio"]:.2f}</span>'
            html += f'''
            <div class="card">
                <div class="stock-title">{code} {r["stock_name"]}<span class="price">{price_str}</span>{rise_str}{turnover_str}{ratio_str}</div>
                <div class="sub" style="color:red; font-weight:bold;">
                    5日涨幅: {r["rise_5"]:.2f}% ｜ 10日涨幅: {r["rise_10"]:.2f}% ｜ 15日涨幅: {r["rise_15"]:.2f}%
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
    filename = f"../html/{datetime.now().strftime('%Y-%m-%d')}_一日持股法.html"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"✅ 完成！文件已生成：{filename}")

if __name__ == "__main__":

    start_time = time.time()
    today = datetime.now().strftime("%Y-%m-%d")
    # today='2026-08-18'
    generate_html(today)
    end_time = time.time()
    cost_time = end_time - start_time
    print(f"程序总耗时：{cost_time:.2f} 秒")