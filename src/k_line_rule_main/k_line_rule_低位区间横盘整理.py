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
    except Exception:
        return ""

# ======================================================================================
# ✅ 修复：SQL增加 stock_name 获取股票名称，解决KeyError
# 策略：
# 1.最近2个交易日涨跌幅绝对值均≤2.5%
# 2.30交易日内最低价 / 当前收盘价 在 0.9 ~1.1
# 3.30交易日内最高价 >= 当前收盘价 *1.2
# 板块内排序：price_highest_close_times 降序，数值越大越靠前
# 卡片顶部红色文字展示：最高价XX倍最新价，最低价XX倍最新价
# ======================================================================================
def generate_low_osc_html():
    print("📥 加载低震荡选股数据...")
    # 1. 使用你写好的SQL，所有%改为%%，增加 stock_name
    sql_filter = """
select 
    a.code,
    a.stock_name,
    -- 最高价是当前价的多少倍(限定条件是 [1.2 ~ ∞) 倍以上)
    round(b.price_highest / a.price_close, 2) as price_highest_close_times,
    -- 最低价是当前价的多少倍(限定条件是 [0.9 ~ 1.1] 倍之间)
    round(b.price_lowest / a.price_close, 2) as price_lowest_close_times,
    a.price_close
from (
    select 
        a.code,
        b.stock_name,
        -- 最新交易日价格
        b.price_close
    from (
        -- step 1 选取最近 2 个交易日 & 且涨幅绝对值均在 2.5% 以内涨幅的
        select
            code
        from (
            select
                code,
                count(1) as num
            from (
                select 
                    dt, 
                    code, 
                    abs(rise) as rise,
                    row_number() over(partition by code order by dt desc) as s
                from stock_detail
                where code not like '688%%'
                    and upper(stock_name) not like '%%ST%%'
            ) t 
            where s<=2 and rise<=2.5
            group by code
        ) t 
        where num=2
    ) a join (
        select 
            code, stock_name, price_close
        from stock_detail
        where dt=(select max(dt) from stock_detail) 
            and code not like '688%%'
            and upper(stock_name) not like '%%ST%%'
    ) b on a.code=b.code
) a join (
    select
        code,
        -- 30 个交易日内最高价
        max(price_highest) as price_highest,
        -- 30 个交易日内最低价
        min(price_lowest) as price_lowest
    from (
        select 
            dt, 
            code, 
            price_highest,
            price_lowest,
            row_number() over(partition by code order by dt desc) as s
        from stock_detail
        where code not like '688%%'
            and upper(stock_name) not like '%%ST%%'
    ) t 
    where s<=30
    group by code
-- 30个交易日内，最低点的价格是当前价格的 [0.9, 1.1] 倍 -> 做低点策略
-- 30个交易日内，最高点的价格是当前价格的 1.2 倍以上 -> 目的去除掉 30 个交易日内不活跃的股票
) b on a.code=b.code 
    and b.price_lowest * 1.1>=a.price_close 
    and b.price_lowest*0.9<=a.price_close
    and b.price_highest>=a.price_close*1.2;
    """
    df = pd.read_sql(text(sql_filter), engine)
    if df.empty:
        print("❌ 暂无符合条件的个股")
        return
    print(f"✅ 筛选出符合条件股票数量：{len(df)}")

    # 关联行业标签
    sql_tag = """
        SELECT code, industry, industry_detail
        FROM dim_stock_tag
    """
    df_tag = pd.read_sql(text(sql_tag), engine)
    df_tag["code_clean"] = df_tag["code"].str.replace("sh","").str.replace("sz","").str.lower()
    df["code"] = df["code"].astype(str)
    df_merge = pd.merge(
        df,
        df_tag[["code_clean","industry","industry_detail"]],
        left_on="code",
        right_on="code_clean",
        how="left"
    )
    df_merge["industry"] = df_merge["industry"].fillna("未分类")

    # 板块内：按 price_highest_close_times 降序排列，越大越靠前
    df_merge = df_merge.sort_values(["industry", "price_highest_close_times"], ascending=[True, False])

    # 3. 加载候选股K线数据（绘图用）
    codes = df_merge["code"].unique().tolist()
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
    df_k = pd.read_sql(text(k_sql), engine)
    df_k["dt"] = pd.to_datetime(df_k["dt"])
    # 绘图取近3个月K线
    end_dt = df_k["dt"].max()
    start_dt = end_dt - pd.DateOffset(months=3)
    df_k = df_k[df_k["dt"] >= start_dt].copy()
    # 平盘K线变红
    df_k.loc[df_k["Close"] == df_k["Open"], "Close"] += 0.0001

    # 获取最新涨跌幅映射
    last_df = df_k.sort_values("dt").groupby("code").last()[["Close", "rise"]]
    rise_map = last_df["rise"].round(2).to_dict()

    # 4. 多线程绘制K线图
    print("🖼️ 开始批量绘制K线图...")
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

    # 板块tab：板块按股票数量降序
    ind_cnt = df_merge["industry"].value_counts().sort_values(ascending=False)
    industries = ind_cnt.index.tolist()

    # 6. 生成HTML页面
    print("🌍 生成选股HTML页面...")
    html = '''
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <title>近期窄幅震荡，30日高点≥现价1.2倍 K线看板</title>
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
                选股规则：<br/>
                1.最近连续2个交易日涨跌幅绝对值都在2.5%以内<br/>
                2.近30交易日最低价/现价 0.9~1.1倍<br/>
                3.近30交易日最高价 ≥ 现价1.2倍<br/>
                板块内排序：最高价相对现价倍数越大，越靠前
            </div>
            <h1 class="title">📊 窄幅震荡蓄势个股 K线看板</h1>
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

    # 板块卡片渲染
    for i, ind in enumerate(industries):
        active = "active" if i == 0 else ""
        html += f'<div class="tab-content {active}">'
        sub_df = df_merge[df_merge["industry"] == ind]
        for _, r in sub_df.iterrows():
            code = r["code"]
            img = img_map.get(code, "")
            if not img:
                continue
            price_close = round(r["price_close"],2)
            rise_val = rise_map.get(code, 0)
            highest_times = r["price_highest_close_times"]
            lowest_times = r["price_lowest_close_times"]
            rise_cls = "rise-red" if rise_val >= 0 else "rise-green"
            rise_str = f'<span class="{rise_cls}">{rise_val:+.2f}%</span>'
            html += f'''
            <div class="card">
                <!-- 【要求】红色字体展示倍数信息 -->
                <div style="color:red; font-weight:bold;">
                    最高价{highest_times}倍最新价，最低价{lowest_times}倍最新价
                </div>
                <div class="stock-title">{code} {r["stock_name"]}<span class="price">{price_close}元</span>{rise_str}</div>
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
    filename = f"../html/{datetime.now().strftime('%Y-%m-%d')}_窄幅区间震荡蓄势股票.html"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"✅ 选股HTML生成完成！文件路径：{filename}")

if __name__ == "__main__":
    start_time = time.time()
    generate_low_osc_html()
    end_time = time.time()
    cost_time = end_time - start_time
    print(f"程序总耗时：{cost_time:.2f} 秒")
