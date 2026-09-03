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

# A股配色：涨红跌绿
mc = mpf.make_marketcolors(
    up='r',
    down='g',
    edge='inherit',
    wick='inherit',
    volume='inherit'
)
s_style = mpf.make_mpf_style(marketcolors=mc, gridstyle='')


def fast_plot(df):
    """绘制K线返回base64图片"""
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
        return ""


def generate_first_limit_up_html():
    print("📥 加载20日内首次涨停个股数据...")

    # 完整原始SQL
    sql = """
    select
        first_upper_dt,
        a.dt,
        a.code,
        a.stock_name,
        open,
        close,
        high,
        low,
        volume,
        rise,
        industry,
        industry_detail,
        row_number() over(partition by a.code order by a.dt asc) as s
    from (
        select 
            dt,
            code,
            stock_name, 
            price_open as open, 
            price_close as close,
            price_highest as high, 
            price_lowest as low,
            trade_amount as volume, 
            rise
        from stock_detail
        where (
                dt>=date_sub(curdate(), interval 20 day)
                and code not like '688%%' 
                and upper(stock_name) not like '%%ST%%' 
                and code not like '30%%'
              )
            or 
            (
                dt>=date_sub(curdate(), interval 20 day) 
                and code like '30%%' 
                and upper(stock_name) not like '%%ST%%'
            )
    ) a join (
        select 
            code,
            min(dt) as first_upper_dt
        from stock_detail
        where (
                dt>=date_sub(curdate(), interval 20 day) 
                and rise>=9.8 
                and code not like '688%%' 
                and upper(stock_name) not like '%%ST%%' 
                and code not like '30%%'
              )
            or 
            (
                dt>=date_sub(curdate(), interval 20 day) 
                and rise>=19.8 
                and code like '30%%' 
                and upper(stock_name) not like '%%ST%%'
            )
        group by code
    ) b on dt>=first_upper_dt and a.code=b.code
    left join (
        select
            code,
            industry,
            industry_detail
        from dim_stock_tag
    ) c on replace(replace(lower(c.code), 'sz', ''), 'sh', '')=a.code
    """
    df_raw = pd.read_sql(text(sql), engine)
    if df_raw.empty:
        print("❌ 暂无20日内首次涨停股票")
        return

    # 取每只股票最新一条记录拿到 first_upper_dt、s、行业信息
    df_latest = df_raw.sort_values("dt").groupby("code").last().reset_index()
    df_latest["industry"] = df_latest["industry"].fillna("未分类")

    # 需要绘图的股票code列表
    codes = df_latest["code"].unique().tolist()

    # 加载K线数据：最近3个月用于画图
    codes_quote = ",".join([f"'{c}'" for c in codes])
    k_sql = f"""
        SELECT dt, code,
               price_open AS Open, price_close AS Close,
               price_highest AS High, price_lowest AS Low,
               trade_amount AS Volume, rise
        FROM stock_detail
        WHERE code IN ({codes_quote})
        ORDER BY code, dt
    """
    df_k = pd.read_sql(k_sql, engine)
    df_k["dt"] = pd.to_datetime(df_k["dt"])
    end_dt = df_k["dt"].max()
    start_dt = end_dt - pd.DateOffset(months=3)
    df_k = df_k[df_k["dt"] >= start_dt].copy()
    # 平盘K线处理，避免一字平盘颜色异常
    df_k.loc[df_k["Close"] == df_k["Open"], "Close"] += 0.0001

    # 最新价格、涨跌幅
    last_price_df = df_k.sort_values("dt").groupby("code").last()[["Close", "rise"]]
    price_map = last_price_df["Close"].round(2).to_dict()
    rise_map = last_price_df["rise"].round(2).to_dict()

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
    for c, img_b64 in res:
        img_map[c] = img_b64

    # 按行业统计排序（tab按钮仍然按行业个股数量降序）
    ind_cnt = df_latest["industry"].value_counts().sort_values(ascending=False)
    industries = ind_cnt.index.tolist()

    print("🌍 生成HTML看板文件...")
    html = '''
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <title>20日内首次涨停个股K线看板</title>
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
            .stock-title{font-weight:bold;font-size:15px}
            .price{color:#e63946;font-size:14px;margin-left:6px}
            .rise-green{color:#28a745;font-size:14px;margin-left:4px}
            .rise-red{color:#e63946;font-size:14px;margin-left:4px}
            .sub{font-size:12px;color:#888;margin-top:4px}
            .limit-notice{color:red;font-weight:bold;font-size:13px;margin-top:6px;margin-bottom:4px}
        </style>
    </head>
    <body>
        <div class="container">
            <h1 class="title">🔥 20日内首次涨停个股 K线看板（板块内部按距今s天升序，天数越小越靠前）</h1>
            <div class="col-switch">
                <button class="col-btn" onclick="changeColumns(2)">2列</button>
                <button class="col-btn active" onclick="changeColumns(3)">3列</button>
                <button class="col-btn" onclick="changeColumns(4)">4列</button>
                <button class="col-btn" onclick="changeColumns(5)">5列</button>
            </div>
            <div class="tab-wrap">
                <div class="tabs">
    '''
    # 生成行业Tab按钮
    for i, ind in enumerate(industries):
        active_cls = "active" if i == 0 else ""
        html += f'<button class="tab {active_cls}" onclick="setTab({i})">{ind}({ind_cnt[ind]})</button>'
    html += '</div></div>'

    # 每个行业卡片内容：【重点修改】板块内部按照 s 升序排序，天数小排在前面
    for tab_idx, ind in enumerate(industries):
        active_cls = "active" if tab_idx == 0 else ""
        html += f'<div class="tab-content {active_cls}">'
        sub_df = df_latest[df_latest["industry"] == ind].copy()
        # ✅核心改动：同一行业内部 s升序（距今天数越小越靠前）
        sub_df = sub_df.sort_values(by="s", ascending=True)
        for _, row in sub_df.iterrows():
            code = row["code"]
            img_src = img_map.get(code, "")
            if not img_src:
                continue
            stock_name = row["stock_name"]
            fu_dt = row["first_upper_dt"]
            s_day = row["s"]
            industry_detail = row["industry_detail"] if pd.notna(row["industry_detail"]) else ""

            close_price = price_map.get(code, "")
            price_str = f"({close_price}元)" if close_price else ""
            rise_val = rise_map.get(code, 0)
            rise_css = "rise-red" if rise_val >= 0 else "rise-green"
            rise_html = f'<span class="{rise_css}">{rise_val:+.2f}%</span>'

            html += f'''
            <div class="card">
                <div class="stock-title">{code} {stock_name}<span class="price">{price_str}</span>{rise_html}</div>
                <div class="limit-notice">首次涨停：{fu_dt}，距今 {int(s_day)} 天</div>
                <div class="sub">{industry_detail}</div>
                <img src="{img_src}">
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
                    e.classList.toggle('active',j===i);
                    document.querySelectorAll('.tab')[j].classList.toggle('active',j===i);
                });
            }
        </script>
    </body></html>
    '''
    out_file = f"../html/{datetime.now().strftime('%Y-%m-%d')}_首次涨停个股看板.html"
    with open(out_file, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"✅ 文件输出完成：{out_file}")


if __name__ == "__main__":
    t_start = time.time()
    generate_first_limit_up_html()
    t_end = time.time()
    print(f"总耗时：{t_end - t_start:.2f} 秒")
