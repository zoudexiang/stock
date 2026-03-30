from datetime import datetime
import pandas as pd
import mplfinance as mpf
import matplotlib.pyplot as plt
import warnings
from io import BytesIO
import base64
from sqlalchemy import create_engine
from concurrent.futures import ThreadPoolExecutor

from src.utils import constants

# ====================== 数据库配置 ======================
MYSQL_HOST = constants.db_config['host']
MYSQL_USER = constants.db_config['user']
MYSQL_PASSWORD = constants.db_config['password']
MYSQL_DB = constants.db_config['database']
MYSQL_PORT = constants.db_config.get('port', 3306)

# -------------------- 屏蔽警告 --------------------
warnings.filterwarnings("ignore")
plt.rcParams['axes.unicode_minus'] = False
plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']

# ====================== 核心加载数据 ======================
def load_data():
    engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}")

    # 1. 获取最新计算日期 calc_dt
    print("📥 获取最新计算日期 calc_dt...")
    max_calc_dt = pd.read_sql("SELECT MAX(calc_dt) AS max_dt FROM stock_detail_calc", engine).iloc[0]['max_dt']
    print(f"✅ 最新计算日期：{max_calc_dt}")

    # 2. 取出该日期下数据，并按 code 去重，保留最大 dt（分歧日）
    print("📥 加载股票列表（按code去重，保留最大分歧日）...")
    df_calc = pd.read_sql(f"""
        SELECT t1.* 
        FROM stock_detail_calc t1
        INNER JOIN (
            SELECT code, MAX(dt) AS max_dt
            FROM stock_detail_calc
            WHERE calc_dt = '{max_calc_dt}'
            GROUP BY code
        ) t2 ON t1.code = t2.code AND t1.dt = t2.max_dt
        WHERE t1.calc_dt = '{max_calc_dt}'
    """, engine)

    code_list = df_calc["code"].tolist()
    placeholders = ",".join([f"'{c}'" for c in code_list])

    # 3. 读取这些股票的日K数据（最近100天）
    print("📥 加载日K数据（最近100天）...")
    df_k = pd.read_sql(f"""
        SELECT dt, code, stock_name,
               price_open AS Open, price_close AS Close,
               price_highest AS High, price_lowest AS Low,
               trade AS Volume, rise
        FROM stock_detail
        WHERE code IN ({placeholders})
        ORDER BY code, dt ASC
    """, engine)

    df_k["dt"] = pd.to_datetime(df_k["dt"])

    # 4. 只保留最近 100 天
    last_date = df_k["dt"].max()
    start_date = last_date - pd.DateOffset(days=100)
    df_k = df_k[(df_k["dt"] >= start_date)].copy()

    # 5. 平盘强制变红（Open == Close → 涨）
    df_k.loc[df_k["Close"] == df_k["Open"], "Close"] += 0.0001

    # 6. 最新价格 + 最新涨幅
    last_data = df_k.sort_values("dt").groupby("code").last()[["Close", "rise", "stock_name"]]
    price_map = last_data["Close"].round(2).to_dict()
    rise_map = (last_data["rise"] * 100).round(2).to_dict()
    name_map = last_data["stock_name"].to_dict()

    # 7. 分歧日映射（每个code对应的最大dt）
    diff_dt_map = df_calc.set_index("code")["dt"].to_dict()

    engine.dispose()
    return df_calc, df_k, price_map, rise_map, name_map, diff_dt_map

# ====================== K线绘图 ======================
def plot_kline(df):
    mc = mpf.make_marketcolors(up='r', down='g', edge='inherit', wick='inherit', volume='inherit')
    s = mpf.make_mpf_style(marketcolors=mc, gridstyle='')

    try:
        fig, ax = mpf.plot(
            df, type='candle', volume=True, style=s,
            figratio=(11, 5), figscale=0.7, returnfig=True
        )
        buf = BytesIO()
        fig.savefig(buf, format='png', bbox_inches='tight', dpi=80)
        buf.seek(0)
        img_str = base64.b64encode(buf.read()).decode()
        plt.close(fig)
        return f"data:image/png;base64,{img_str}"
    except:
        return ""

# ====================== 生成HTML ======================
def generate_html():
    df_calc, df_k, price_map, rise_map, name_map, diff_dt_map = load_data()

    # 行业按股票数量【从多到少排序】
    industry_count = df_calc["industry"].value_counts().sort_values(ascending=False)
    industries = industry_count.index.tolist()

    print("🖼️ 开始生成K线图片...")
    stock_image_map = {}

    def process(code):
        sub = df_k[df_k["code"] == code].copy()
        if len(sub) < 5:
            return code, ""
        sub.set_index("dt", inplace=True)
        return code, plot_kline(sub)

    with ThreadPoolExecutor(6) as executor:
        results = executor.map(process, df_calc["code"].unique())
    for code, img in results:
        stock_image_map[code] = img

    # ====================== 拼接HTML ======================
    html = '''
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <title>倍量回调 - 踩上分歧日 K线图</title>
        <style>
            *{box-sizing:border-box;margin:0;padding:0;font-family:Microsoft YaHei}
            body{background:#f5f7fa;padding:20px}
            .container{max-width:2000px;margin:auto}
            .title{text-align:center;font-size:24px;margin-bottom:20px}
            .col-switch{display:flex;gap:10px;justify-content:center;margin-bottom:20px}
            .col-btn{padding:10px 20px;border:0;border-radius:6px;background:#e3e6ed;cursor:pointer}
            .col-btn.active{background:#2f80ed;color:#fff}
            .tab-wrap{background:white;padding:15px;border-radius:10px;margin-bottom:20px}
            .tabs{display:flex;gap:8px;flex-wrap:wrap}
            .tab{padding:8px 16px;background:#f1f3f5;border:0;border-radius:6px;cursor:pointer}
            .tab.active{background:#2f80ed;color:white}
            .tab-content{display:none;grid-template-columns:repeat(4,1fr);gap:20px;padding-top:10px}
            .tab-content.active{display:grid}
            .card{background:white;padding:12px;border-radius:12px;box-shadow:0 2px 6px #00000010}
            .card img{width:100%;border-radius:8px;margin-top:10px}
            .stock-title{font-weight:bold;font-size:15px;margin-bottom:5px}
            .diff-day{color:#666;font-size:13px;margin-bottom:5px}
            .price{color:#e63946;margin-left:6px}
            .rise-red{color:#e63946;margin-left:4px}
            .rise-green{color:#28a745;margin-left:4px}
            .sub{color:#999;font-size:13px}
        </style>
    </head>
    <body>
    <div class="container">
        <h1 class="title">📊 倍量回调 - 踩上分歧日 近期100天K线</h1>
        <div class="col-switch">
            <button class="col-btn" onclick="setCol(3)">3列</button>
            <button class="col-btn active" onclick="setCol(4)">4列</button>
            <button class="col-btn" onclick="setCol(5)">5列</button>
        </div>
        <div class="tab-wrap"><div class="tabs">
    '''

    # 行业TAB
    for i, ind in enumerate(industries):
        active = "active" if i == 0 else ""
        cnt = industry_count[ind]
        html += f'<button class="tab {active}" onclick="setTab({i})">{ind}({cnt})</button>'

    html += '</div></div>'

    # 行业内容
    for i, ind in enumerate(industries):
        active = "active" if i == 0 else ""
        html += f'<div class="tab-content {active}">'
        for _, r in df_calc[df_calc["industry"] == ind].iterrows():
            code = r["code"]
            img = stock_image_map.get(code)
            if not img:
                continue

            # 数据获取
            close_price = price_map.get(code, "")
            rise_val = rise_map.get(code, 0.0)
            stock_name = name_map.get(code, "")
            diff_dt = diff_dt_map.get(code, "")
            ind_detail = r.get("industry_detail", "")

            # 样式
            rise_cls = "rise-red" if rise_val >= 0 else "rise-green"

            # 顶部标题（你要的格式）
            title_line = f'{code} {stock_name} <span class="price">{close_price}元</span> <span class="{rise_cls}">{rise_val:+.2f}%</span>'
            diff_line = f'分歧日：{diff_dt}'

            html += f'''
            <div class="card">
                <div class="stock-title">{title_line}</div>
                <div class="diff-day">{diff_line}</div>
                <div class="sub">{ind_detail}</div>
                <img src="{img}">
            </div>
            '''
        html += "</div>"

    # JS
    html += '''
    <script>
        function setCol(n){
            document.querySelectorAll('.tab-content').forEach(x=>{
                x.style.gridTemplateColumns=`repeat(${n},1fr)`
            })
            document.querySelectorAll('.col-btn').forEach((btn,i)=>{
                btn.classList.toggle('active',[3,4,5][i]==n)
            })
        }
        function setTab(i){
            document.querySelectorAll('.tab-content').forEach((x,j)=>x.classList.toggle('active',j==i))
            document.querySelectorAll('.tab').forEach((x,j)=>x.classList.toggle('active',j==i))
        }
        window.onload=()=>setCol(4)
    </script>
    </div></body></html>
    '''

    # 保存
    file_path = f"../html/{datetime.now().strftime('%Y-%m-%d')}_倍量回调分歧日 K 线图.html"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"✅ 生成完成！文件路径：{file_path}")

if __name__ == "__main__":
    generate_html()
