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


def fast_section_plot(df_sec):
    """绘制板块K线，df_sec必须包含 dt,Open,High,Low,Close,Volume"""
    try:
        fig, ax = mpf.plot(
            df_sec, type="candle", volume=True, style=s_style,
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
        print(f"板块绘图异常:{e}")
        return ""


def generate_section_html():
    print("📥 加载板块数据 section_detail ...")
    # 获取最新交易日
    last_dt_df = pd.read_sql("SELECT MAX(dt) as dt FROM section_detail", engine)
    last_dt = last_dt_df.iloc[0]["dt"]

    # 获取当日全部板块基础信息
    sql_section_latest = f"""
    SELECT
        dt,
        section_name,
        rise,
        rise_5day,
        rise_10day,
        rise_20day,
        up_num,
        add_num,
        down_num,
        leader_stock,
        ratio,
        trade_amount
    FROM section_detail
    WHERE dt = '{last_dt}'
    ORDER BY rise DESC
    """
    df_latest = pd.read_sql(sql_section_latest, engine)
    if df_latest.empty:
        print("❌ 没有板块数据")
        return

    section_name_list = df_latest["section_name"].unique().tolist()

    # 查询板块近3个月全部历史数据
    name_placeholder = ",".join([f"'{n}'" for n in section_name_list])
    history_sql = f"""
    SELECT
        dt,
        section_name,
        rise,
        trade as Volume
    FROM section_detail
    WHERE section_name IN ({name_placeholder})
    ORDER BY section_name, dt
    """
    df_history = pd.read_sql(history_sql, engine)
    df_history["dt"] = pd.to_datetime(df_history["dt"])

    end_dt = df_history["dt"].max()
    start_dt = end_dt - pd.DateOffset(months=3)
    df_history = df_history[df_history["dt"] >= start_dt].copy()

    # 板块没有真实Open High Low Close；根据rise模拟价格序列用于K线绘图
    img_map = {}

    def plot_one_section(sec_name):
        sub = df_history[df_history["section_name"] == sec_name].copy().sort_values("dt")
        if len(sub) < 5:
            return sec_name, ""
        base_price = 100.0
        close_list = []
        for _, row in sub.iterrows():
            if len(close_list) == 0:
                new_close = base_price
            else:
                new_close = close_list[-1] * (1.0 + row["rise"] / 100.0)
            close_list.append(new_close)
        sub["Close"] = close_list
        # 模拟 OHLC：板块无真实高低，做小幅波动模拟K线形态
        sub["Open"] = sub["Close"].shift(1)
        sub.loc[sub.index[0], "Open"] = sub.loc[sub.index[0], "Close"]
        sub["High"] = sub[["Open", "Close"]].max(axis=1) * 1.003
        sub["Low"] = sub[["Open", "Close"]].min(axis=1) * 0.997

        sub.set_index("dt", inplace=True)
        img_b64 = fast_section_plot(sub)
        return sec_name, img_b64

    print("🖼️ 开始绘制板块K线图...")
    with ThreadPoolExecutor(max_workers=6) as executor:
        results = list(executor.map(plot_one_section, section_name_list))
    for sec, img in results:
        img_map[sec] = img

    print("🌍 生成板块HTML页面...")
    html = '''
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <title>板块K线看板</title>
        <style>
            *{box-sizing:border-box;margin:0;padding:0;font-family:Microsoft YaHei}
            body{background:#f5f7fa;padding:20px}
            .container{max-width:1900px;margin:0 auto}
            .title{text-align:center;margin-bottom:20px}
            .col-switch{display:flex;gap:8px;justify-content:center;margin-bottom:20px}
            .col-btn{padding:10px 20px;border:none;border-radius:6px;background:#e3e6ed;cursor:pointer}
            .col-btn.active{background:#2f80ed;color:white}
            .card-wrap{display:grid;grid-template-columns:repeat(3,1fr);gap:16px}
            .card{background:white;padding:12px;border-radius:12px}
            .card img{width:100%;border-radius:8px;margin-top:10px}
            .sec-title{font-weight:bold;font-size:15px}
            .rise-red{color:#e63946;font-size:14px;margin-left:4px}
            .rise-green{color:#28a745;font-size:14px;margin-left:4px}
            .sub{font-size:12px;color:#666;margin-top:4px;line-height:1.6}
        </style>
    </head>
    <body>
        <div class="container">
            <h1 class="title">📈 板块K线看板</h1>
            <div class="col-switch">
                <button class="col-btn" onclick="changeColumns(2)">2列</button>
                <button class="col-btn active" onclick="changeColumns(3)">3列</button>
                <button class="col-btn" onclick="changeColumns(4)">4列</button>
                <button class="col-btn" onclick="changeColumns(5)">5列</button>
            </div>
            <div class="card-wrap">
    '''
    for _, row in df_latest.iterrows():
        sec_name = row["section_name"]
        img_data = img_map.get(sec_name, "")
        if not img_data:
            continue
        rise_val = row["rise"]
        rise_cls = "rise-red" if rise_val >= 0 else "rise-green"
        rise5 = round(row["rise_5day"],2)
        rise10 = round(row["rise_10day"],2)
        rise20 = round(row["rise_20day"],2)
        up = row["up_num"]
        add = row["add_num"]
        down = row["down_num"]
        lead = row["leader_stock"] if row["leader_stock"] else "-"

        html += f'''
        <div class="card">
            <div class="sec-title">{sec_name}
                <span class="{rise_cls}">{rise_val:+.2f}%</span>
            </div>
            <div class="sub" style="color:red;font-weight:bold;">
                5日:{rise5}%｜10日:{rise10}%｜20日:{rise20}%
            </div>
            <div class="sub">涨停:{up} 上涨:{add} 下跌:{down}｜领涨股:{lead}</div>
            <img src="{img_data}">
        </div>
        '''

    html += '''
            </div>
        </div>
        <script>
            function changeColumns(col){
                document.querySelector('.card-wrap').style.gridTemplateColumns = `repeat(${col},1fr)`;
                document.querySelectorAll('.col-btn').forEach(btn=>{
                    btn.classList.toggle('active', parseInt(btn.innerText[0]) === col);
                })
            }
        </script>
    </body></html>
    '''
    out_file = f"../html/{datetime.now().strftime('%Y-%m-%d')}_板块K线看板.html"
    with open(out_file, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"✅ 板块HTML生成完成：{out_file}")


if __name__ == "__main__":
    t0 = time.time()
    generate_section_html()
    t1 = time.time()
    print(f"总耗时 {t1-t0:.2f} 秒")
