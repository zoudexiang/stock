"""
@Author  : zoudexiag
@Date    : 2026/7/1
@Time    : 20:20
@Desc    : 自定义股票列表绘制 k 线
"""
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

# ====================== MySQL 配置 ======================
MYSQL_HOST = constants.db_config['host']
MYSQL_USER = constants.db_config['user']
MYSQL_PASSWORD = constants.db_config['password']
MYSQL_DB = constants.db_config['database']
# ========================================================

# 屏蔽警告、中文显示配置
warnings.filterwarnings("ignore")
plt.set_loglevel("error")
plt.rcParams['figure.max_open_warning'] = 0
plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']
plt.rcParams['axes.unicode_minus'] = False

# 数据库连接
engine = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DB}?charset=utf8mb4"
)

# A股涨红跌绿配色
mc = mpf.make_marketcolors(
    up='r',
    down='g',
    edge='inherit',
    wick='inherit',
    volume='inherit'
)
s_style = mpf.make_mpf_style(marketcolors=mc, gridstyle='')

# 绘图函数
def fast_plot(df):
    try:
        fig, _ = mpf.plot(
            df, type="candle", volume=True, style=s_style,
            figratio=(10, 5), figscale=0.7,
            returnfig=True
        )
        buf = BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight", dpi=80)
        buf.seek(0)
        img_base64 = base64.b64encode(buf.read()).decode()
        plt.close(fig)
        return f"data:image/png;base64,{img_base64}"
    except Exception:
        return ""

def generate_custom_html(tag_codes):
    print("📥 读取自定义股票数据...")
    # 合并所有分组的全部代码，一次性查库
    all_total_codes = []
    tag_code_map = {}
    tag_name_list = []
    for tag_name, code_list in tag_codes:
        tag_name_list.append(tag_name)
        tag_code_map[tag_name] = code_list
        all_total_codes.extend(code_list)
    # 去重，避免重复查询K线
    all_total_codes = list(set(all_total_codes))

    # 获取最新交易日
    last_dt = pd.read_sql("select max(dt) as dt from stock_detail", engine).iloc[0]["dt"]
    code_quote = ",".join([f"'{c}'" for c in all_total_codes])

    # 查询个股基础信息
    sql_info = f"""
    select
        s.code,
        s.stock_name,
        s.rise_5,
        s.rise_10,
        s.rise_15,
        s.price_close,
        s.rise,
        dst.industry,
        dst.industry_detail
    from stock_detail s
    left join dim_stock_tag dst on replace(replace(lower(dst.code), 'sz', ''), 'sh', '') = s.code
    where s.dt = '{last_dt}' and s.code IN ({code_quote})
    """
    df_info = pd.read_sql(sql_info, engine)
    if df_info.empty:
        print("❌ 未查询到对应股票数据")
        return

    # 股票信息字典 code -> row
    stock_row_dict = {row["code"]: row for _, row in df_info.iterrows()}

    # 读取K线数据
    code_k_quote = ",".join([f"'{c}'" for c in all_total_codes])
    sql_kline = f"""
    select 
        dt, code,
        price_open as Open, price_close as Close,
        price_highest as High, price_lowest as Low,
        trade_amount as Volume, rise
    from stock_detail
    where code in ({code_k_quote})
    order by code, dt
    """
    df_k = pd.read_sql(sql_kline, engine)
    df_k["dt"] = pd.to_datetime(df_k["dt"])

    # 只保留近3个月K线
    end_date = df_k["dt"].max()
    start_date = end_date - pd.DateOffset(months=3)
    df_k = df_k[df_k["dt"] >= start_date].copy()
    # 平盘K线变红
    df_k.loc[df_k["Close"] == df_k["Open"], "Close"] += 0.0001

    # 构建价格、当日涨幅映射
    latest_k = df_k.sort_values("dt").groupby("code").last()[["Close", "rise"]]
    price_map = latest_k["Close"].round(2).to_dict()
    rise_map = latest_k["rise"].round(2).to_dict()

    # 多线程批量绘图
    print("🖼️ 绘制K线图...")
    img_map = {}
    def single_draw(code):
        df_sub = df_k[df_k["code"] == code].copy()
        if len(df_sub) < 5:
            return code, ""
        df_sub.set_index("dt", inplace=True)
        return code, fast_plot(df_sub)

    with ThreadPoolExecutor(max_workers=6) as executor:
        result_list = list(executor.map(single_draw, all_total_codes))
    for c, img_str in result_list:
        img_map[c] = img_str

    # 生成HTML页面
    print("🌍 生成HTML文件...")
    html_template = '''
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <title>分组股票 K 线看板</title>
        <style>
            *{box-sizing:border-box;margin:0;padding:0;font-family:Microsoft YaHei}
            body{background:#f5f7fa;padding:20px}
            .container{max-width:1900px;margin:0 auto}
            .title{text-align:center;margin-bottom:20px}
            .col-switch{display:flex;gap:8px;justify-content:center;margin-bottom:20px}
            .col-btn{padding:10px 20px;border:none;border-radius:6px;background:#e3e6ed;cursor:pointer}
            .col-btn.active{background:#2f80ed;color:white}

            /* Tab样式 */
            .tab-group{display:flex;gap:10px;justify-content:center;margin-bottom:20px}
            .tab-btn{padding:9px 22px;border:none;border-radius:6px;background:#e8ebf0;cursor:pointer;font-size:15px}
            .tab-btn.active{background:#2f80ed;color:#fff}

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
            <h1 class="title">分组股票 K 线看板</h1>
            <div class="col-switch">
                <button class="col-btn" onclick="changeColumns(2)">2列</button>
                <button class="col-btn active" onclick="changeColumns(3)">3列</button>
                <button class="col-btn" onclick="changeColumns(4)">4列</button>
                <button class="col-btn" onclick="changeColumns(5)">5列</button>
            </div>

            <!-- Tab按钮区域 -->
            <div class="tab-group">
    '''
    # 渲染Tab按钮
    for idx, tag_name in enumerate(tag_name_list):
        active_cls = "active" if idx == 0 else ""
        html_template += f'<button class="tab-btn {active_cls}" onclick="switchTab({idx})">{tag_name}</button>'
    html_template += '''
            </div>
    '''

    # 渲染每个Tab对应的卡片内容
    for idx, (tag_name, code_list) in enumerate(tag_codes):
        active_cls = "active" if idx == 0 else ""
        html_template += f'<div class="tab-content {active_cls}" id="tab_{idx}">'
        # 按当前分组内代码顺序渲染
        for code in code_list:
            if code not in stock_row_dict:
                continue
            row = stock_row_dict[code]
            img_data = img_map.get(code, "")
            if not img_data:
                continue
            close_price = price_map.get(code, "")
            today_rise = rise_map.get(code, 0)
            price_text = f"({close_price}元)" if close_price else ""
            rise_class = "rise-red" if today_rise >= 0 else "rise-green"
            rise_text = f'<span class="{rise_class}">{today_rise:+.2f}%</span>'

            html_template += f'''
            <div class="card">
                <div class="stock-title">{code} {row["stock_name"]}<span class="price">{price_text}</span>{rise_text}</div>
                <div class="sub" style="color:red;font-weight:bold;">
                    5日涨幅: {row["rise_5"]:.2f}% ｜ 10日涨幅: {row["rise_10"]:.2f}% ｜ 15日涨幅: {row["rise_15"]:.2f}%
                </div>
                <div class="sub">{row["industry"]} | {row["industry_detail"]}</div>
                <img src="{img_data}">
            </div>
            '''
        html_template += "</div>"

    html_template += '''
        </div>
        <script>
            // Tab切换
            function switchTab(tabIdx){
                // 隐藏所有内容
                document.querySelectorAll('.tab-content').forEach(box => box.classList.remove('active'));
                // 取消按钮激活
                document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
                // 激活目标
                document.getElementById(`tab_${tabIdx}`).classList.add('active');
                event.target.classList.add('active');
            }
            // 全局切换列数
            function changeColumns(col) {
                document.querySelectorAll(".tab-content").forEach(wrap=>{
                    wrap.style.gridTemplateColumns = `repeat(${col},1fr)`;
                })
                document.querySelectorAll(".col-btn").forEach(btn=>{
                    btn.classList.toggle("active", parseInt(btn.innerText[0])===col);
                })
            }
        </script>
    </body></html>
    '''
    # 保存文件
    save_path = f"../html/{datetime.now().strftime('%Y-%m-%d')}_分组股票K线.html"
    with open(save_path, "w", encoding="utf-8") as f:
        f.write(html_template)
    print(f"✅ 生成完成：{save_path}")

if __name__ == "__main__":
    # ====================== 修改这里：[(分组名称, [股票代码数组]), ...] ======================
    tag_codes = [
        ('近 10 日涨幅居高 + 连续至少 1 天下跌股票 K 线看板', ['300489', '300909', '600552', '300721', '603823', '603989', '301310', '002965', '603358', '600545', '300285', '301526', '600176']),
        ('核心股票 K 线看板', ['301392', '600584', '002185', '000021', '603629', '600378', '002971', '002428', '600301', '600961', '605358', '300554', '301217', '301511', '300408', '002636', '605006', '000725'])
    ]

    start = time.time()
    generate_custom_html(tag_codes)
    end = time.time()
    print(f"执行耗时：{end - start:.2f} 秒")