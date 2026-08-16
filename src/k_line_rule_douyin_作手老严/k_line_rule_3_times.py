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

# ====================== 基础配置 ======================
warnings.filterwarnings("ignore")
plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']
plt.rcParams['axes.unicode_minus'] = False

# K线颜色：涨红 跌绿
mc = mpf.make_marketcolors(up='r', down='g', edge='inherit', wick='inherit', volume='inherit')
s_style = mpf.make_mpf_style(marketcolors=mc, gridstyle='')

# ====================== 数据库连接 ======================
def get_engine():
    db = constants.db_config
    return create_engine(
        f"mysql+pymysql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}"
    )

# ====================== 核心策略选股 ======================
def select_stocks():
    engine = get_engine()
    print("📥 加载全量股票数据（确保足够交易日）...")

    # 读取足够长的数据，保证能计算 100 个交易日 + 5日均值
    df = pd.read_sql("""
        SELECT * FROM stock_detail
        WHERE dt >= DATE_SUB(CURDATE(), INTERVAL 180 DAY)
        ORDER BY code, dt
    """, engine)

    df["dt"] = pd.to_datetime(df["dt"])
    df = df.sort_values(["code", "dt"]).reset_index(drop=True)

    valid_codes = []
    target_map = {}  # 存储目标天信息

    print("🔍 执行策略：3倍放量阳线 + 今日第一次突破...")

    for code, group in df.groupby("code"):
        group = group.reset_index(drop=True)
        n = len(group)
        if n < 60:
            continue

        # ========== 规则1：计算5日成交额均值 ==========
        group["amount_5d"] = group["trade_amount"].rolling(5).mean().shift(1)
        group["is_up"] = group["price_close"] >= group["price_open"]
        group["is_target"] = (group["trade_amount"] >= 3 * group["amount_5d"]) & (group["is_up"])

        # 近100个交易日内的目标天
        targets = group[group["is_target"]].copy()
        if len(targets) == 0:
            continue
        last_target = targets.iloc[-1]
        target_idx = last_target.name
        target_close = last_target["price_close"]

        # ========== 只保留 100 个交易日内的目标天 ==========
        days_from_target = n - target_idx - 1
        if days_from_target > 100:
            continue

        # ========== 规则2：今日收盘价 >= 目标价 * 0.98 ==========
        latest = group.iloc[-1]
        latest_close = latest["price_close"]
        if latest_close < target_close * 0.98:
            continue

        # ========== 规则3：今日第一次突破（核心！） ==========
        post_target_rows = group.iloc[target_idx+1 : -1].copy()
        has_break_before = False
        for _, r in post_target_rows.iterrows():
            if r["price_close"] >= target_close * 0.98:
                has_break_before = True
                break
        if has_break_before:
            continue

        # ========== 全部满足：入选 ==========
        valid_codes.append(code)
        target_map[code] = {
            "target_dt": last_target["dt"].strftime("%Y-%m-%d"),
            "target_close": round(target_close, 2),
            "latest_close": round(latest_close, 2),
            "rise": round(latest["rise"], 2),
            "stock_name": latest["stock_name"]
        }

    print(f"✅ 最终符合【今日第一次突破】股票数量：{len(valid_codes)}")
    return valid_codes, target_map

# ====================== 加载K线（100个交易日） ======================
def load_k_and_industry(codes):
    engine = get_engine()
    code_str = ",".join([f"'{c}'" for c in codes])

    # 读取近 180 自然日 ≈ 120+ 交易日，保证绘图完整
    df_k = pd.read_sql(f"""
        SELECT dt, code, stock_name,
               price_open AS Open, price_close AS Close,
               price_highest AS High, price_lowest AS Low,
               trade_amount AS Volume, rise
        FROM stock_detail
        WHERE code IN ({code_str})
          AND dt >= DATE_SUB(CURDATE(), INTERVAL 180 DAY)
        ORDER BY code, dt
    """, engine)
    df_k["dt"] = pd.to_datetime(df_k["dt"])

    # 行业信息（空值自动填未分类）
    df_tag = pd.read_sql(f"""
        SELECT 
            REPLACE(REPLACE(LOWER(code),'sz',''),'sh','') AS code, 
            IFNULL(industry, '未分类') AS industry, 
            IFNULL(industry_detail, '未分类') AS industry_detail
        FROM dim_stock_tag
        WHERE REPLACE(REPLACE(LOWER(code),'sz',''),'sh','') IN ({code_str})
    """, engine)

    name_df = df_k.drop_duplicates("code")[["code", "stock_name"]]
    df_stock = name_df.merge(df_tag, on="code", how="left")
    df_stock["industry"] = df_stock["industry"].fillna("未分类")
    df_stock["industry_detail"] = df_stock["industry_detail"].fillna("未分类")

    return df_k, df_stock

# ====================== 多线程绘图 ======================
def draw_all(df_k, codes):
    img_map = {}

    def draw(code):
        sub = df_k[df_k["code"] == code].copy()
        if len(sub) < 20:
            return code, "no_data"
        sub.set_index("dt", inplace=True)
        try:
            fig, ax = mpf.plot(sub, type="candle", volume=True, style=s_style,
                               figratio=(12, 6), figscale=0.75, returnfig=True)
            buf = BytesIO()
            fig.savefig(buf, format="png", bbox_inches="tight", dpi=90)
            buf.seek(0)
            img = base64.b64encode(buf.read()).decode()
            plt.close(fig)
            return code, f"data:image/png;base64,{img}"
        except Exception as e:
            print(f"绘图失败 {code}: {str(e)}")
            return code, "error"

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = executor.map(draw, codes)
    for code, img in results:
        img_map[code] = img
    return img_map

# ====================== 生成HTML ======================
def generate_html():
    today = datetime.now().strftime("%Y-%m-%d")
    codes, target_map = select_stocks()
    if not codes:
        print("❌ 无符合条件股票")
        return

    df_k, df_stock = load_k_and_industry(codes)
    img_map = draw_all(df_k, codes)

    # 行业按股票数从多到少排序
    industry_cnt = df_stock["industry"].value_counts().sort_values(ascending=False)
    industries = industry_cnt.index.tolist()

    html = f'''
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <title>3倍放量·今日首次突破策略 {today}</title>
        <style>
            *{{box-sizing:border-box;margin:0;padding:0;font-family:Microsoft YaHei}}
            body{{background:#f5f7fa;padding:20px}}
            .container{{max-width:2000px;margin:auto}}
            .title{{text-align:center;font-size:24px;margin-bottom:20px}}
            .col-switch{{display:flex;gap:10px;justify-content:center;margin-bottom:20px}}
            .col-btn{{padding:10px 22px;border:0;border-radius:6px;background:#e3e6ed;cursor:pointer}}
            .col-btn.active{{background:#2f80ed;color:white}}
            .tab-wrap{{background:white;padding:16px;border-radius:10px;margin-bottom:20px}}
            .tabs{{display:flex;gap:8px;flex-wrap:wrap}}
            .tab{{padding:9px 16px;background:#f1f3f5;border:0;border-radius:6px;cursor:pointer}}
            .tab.active{{background:#2f80ed;color:white}}
            .tab-content{{display:none;grid-template-columns:repeat(4,1fr);gap:20px}}
            .tab-content.active{{display:grid}}
            .card{{background:white;padding:14px;border-radius:12px;box-shadow:0 2px 8px rgba(0,0,0,0.08)}}
            .card img{{width:100%;border-radius:8px;margin-top:10px}}
            .line1{{font-weight:bold;font-size:15px;margin-bottom:4px}}
            .line2{{color:#444;font-size:13px;margin-bottom:4px}}
            .line3{{color:#888;font-size:12px}}
            .red{{color:#e63946}}
            .green{{color:#28a745}}
        </style>
    </head>
    <body>
    <div class="container">
        <h1 class="title">🚀 3倍放量 · 今日首次突破目标价 · 选股看板 {today}</h1>
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
        html += f'<button class="tab {active}" onclick="setTab({i})">{ind}({industry_cnt[ind]})</button>'
    html += '</div></div>'

    # 股票卡片
    for i, ind in enumerate(industries):
        active = "active" if i == 0 else ""
        html += f'<div class="tab-content {active}">'
        for _, r in df_stock[df_stock["industry"] == ind].iterrows():
            code = r["code"]
            img = img_map.get(code)
            info = target_map.get(code, {})

            t_dt = info.get("target_dt", "")
            t_p = info.get("target_close", "")
            l_p = info.get("latest_close", "")
            rise = info.get("rise", 0)
            color = "red" if rise >= 0 else "green"

            img_html = f'<img src="{img}">' if img not in ["no_data", "error"] else "<div style='height:220px;display:flex;align-items:center;justify-content:center;color:#aaa'>无数据</div>"

            html += f'''
            <div class="card">
                <div class="line1">{code} {r["stock_name"]} | 现价：{l_p}元 <span class="{color}">{rise:+.2f}%</span></div>
                <div class="line2">目标天：{t_dt} | 目标价：{t_p}元</div>
                <div class="line3">{r["industry_detail"]}</div>
                {img_html}
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
    path = f"../html/{today}_3倍放量首次突破.html"
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"✅ HTML已生成：{path}")

if __name__ == "__main__":
    generate_html()