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


def generate_html():
    print("📥 加载核心股票数据...")

    # ========== 1、定义原始自选固定顺序列表 + 股票对应分类注释映射 ==========
    stock_origin_order = [
        # 先进封装
        '长电科技', '华天科技', '通富微电', '兴森科技', '深科技', '盛合晶微', '甬矽电子', '晶方科技', '芯原股份', '大港股份', '联瑞新材',
        # 存储芯片
        '兆易创新', '佰维存储', '香农芯创', '德明利', '江波龙', '朗科科技',
        # 玻璃基板
        '京东方A', '中国巨石', '沃格光电', '彩虹股份', '凯盛科技', '帝尔激光', '长信科技', '力诺药包', '红星发展', '金瑞矿业',
        # 光纤
        '长飞光纤', '亨通光电', '中天科技', '烽火通信', '三安光电', '通鼎股份',
        # 算力租赁
        '利通电子', '协创数据', '润建股份', '盈峰环境', '工业富联', '紫光股份', '宏景科技', '优刻得',
        # PCB
        '生益科技', '鹏鼎控股', '沪电股份', '东山精密', '深南电路', '胜宏科技', '胜宏科技',
        # 六氟化钨
        '中川特气', '昊华科技', '和远气体', '中巨芯U', '中钨高新', '厦门钨业', '雅克科技', '华特气体',
        # 树脂
        '东材科技', '圣泉集团', '美联新材', '宏昌电子',
        # 磷化铟
        '云南锗业', '兴福电子', '兴发集团', '华锡有色', '博杰股份', '株冶集团',
        # 氮化镓
        '英诺赛科', '华润微', '立昂微', '宏微科技', '三安光电', '海特高新', '露笑科技', '铭普光磁', '晶方科技',
        # 碳化硅
        '华润微', '杰华特', '士兰微', '天岳先进', '晶升股份', '晶盛机电', '扬杰科技', '时代电气', '斯达半导',
        # 金刚石
        '惠丰钻石', '力量钻石', '四方达', '黄河旋风', '三超新材', '岱勒新材', '国机精工', '楚江新材', '中兵红箭',
        # 铜箔
        '铜冠铜箔', '诺德股份', '嘉元科技', '德福科技',
        # 元件
        '风华高科', '顺络电子', '三环集团', '麦捷科技',
        # 先进封装
        '长电科技', '通富微电', '华天科技', '深科技',
        # CPO
        '中际旭创', '新易盛', '天孚通信', '华工科技',
        # 电子布
        '中国巨石', '金安国纪', '宏和科技', '中材科技', '山东玻纤', '国际复材', '泰坦股份', '菲利华',
        # OCS
        '腾景科技', '福晶科技', '光库科技', '德科立',
        # 光芯片
        '源杰科技', '仕佳光子', '光讯科技', '长光华芯',
        # PET 铜箔
        '宝明科技', '双星新材', '东威科技',
        # 机器人
        '绿的谐波', '拓普集团', '三花智控', '汇川技术',
        # 铜缆
        '精达股份', '海亮股份', '金田股份', '楚江新材',
        # 光模块设备
        '罗博特科', '科瑞技术', '博杰股份', '德龙激光',
        # 高速链接
        '立讯精密', '兆龙互联', '沃尔核材', '鼎通科技',
        # 贵金属
        '盛达资源',
        # 半导体
        '北方华创', '中微公司', '华润微',
        # 三代半导
        '三安光电', '斯达半导',
        # 小金属
        '厦门钨业', '章源钨业', '洛阳钼业', '盛和资源',
        # 消费电子
        '歌尔股份', '蓝思科技', '领益智造',
        # 光刻机
        '张江高科', '芯碁微装', '中瓷电子',
        # 超级电容
        '江海股份', '铜峰电子', '新筑股份',
        # AI 芯片
        '海光信息', '寒武纪', '沐曦股份', '摩尔线程',
        # AI 服务器
        '工业富联', '紫光股份', '中科曙光', '浪溯信息',
        # 液冷服务器
        '英维克', '高澜股份', '中菱环境',
        # 电源
        '中恒电气', '圣阳股份', '欧陆通', '麦格米特',
        # 燃气轮机
        '杰瑞股份', '联德股份', '应流股份', '东方电气',
        # 固态变压器
        '四方股份', '中国西电', '伊戈尔', '金盘科技',
        # AIDC
        '润泽科技', '网宿科技', '光环新网', '数据港',
        # 算电协同
        '豫能控股', '协鑫能科', '南网数字', '韶能股份',
        # 商业航天
        '中国卫星', '中国卫通', '航天电子', '航天动力'
    ]

    # 新增：股票名称 对应 # 注释分类映射表（完全匹配上方#注释）
    stock_tag_map = {}
    tag_blocks = [
        ("先进封装", ['长电科技', '华天科技', '通富微电', '兴森科技', '深科技', '盛合晶微', '甬矽电子', '晶方科技', '芯原股份', '大港股份', '联瑞新材']),
        ("存储芯片", ['兆易创新', '佰维存储', '香农芯创', '德明利', '江波龙', '朗科科技']),
        ("玻璃基板", ['京东方A', '中国巨石', '沃格光电', '彩虹股份', '凯盛科技', '帝尔激光', '长信科技', '力诺药包', '红星发展', '金瑞矿业']),
        ("光纤", ['长飞光纤', '亨通光电', '中天科技', '烽火通信', '三安光电', '通鼎股份']),
        ("算力租赁", ['利通电子', '协创数据', '润建股份', '盈峰环境', '工业富联', '紫光股份', '宏景科技', '优刻得']),
        ("PCB", ['生益科技', '鹏鼎控股', '沪电股份', '东山精密', '深南电路', '胜宏科技', '胜宏科技']),
        ("六氟化钨", ['中川特气', '昊华科技', '和远气体', '中巨芯U', '中钨高新', '厦门钨业', '雅克科技', '华特气体']),
        ("树脂", ['东材科技', '圣泉集团', '美联新材', '宏昌电子']),
        ('磷化铟', ['云南锗业', '兴福电子', '兴发集团', '华锡有色', '博杰股份', '株冶集团']),
        ("氮化镓", ['英诺赛科', '华润微', '立昂微', '宏微科技', '三安光电', '海特高新', '露笑科技', '铭普光磁', '晶方科技']),
        ("碳化硅", ['华润微', '杰华特', '士兰微', '天岳先进', '晶升股份', '晶盛机电', '扬杰科技', '时代电气', '斯达半导']),
        ("金刚石", ['惠丰钻石', '力量钻石', '四方达', '黄河旋风', '三超新材', '岱勒新材', '国机精工', '楚江新材', '中兵红箭']),
        ("铜箔", ['铜冠铜箔', '诺德股份', '嘉元科技', '德福科技']),
        ("元件", ['风华高科', '顺络电子', '三环集团', '麦捷科技']),
        ("先进封装", ['长电科技', '通富微电', '华天科技', '深科技']),
        ("CPO", ['中际旭创', '新易盛', '天孚通信', '华工科技']),
        ("电子布", ['中国巨石', '金安国纪', '宏和科技', '中材科技', '山东玻纤', '国际复材', '泰坦股份', '菲利华']),
        ("OCS", ['腾景科技', '福晶科技', '光库科技', '德科立']),
        ("光芯片", ['源杰科技', '仕佳光子', '光讯科技', '长光华芯']),
        ("PET 铜箔", ['宝明科技', '双星新材', '东威科技']),
        ("机器人", ['绿的谐波', '拓普集团', '三花智控', '汇川技术']),
        ("铜缆", ['精达股份', '海亮股份', '金田股份', '楚江新材']),
        ("光模块设备", ['罗博特科', '科瑞技术', '博杰股份', '德龙激光']),
        ("高速链接", ['立讯精密', '兆龙互联', '沃尔核材', '鼎通科技']),
        ("贵金属", ['盛达资源']),
        ("半导体", ['北方华创', '中微公司', '华润微']),
        ("三代半导", ['三安光电', '斯达半导']),
        ("小金属", ['厦门钨业', '章源钨业', '洛阳钼业', '盛和资源']),
        ("消费电子", ['歌尔股份', '蓝思科技', '领益智造']),
        ("光刻机", ['张江高科', '芯碁微装', '中瓷电子']),
        ("超级电容", ['江海股份', '铜峰电子', '新筑股份']),
        ("AI 芯片", ['海光信息', '寒武纪', '沐曦股份', '摩尔线程']),
        ("AI 服务器", ['工业富联', '紫光股份', '中科曙光', '浪溯信息']),
        ("液冷服务器", ['英维克', '高澜股份', '中菱环境']),
        ("电源", ['中恒电气', '圣阳股份', '欧陆通', '麦格米特']),
        ("燃气轮机", ['杰瑞股份', '联德股份', '应流股份', '东方电气']),
        ("固态变压器", ['四方股份', '中国西电', '伊戈尔', '金盘科技']),
        ("AIDC", ['润泽科技', '网宿科技', '光环新网', '数据港']),
        ("算电协同", ['豫能控股', '协鑫能科', '南网数字', '韶能股份']),
        ("商业航天", ['中国卫星', '中国卫通', '航天电子', '航天动力'])
    ]
    # 填充映射字典
    for tag, stock_list in tag_blocks:
        for name in stock_list:
            stock_tag_map[name] = tag

    # 去重，保证顺序不变
    unique_stock_list = []
    seen = set()
    for name in stock_origin_order:
        if name not in seen:
            seen.add(name)
            unique_stock_list.append(name)

    # 1. 获取最新一天日期
    last_dt = pd.read_sql("SELECT MAX(dt) AS dt FROM stock_detail", engine).iloc[0]["dt"]

    # 2. 查询指定股票列表，删除末尾 order by，不打乱原生顺序
    sql = f"""
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
    from (
        select
            code,
            stock_name,
            rise_5,
            rise_10,
            rise_15,
            price_close,
            rise
        from stock_detail
        where dt = '{last_dt}'
            and stock_name in ({','.join([f"'{x}'" for x in unique_stock_list])})
    ) s left join (
        select
            code,
            industry,
            industry_detail
        from dim_stock_tag
    ) dst on replace(replace(lower(dst.code), 'sz', ''), 'sh', '') = s.code
    """

    df = pd.read_sql(sql, engine)
    if df.empty:
        print("❌ 未选出任何核心股票")
        return

    # ========== 核心：按照你原始书写顺序强制重排DataFrame ==========
    sort_mapping = {name: idx for idx, name in enumerate(unique_stock_list)}
    df["sort_key"] = df["stock_name"].map(sort_mapping)
    df = df.sort_values("sort_key").reset_index(drop=True)
    df.drop(columns=["sort_key"], inplace=True)

    # 3. 加载这些股票的K线
    codes = df["code"].unique().tolist()
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
    print("🖼️ 开始绘制 K 线...")
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

    # ---------------------- 按代码分三大板块，板块内部保留原始自选顺序 ----------------------
    main_board = []    # 主板
    growth = []         # 创业板300/301
    star = []           # 科创板688
    all_stock = df.to_dict("records") # 全部股票列表，顺序不变
    for _, row in df.iterrows():
        code = row["code"]
        if code.startswith("688"):
            star.append(row)
        elif code.startswith("300") or code.startswith("301"):
            growth.append(row)
        else:
            main_board.append(row)

    main_cnt = len(main_board)
    growth_cnt = len(growth)
    star_cnt = len(star)
    all_cnt = len(all_stock)

    # 6. 生成HTML，新增【全部个股】Tab
    print("🌍 生成HTML...")
    html = '''
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <title>核心股票 K 线看板</title>
        <style>
            *{box-sizing:border-box;margin:0;padding:0;font-family:Microsoft YaHei}
            body{background:#f5f7fa;padding:20px}
            .container{max-width:1900px;margin:0 auto}
            .title{text-align:center;margin-bottom:20px}

            /* 列切换按钮 */
            .col-switch{display:flex;gap:8px;justify-content:center;margin-bottom:20px}
            .col-btn{padding:10px 20px;border:none;border-radius:6px;background:#e3e6ed;cursor:pointer}
            .col-btn.active{background:#2f80ed;color:white}

            /* 板块Tab样式 */
            .tab-group{display:flex;gap:10px;justify-content:center;margin-bottom:20px}
            .tab-btn{padding:9px 22px;border:none;border-radius:6px;background:#e8ebf0;cursor:pointer;font-size:15px}
            .tab-btn.active{background:#2f80ed;color:#fff}

            /* 卡片容器 */
            .tab-content{display:none;grid-template-columns:repeat(3,1fr);gap:16px}
            .tab-content.active{display:grid}

            .card{background:white;padding:12px;border-radius:12px}
            .card img{width:100%;border-radius:8px;margin-top:10px}
            .stock-title{font-weight:bold}
            .price{color:#e63946;font-size:14px;margin-left:6px}
            .rise-green{color:#28a745;font-size:14px;margin-left:4px}
            .rise-red{color:#e63946;font-size:14px;margin-left:4px}
            .sub{font-size:12px;color:#888;margin-top:4px}

            /* 四色文字动画 */
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
            <h1 class="title">🚀 核心股票 K 线看板</h1>

            <!-- 板块Tab按钮：新增全部个股tab，默认激活全部 -->
            <div class="tab-group">
                <button class="tab-btn active" onclick="switchTab('all')">全部个股({all_cnt})</button>
                <button class="tab-btn" onclick="switchTab('main')">主板({main_cnt})</button>
                <button class="tab-btn" onclick="switchTab('cy')">创业板({growth_cnt})</button>
                <button class="tab-btn" onclick="switchTab('kc')">科创板({star_cnt})</button>
            </div>

            <!-- 列数切换 -->
            <div class="col-switch">
                <button class="col-btn" onclick="changeColumns(2)">2列</button>
                <button class="col-btn active" onclick="changeColumns(3)">3列</button>
                <button class="col-btn" onclick="changeColumns(4)">4列</button>
                <button class="col-btn" onclick="changeColumns(5)">5列</button>
            </div>

            <!-- 全部个股容器（默认显示） -->
            <div class="tab-content active" id="all">
    '''
    # 填充全部个股卡片（完整原始自选顺序）
    for r in all_stock:
        code = r["code"]
        stock_name = r["stock_name"]
        img = img_map.get(code, "")
        if not img:
            continue
        price = price_map.get(code, "")
        rise_val = rise_map.get(code, 0)
        price_str = f"({price}元)" if price else ""
        rise_cls = "rise-red" if rise_val >= 0 else "rise-green"
        rise_str = f'<span class="{rise_cls}">{rise_val:+.2f}%</span>'
        # 取出#注释分类
        tag_text = stock_tag_map.get(stock_name, "")
        tag_html = f'<span style="color:#2f80ed; font-weight:bold;">{tag_text}</span>'
        line_text = f"{tag_html} | {r['industry']} | {r['industry_detail']}"
        html += f'''
        <div class="card">
            <div class="stock-title">{code} {stock_name}<span class="price">{price_str}</span>{rise_str}</div>
            <div class="sub" style="color:red; font-weight:bold;">
                5日涨幅: {r["rise_5"]:.2f}% ｜ 10日涨幅: {r["rise_10"]:.2f}% ｜ 15日涨幅: {r["rise_15"]:.2f}%
            </div>
            <div class="sub">{line_text}</div>
            <img src="{img}">
        </div>
        '''
    html += '''
            </div>

            <!-- 主板容器 -->
            <div class="tab-content" id="main">
    '''
    # 填充主板卡片
    for r in main_board:
        code = r["code"]
        stock_name = r["stock_name"]
        img = img_map.get(code, "")
        if not img:
            continue
        price = price_map.get(code, "")
        rise_val = rise_map.get(code, 0)
        price_str = f"({price}元)" if price else ""
        rise_cls = "rise-red" if rise_val >= 0 else "rise-green"
        rise_str = f'<span class="{rise_cls}">{rise_val:+.2f}%</span>'
        tag_text = stock_tag_map.get(stock_name, "")
        tag_html = f'<span style="color:#2f80ed; font-weight:bold;">{tag_text}</span>'
        line_text = f"{tag_html} | {r['industry']} | {r['industry_detail']}"
        html += f'''
        <div class="card">
            <div class="stock-title">{code} {stock_name}<span class="price">{price_str}</span>{rise_str}</div>
            <div class="sub" style="color:red; font-weight:bold;">
                5日涨幅: {r["rise_5"]:.2f}% ｜ 10日涨幅: {r["rise_10"]:.2f}% ｜ 15日涨幅: {r["rise_15"]:.2f}%
            </div>
            <div class="sub">{line_text}</div>
            <img src="{img}">
        </div>
        '''
    html += '''
            </div>

            <!-- 创业板容器 -->
            <div class="tab-content" id="cy">
    '''
    # 填充创业板卡片
    for r in growth:
        code = r["code"]
        stock_name = r["stock_name"]
        img = img_map.get(code, "")
        if not img:
            continue
        price = price_map.get(code, "")
        rise_val = rise_map.get(code, 0)
        price_str = f"({price}元)" if price else ""
        rise_cls = "rise-red" if rise_val >= 0 else "rise-green"
        rise_str = f'<span class="{rise_cls}">{rise_val:+.2f}%</span>'
        tag_text = stock_tag_map.get(stock_name, "")
        tag_html = f'<span style="color:#2f80ed; font-weight:bold;">{tag_text}</span>'
        line_text = f"{tag_html} | {r['industry']} | {r['industry_detail']}"
        html += f'''
        <div class="card">
            <div class="stock-title">{code} {stock_name}<span class="price">{price_str}</span>{rise_str}</div>
            <div class="sub" style="color:red; font-weight:bold;">
                5日涨幅: {r["rise_5"]:.2f}% ｜ 10日涨幅: {r["rise_10"]:.2f}% ｜ 15日涨幅: {r["rise_15"]:.2f}%
            </div>
            <div class="sub">{line_text}</div>
            <img src="{img}">
        </div>
        '''
    html += '''
            </div>

            <!-- 科创板容器 -->
            <div class="tab-content" id="kc">
    '''
    # 填充科创板卡片
    for r in star:
        code = r["code"]
        stock_name = r["stock_name"]
        img = img_map.get(code, "")
        if not img:
            continue
        price = price_map.get(code, "")
        rise_val = rise_map.get(code, 0)
        price_str = f"({price}元)" if price else ""
        rise_cls = "rise-red" if rise_val >= 0 else "rise-green"
        rise_str = f'<span class="{rise_cls}">{rise_val:+.2f}%</span>'
        tag_text = stock_tag_map.get(stock_name, "")
        tag_html = f'<span style="color:#2f80ed; font-weight:bold;">{tag_text}</span>'
        line_text = f"{tag_html} | {r['industry']} | {r['industry_detail']}"
        html += f'''
        <div class="card">
            <div class="stock-title">{code} {stock_name}<span class="price">{price_str}</span>{rise_str}</div>
            <div class="sub" style="color:red; font-weight:bold;">
                5日涨幅: {r["rise_5"]:.2f}% ｜ 10日涨幅: {r["rise_10"]:.2f}% ｜ 15日涨幅: {r["rise_15"]:.2f}%
            </div>
            <div class="sub">{line_text}</div>
            <img src="{img}">
        </div>
        '''
    html += '''
            </div>
        </div>

        <script>
            // 板块切换（新增all全部标签逻辑）
            function switchTab(tabId) {
                // 隐藏所有内容
                document.querySelectorAll('.tab-content').forEach(box => {
                    box.classList.remove('active');
                });
                // 取消所有按钮激活
                document.querySelectorAll('.tab-btn').forEach(btn => {
                    btn.classList.remove('active');
                });
                // 激活目标tab
                document.getElementById(tabId).classList.add('active');
                event.target.classList.add('active');
            }

            // 列数切换，全部4个tab同步生效
            function changeColumns(col) {
                document.querySelectorAll('.tab-content').forEach(wrap => {
                    wrap.style.gridTemplateColumns = `repeat(${col}, 1fr)`;
                });
                document.querySelectorAll('.col-btn').forEach((btn,i) => {
                    btn.classList.toggle('active', parseInt(btn.innerText[0]) === col);
                });
            }
        </script>
    </body></html>
    '''
    # 填充板块数量占位符
    html = html.replace("{all_cnt}", str(all_cnt))
    html = html.replace("{main_cnt}", str(main_cnt))
    html = html.replace("{growth_cnt}", str(growth_cnt))
    html = html.replace("{star_cnt}", str(star_cnt))

    filename = f"../html/{datetime.now().strftime('%Y-%m-%d')}_核心股票 K 线看板.html"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"✅ 完成！文件已生成：{filename}")

if __name__ == "__main__":
    start_time = time.time()
    today = datetime.now().strftime("%Y-%m-%d")
    generate_html()
    end_time = time.time()
    cost_time = end_time - start_time
    print(f"程序总耗时：{cost_time:.2f} 秒")