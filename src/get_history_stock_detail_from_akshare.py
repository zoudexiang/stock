import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import time
from requests.adapters import HTTPAdapter
import requests
from urllib3.util.retry import Retry

# 基于 akshare 平台获取数据，执行时若遇到框架相关的问题  执行一次框架更新 pip install akshare -i https://mirrors.aliyun.com/pypi/simple/


# 配置requests会话，增加重试机制和超时时间
def create_retry_session(retries=3, backoff_factor=0.5, timeout=30):
    """
    创建带重试机制的requests会话，解决网络超时问题
    """
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    # 替换AKShare的默认session，使用自定义会话
    ak.session = session
    return session


if __name__ == '__main__':
    # 1. 初始化重试会话，延长超时时间（解决深交所接口超时）
    create_retry_session(retries=3, timeout=30)

    # 2. 设定时间区间：近一年（结束日期为今日，开始日期为去年今日）
    # end_date = datetime.now().strftime("%Y%m%d")
    # start_date = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")
    end_date = '20260120'
    start_date = '20260120'

    # 3. 获取A股所有股票代码列表（兼容备选接口，解决szse.cn访问超时）
    stock_code_df = None
    try:
        # 优先使用原接口
        stock_code_df = ak.stock_info_a_code_name()
        print("成功通过原接口获取股票列表")
    except Exception as e:
        print(f"原接口获取股票列表失败：{e}，尝试使用备选接口")
        # 备选接口1：东方财富A股列表（绕开深交所官网）
        try:
            stock_code_df = ak.stock_zh_a_spot_em()
            # 字段映射，保持与原接口一致
            stock_code_df.rename(columns={'代码': 'code', '名称': 'name'}, inplace=True)
            stock_code_df = stock_code_df[['code', 'name']].drop_duplicates(subset=['code'])
            print("备选接口1获取股票列表成功")
        except Exception as e2:
            print(f"备选接口1失败：{e2}，尝试备选接口2")
            # 备选接口2：沪深市场股票列表
            stock_sh = ak.stock_info_sh_name_code(symbol="A股列表")
            stock_sh.rename(columns={'证券代码': 'code', '证券名称': 'name'}, inplace=True)
            stock_sz = ak.stock_info_sz_name_code(symbol="A股列表")
            stock_sz.rename(columns={'证券代码': 'code', '证券名称': 'name'}, inplace=True)
            stock_code_df = pd.concat([stock_sh, stock_sz], ignore_index=True)
            print("备选接口2获取股票列表成功")

    if stock_code_df is None or stock_code_df.empty:
        raise Exception("所有股票列表接口均获取失败，请检查网络环境")

    stock_codes = stock_code_df['code'].tolist()

    print(f"共获取 {len(stock_codes)} 只A股股票代码")

    # 4. 批量拉取每只股票近一年的日线数据（添加延时，规避反爬）
    all_stock_data = []
    # 遍历股票代码，批量获取（测试阶段取前100只，删除[:100]即可获取全市场）
    # for index, code in enumerate(stock_codes[:5]):
    for index, code in enumerate(stock_codes):
        try:
            # 拉取日线数据（延长超时时间，添加重试后无需额外配置）
            stock_df = ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"  # 前复权，消除分红送股影响
            )
            # 添加股票代码和股票名称，便于识别
            stock_df['code'] = code
            # 避免股票名称匹配失败
            stock_name = stock_code_df[stock_code_df['code'] == code]['name'].iloc[0] if len(stock_code_df[stock_code_df['code'] == code]) > 0 else "未知名称"
            stock_df['stock_name'] = stock_name
            all_stock_data.append(stock_df)
            # print(f"[{index + 1}/{len(stock_codes[:100])}] 已获取 {code} - {stock_name} 的数据")
            print(f"[{index + 1}/{len(stock_codes)}] 已获取 {code} - {stock_name} 的数据")

            # 添加延时，规避反爬（关键：1秒/次，可根据情况调整）
            time.sleep(1)

        except Exception as e:
            # print(f"[{index + 1}/{len(stock_codes[:100])}] 获取 {code} 数据失败：{e}")
            print(f"[{index + 1}/{len(stock_codes)}] 获取 {code} 数据失败：{e}")
            # 失败时也短暂延时，避免高频重试
            time.sleep(1)
            continue

    # 5. 合并所有股票数据，整理格式
    if all_stock_data:
        result_df = pd.concat(all_stock_data, ignore_index=True)
        # 字段映射（匹配需求：开盘价/收盘价/涨跌幅/最高价/最低价/成交量）
        result_df.rename(columns={
            '开盘': 'price_open',
            '收盘': 'price_close',
            '涨跌幅': 'rise',
            '最高': 'price_highest',
            '最低': 'price_lowest',
            '成交量': 'trade',
            '日期': 'dt'
        }, inplace=True)

        # 6. 保存数据（CSV格式，可直接导入Excel/OLAP引擎）
        result_df.to_csv(f'./file/{start_date}_{end_date}_stock_detail.csv', index=False, encoding='utf-8-sig')
        print(f"\n数据保存完成！共获取 {len(result_df)} 条记录，涉及 {len(set(result_df['code']))} 只股票")
        # 查看数据预览
        print(result_df[['dt', 'code', 'stock_name', 'price_open', 'price_close', 'rise', 'trade']].head())
    else:
        print("未获取到任何股票数据，请检查接口或网络环境")