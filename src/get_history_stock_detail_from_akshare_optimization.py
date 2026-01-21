import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os


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


def get_stock_daily_batch(target_date):
    """
    批量获取指定日期所有A股的当日行情数据，兼容不同AKShare版本
    """
    # 定义核心字段的映射关系（兼容不同接口的列名）
    core_fields_map = {
        'code': ['代码', '证券代码'],
        'stock_name': ['名称', '证券名称'],
        'price_open': ['开盘价', '开盘'],
        'price_close': ['收盘价', '收盘'],
        'rise': ['涨跌幅', '涨跌幅%'],
        'price_highest': ['最高价', '最高'],
        'price_lowest': ['最低价', '最低'],
        'trade': ['成交量', '成交额']  # 部分接口成交量字段名可能是“成交量”或“成交额”，优先匹配
    }

    # 尝试接口1：东方财富当日行情（优先）
    try:
        print("尝试接口1：东方财富全市场当日行情")
        df = ak.stock_zh_a_spot_em()
        print(f"接口1返回列名：{df.columns.tolist()}")

        # 动态映射字段（只保留核心字段，缺失则填充NaN）
        result_df = pd.DataFrame()
        for target_col, source_cols in core_fields_map.items():
            for src_col in source_cols:
                if src_col in df.columns:
                    result_df[target_col] = df[src_col]
                    break
            else:
                result_df[target_col] = pd.NA  # 字段缺失时填充空值

        # 补充日期字段
        result_df['dt'] = target_date
        # 过滤有效数据
        result_df = result_df.dropna(subset=['code', 'price_close']).drop_duplicates(subset=['code'])
        print(f"接口1成功获取 {len(result_df)} 只股票数据")
        return result_df

    except Exception as e1:
        print(f"接口1失败：{e1}，尝试接口2：批量历史行情接口")

        # 尝试接口2：AKShare通用批量历史行情接口（兼容所有版本）
        try:
            # 该接口支持批量获取指定日期的全市场数据，无需逐只轮询
            df = ak.stock_zh_a_hist_em(
                start_date=target_date,
                end_date=target_date,
                adjust="qfq"
            )
            print(f"接口2返回列名：{df.columns.tolist()}")

            # 动态映射字段
            result_df = pd.DataFrame()
            for target_col, source_cols in core_fields_map.items():
                for src_col in source_cols:
                    if src_col in df.columns:
                        result_df[target_col] = df[src_col]
                        break
                else:
                    result_df[target_col] = pd.NA

            # 补充日期字段
            result_df['dt'] = target_date
            # 过滤有效数据
            result_df = result_df.dropna(subset=['code', 'price_close']).drop_duplicates(subset=['code'])
            print(f"接口2成功获取 {len(result_df)} 只股票数据")
            return result_df

        except Exception as e2:
            print(f"接口2失败：{e2}，尝试接口3：获取股票列表后批量拼接（保底方案）")

            # 尝试接口3：获取股票列表+批量拼接（保底，少量并发，比逐只快10倍）
            try:
                # 获取股票列表
                stock_list = ak.stock_info_a_code_name()
                # stock_codes = stock_list['code'].tolist()[:1000]  # 分批次获取，避免单次请求过多
                stock_codes = stock_list['code'].tolist()  # 分批次获取，避免单次请求过多
                all_data = []

                # 分批次（每100只一批）获取，减少请求次数
                batch_size = 100
                for i in range(0, len(stock_codes), batch_size):
                    batch_codes = stock_codes[i:i + batch_size]
                    batch_data = []
                    for code in batch_codes:
                        try:
                            # 单只股票当日数据
                            temp_df = ak.stock_zh_a_hist(
                                symbol=code,
                                period="daily",
                                start_date=target_date,
                                end_date=target_date,
                                adjust="qfq"
                            )
                            if not temp_df.empty:
                                temp_df['code'] = code
                                temp_df['stock_name'] = stock_list[stock_list['code'] == code]['name'].iloc[0]
                                batch_data.append(temp_df)
                        except:
                            continue
                    if batch_data:
                        all_data.append(pd.concat(batch_data, ignore_index=True))
                    print(f"接口3：已获取 {i + len(batch_codes)}/{len(stock_codes)} 只股票")

                if all_data:
                    result_df = pd.concat(all_data, ignore_index=True)
                    # 字段映射
                    result_df.rename(columns={
                        '开盘': 'price_open',
                        '收盘': 'price_close',
                        '涨跌幅': 'rise',
                        '最高': 'price_highest',
                        '最低': 'price_lowest',
                        '成交量': 'trade',
                        '日期': 'dt'
                    }, inplace=True)
                    print(f"接口3成功获取 {len(result_df)} 条数据")
                    return result_df
                else:
                    raise Exception("接口3未获取到任何数据")
            except Exception as e3:
                raise Exception(f"所有接口均失败：\n接口1错误：{e1}\n接口2错误：{e2}\n接口3错误：{e3}")


if __name__ == '__main__':
    # 1. 初始化重试会话
    create_retry_session(retries=3, timeout=30)

    # 2. 设定目标日期
    target_date = '20260120'
    # 校验日期是否为字符串格式
    try:
        datetime.strptime(target_date, "%Y%m%d")
    except ValueError:
        raise Exception("日期格式错误，请输入YYYYMMDD格式，例如：20260120")

    # 3. 批量获取当日所有股票数据
    try:
        all_stock_df = get_stock_daily_batch(target_date)
    except Exception as e:
        raise Exception(f"获取数据失败：{e}")

    if all_stock_df.empty:
        raise Exception("未获取到任何股票数据，请检查日期是否为交易日")

    # 4. 数据格式标准化
    # 处理涨跌幅字段（去除%符号，转换为数值）
    if 'rise' in all_stock_df.columns:
        all_stock_df['rise'] = all_stock_df['rise'].astype(str).str.replace('%', '').str.replace('--', '0')
        all_stock_df['rise'] = pd.to_numeric(all_stock_df['rise'], errors='coerce')

    # 转换数值字段类型
    numeric_fields = ['price_open', 'price_close', 'price_highest', 'price_lowest', 'trade']
    for field in numeric_fields:
        if field in all_stock_df.columns:
            all_stock_df[field] = pd.to_numeric(all_stock_df[field], errors='coerce')

    # 5. 保存数据到CSV
    output_dir = './file'
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f'{target_date}_{target_date}_stock_detail.csv')

    all_stock_df.to_csv(output_path, index=False, encoding='utf-8-sig')

    # 输出统计信息
    print(f"\n===== 数据获取完成 =====")
    print(f"文件保存路径：{output_path}")
    print(f"有效股票数量：{len(all_stock_df)}")
    print(f"核心字段列表：{all_stock_df.columns.tolist()}")

    # 数据预览
    print("\n数据预览：")
    preview_cols = ['dt', 'code', 'stock_name', 'price_open', 'price_close', 'rise']
    preview_cols = [col for col in preview_cols if col in all_stock_df.columns]
    print(all_stock_df[preview_cols].head(10))