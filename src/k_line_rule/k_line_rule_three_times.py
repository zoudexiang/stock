import pymysql
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

from src.utils import constants


def get_strategy_stocks_python(start_date: str, end_date: str):
    """
    Python端读取数据后处理，筛选符合策略的股票数据（消除pandas read_sql警告版）
    :param start_date: 开始日期（格式：yyyy-MM-dd）
    :param end_date: 结束日期（格式：yyyy-MM-dd）
    :return: 符合条件的股票数据（DataFrame格式）
    """
    # 1. 配置MySQL数据库连接参数
    db_config = constants.db_config

    # 2. 构建SQLAlchemy引擎（核心修改：替代直接的pymysql连接）
    # 连接格式：mysql+pymysql://用户名:密码@主机:端口/数据库名?charset=字符集
    engine_url = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@" \
                 f"{db_config['host']}:{db_config['port']}/{db_config['database']}?charset={db_config['charset']}"
    engine = create_engine(engine_url)

    # 3. 查询目标时间范围的所有原始数据
    sql_query = f'''
    select 
        * 
    from stock.stock_detail
    where dt>='{start_date}' 
        and dt<='{end_date}' 
        and code not like '688%%'
        and code not like '920%%' 
        and stock_name not like '%%ST%%'
    order by code, dt;
    '''

    try:
        # 4. 用SQLAlchemy引擎执行查询（无警告，符合pandas推荐规范）
        # df = pd.read_sql(sql_query, engine, params=(start_date, end_date))
        df = pd.read_sql(sql_query, engine)

        print(len(df))
        # 5. 后续数据处理逻辑（与原代码完全一致，无修改）
        df['dt_date'] = pd.to_datetime(df['dt'], format='%Y-%m-%d')

        # 计算前5日成交额平均值
        df['pre5d_trade_amount_avg'] = df.groupby('code')['trade_amount'].transform(
            lambda x: x.shift(1).rolling(window=5, min_periods=5).mean()
        )

        # 筛选符合策略条件的记录
        df_strategy = df[
            (df['trade_amount'] > 3 * df['pre5d_trade_amount_avg']) &
            (df['pre5d_trade_amount_avg'].notna()) &
            ((df['price_close'] - df['price_open']) > 0)
            ].copy()

        # 保留原表字段，删除辅助字段
        df_strategy = df_strategy.drop(columns=['dt_date', 'pre5d_trade_amount_avg'])

        # 将 df_strategy 导出为 CSV 文件
        if not df_strategy.empty:
            current_date = datetime.now().strftime('%Y-%m-%d')
            csv_file_path = f"../file/k_line_符合策略的股票数据_{current_date}.csv"

            # 导出CSV
            df_strategy.to_csv(
                csv_file_path,
                index=False,  # 不导出行索引
                encoding='utf-8-sig',  # 解决中文乱码
                sep=','  # CSV分隔符，默认逗号，可改为'\t'等
            )
            print(f"CSV文件导出成功！保存路径：{csv_file_path}，包含 {len(df_strategy)} 条记录")
        else:
            print("无符合策略的数据，无需导出CSV文件")

        return df_strategy
    except Exception as e:
        # print(f"处理失败：{e}")
        # return pd.DataFrame()
        raise e


# 调用函数
if __name__ == "__main__":
    strategy_stocks = get_strategy_stocks_python('2025-12-01', '2026-01-16')
    print(f"符合条件的记录数：{len(strategy_stocks)}")
    print(strategy_stocks)