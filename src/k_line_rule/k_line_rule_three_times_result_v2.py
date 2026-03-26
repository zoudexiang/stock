import pandas as pd
import datetime
import time
from sqlalchemy import create_engine, text

from src.utils import constants

def process_stock_full_strategy_with_return_target(start_date: str, end_date: str):
    """
    纯Python实现完整股票策略（含返回目标天约束），仅输出符合条件的目标天数据（保留原表结构）
    :param start_date: 开始日期（yyyy-MM-dd）
    :param end_date: 结束日期（yyyy-MM-dd）
    :return: 符合所有策略的目标天数据（DataFrame）
    """
    # 1. 配置MySQL数据库连接参数（替换为你的实际配置）
    db_config = constants.db_config

    try:
        # 2. 构建SQLAlchemy引擎，读取原始数据
        engine_url = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@" \
                     f"{db_config['host']}:{db_config['port']}/{db_config['database']}?charset={db_config['charset']}"
        engine = create_engine(engine_url)

        sql_query = f'''
        select 
            dt, 
            a.code as code, 
            stock_name, 
            price_open, 
            price_close, 
            price_highest, 
            price_lowest, 
            trade, 
            trade_amount, 
            amplitude, 
            rise, 
            amount_increase_decrease, 
            turnover_rate,
            if(industry is null, '', industry) as industry,
            if(industry_detail is null, '', industry_detail) as industry_detail
        from (
            select
                dt, 
                code, 
                stock_name, 
                price_open, 
                price_close, 
                price_highest, 
                price_lowest, 
                trade, 
                trade_amount, 
                amplitude, 
                rise, 
                amount_increase_decrease, 
                turnover_rate
            from stock.stock_detail 
            where dt>='{start_date}' 
                and dt<='{end_date}' 
                and code not like '688%%' 
                and code not like '920%%' 
                and stock_name not like '%%st%%'
        ) a left join (
            select 
                replace(replace(code, 'SZ', ''), 'SH', '') as code, 
                industry,
                industry_detail
            from stock.dim_stock_tag
        ) b on a.code=b.code
        order by code, dt;
        '''
        df = pd.read_sql(sql_query, engine)
        if df.empty:
            print("提示：未查询到指定日期范围内的原始股票数据")
            return pd.DataFrame()

        # 3. 数据预处理：转换日期+加交易日行号+排序
        df['dt_date'] = pd.to_datetime(df['dt'], format='%Y-%m-%d')
        df = df.sort_values(by=['code', 'dt_date']).reset_index(drop=True)

        # 给每只股票的交易日添加行号（用于计算交易日间隔）
        df['trade_day_rn'] = df.groupby('code').cumcount() + 1

        # 4. 按股票分组，计算前5日成交额平均值（筛选目标天）
        def calculate_pre5d_trade_avg(group):
            # 新增列存储前5日成交额均值，不修改原表列
            group['pre5d_trade_amount_avg'] = group['trade_amount'].shift(1).rolling(
                window=5, min_periods=5
            ).mean()
            return group

        df = df.groupby('code').apply(calculate_pre5d_trade_avg).reset_index(drop=True)

        # 5. 筛选「目标天」记录（规则1）—— 不重命名原列，仅筛选，保留所有原表列
        target_df = df[
            (df['trade_amount'] > 3 * df['pre5d_trade_amount_avg']) &
            (df['pre5d_trade_amount_avg'].notna()) &
            ((df['price_close'] - df['price_open']) > 0)
            ].copy()

        if target_df.empty:
            print("提示：未筛选到符合条件的目标天数据")
            return pd.DataFrame()

        # 关键修改：新增列存储目标天相关数据，不修改原表列名（保留price_open等原始列）
        target_df['target_trade_amount'] = target_df['trade_amount']
        target_df['target_open'] = target_df['price_open']
        target_df['target_close'] = target_df['price_close']
        target_df['target_dt_date'] = target_df['dt_date']
        target_df['target_trade_rn'] = target_df['trade_day_rn']

        # 6. 确定「分歧天」+ 计算disagreementMaxPrice（规则2+3）
        valid_target_list = []
        for _, target_row in target_df.iterrows():
            # 提取目标天核心信息（从新增列中提取，不依赖重命名的原列）
            code = target_row['code']
            target_dt = target_row['dt']
            target_dt_date = target_row['target_dt_date']
            target_trade_amount = target_row['target_trade_amount']
            target_trade_rn = target_row['target_trade_rn']
            target_open = target_row['target_open']
            target_close = target_row['target_close']

            # 步骤6.1：获取目标天后3个交易日数据
            follow_df = df[
                (df['code'] == code) &
                (df['dt_date'] > target_dt_date)
                ].sort_values(by='dt_date').head(3).copy()

            # 步骤6.2：确定分歧天
            if not follow_df.empty:
                bigger_follow_df = follow_df[follow_df['trade_amount'] > target_trade_amount].copy()
                if not bigger_follow_df.empty:
                    # 取成交额最大、日期最早的分歧天
                    bigger_follow_df = bigger_follow_df.sort_values(
                        by=['trade_amount', 'dt_date'],
                        ascending=[False, True]
                    ).reset_index(drop=True)
                    disagreement_row = bigger_follow_df.iloc[0]
                    disagreement_dt_date = disagreement_row['dt_date']
                    disagreement_open = disagreement_row['price_open']
                    disagreement_close = disagreement_row['price_close']
                else:
                    # 无符合条件，分歧天=目标天
                    disagreement_dt_date = target_dt_date
                    disagreement_open = target_open
                    disagreement_close = target_close
            else:
                disagreement_dt_date = target_dt_date
                disagreement_open = target_open
                disagreement_close = target_close

            # 步骤6.3：计算分歧天最高实体价
            disagreement_max_price = disagreement_open if disagreement_open > disagreement_close else disagreement_close

            # 步骤7：筛选「返回目标天」+ 验证规则4的3个约束
            # 7.1：获取分歧天后所有交易日数据
            after_disagreement_df = df[
                (df['code'] == code) &
                (df['dt_date'] > disagreement_dt_date)
                ].sort_values(by='dt_date').copy()

            if after_disagreement_df.empty:
                continue

            # 7.2：筛选达到disagreementMaxPrice的记录
            reach_max_price_df = after_disagreement_df[
                after_disagreement_df['price_close'] >= disagreement_max_price
                ].copy()

            if reach_max_price_df.empty:
                continue

            # 7.3：取第一次达到的返回目标天（约束1）
            return_target_row = reach_max_price_df.iloc[0]
            return_target_dt_date = return_target_row['dt_date']
            return_target_trade_rn = return_target_row['trade_day_rn']
            return_target_close = return_target_row['price_close']

            # 7.4：验证约束2：交易日间隔≥2（行号差≥3）
            if (return_target_trade_rn - target_trade_rn) < 3:
                continue

            # 7.5：验证约束3：中间所有交易日收盘价≤disagreementMaxPrice
            middle_df = df[
                (df['code'] == code) &
                (df['dt_date'] > target_dt_date) &
                (df['dt_date'] < return_target_dt_date)
                ].copy()

            middle_close_valid = True
            if not middle_df.empty:
                middle_close_max = middle_df['price_close'].max()
                if middle_close_max > disagreement_max_price:
                    middle_close_valid = False

            if not middle_close_valid:
                continue

            # 所有约束验证通过，保留该目标天（target_row包含完整原表列）
            valid_target_list.append(target_row)

        # 8. 转换为DataFrame，保留原表结构
        valid_target_df = pd.DataFrame(valid_target_list)
        if valid_target_df.empty:
            print("提示：无符合所有策略约束的目标天数据")
            return pd.DataFrame()

        # 定义原表字段结构（可正常找到price_open等列）
        original_table_columns = [
            'dt', 'code', 'stock_name', 'price_open', 'price_close', 'price_highest',
            'price_lowest', 'trade', 'trade_amount', 'amplitude', 'rise',
            'amount_increase_decrease', 'turnover_rate', 'industry', 'industry_detail'
        ]

        final_result = valid_target_df[original_table_columns].drop_duplicates()
        print(f"全流程策略执行完成！符合所有条件的目标天记录共 {len(final_result)} 条")
        return final_result

    except Exception as e:
        print(f"执行失败：{str(e)}")
        return pd.DataFrame()

def insert_data_to_mysql(stock_valid_target, end_date):
    """
    将 stock_valid_target 数据写入 MySQL 表 stock.stock_detail_calc
    逻辑：
    1. 新增 calc_dt 字段（当日日期，格式 yyyy-MM-dd）
    2. 先删除当前 calc_dt 的所有数据（防重复）
    3. 将数据写入表中
    : param stock_valid_target: 策略计算后的 DataFrame（process_stock_full_strategy_with_return_target 返回值）
    """
    # 0. 空数据直接返回
    if stock_valid_target.empty:
        print("⚠️  无有效数据，跳过MySQL写入")
        return

    try:
        # 1. 配置数据库连接（复用原有 constants 配置，保持一致性）
        db_config = constants.db_config
        engine_url = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@" \
                     f"{db_config['host']}:{db_config['port']}/{db_config['database']}?charset={db_config['charset']}"
        engine = create_engine(
            engine_url,
            pool_size=10,
            pool_recycle=3600,
            echo=False  # 关闭SQL日志，减少输出
        )

        # 2. 新增 calc_dt 字段（数据入表日期，格式 yyyy-MM-dd）
        stock_data = stock_valid_target.copy()  # 深拷贝，避免修改原数据
        stock_data['calc_dt'] = end_date  # 新增字段

        # 3. 定义目标表字段（严格匹配建表语句，顺序一致）
        target_columns = [
            'calc_dt', 'dt', 'code', 'stock_name', 'price_open', 'price_close',
            'price_highest', 'price_lowest', 'trade', 'trade_amount', 'amplitude',
            'rise', 'amount_increase_decrease', 'turnover_rate', 'industry', 'industry_detail'
        ]

        # 4. 筛选并对齐字段（防止多余字段导致写入失败）
        stock_data = stock_data[target_columns]

        # 5. 先删除当前calc_dt的所有数据（防重复）
        with engine.connect() as conn:
            # 开启事务
            trans = conn.begin()
            try:
                # 删除语句
                delete_sql = text(f"DELETE FROM stock.stock_detail_calc WHERE calc_dt = '{end_date}'")
                conn.execute(delete_sql)
                print(f"🗑️  已删除calc_dt={end_date}的历史数据")

                # 写入新数据
                stock_data.to_sql(
                    name='stock_detail_calc',
                    con=conn,
                    schema='stock',
                    if_exists='append',  # 追加（已删过，无重复）
                    index=False,
                    chunksize=1000  # 分批写入，避免大数据量超时
                )
                trans.commit()  # 提交事务
                print(f"✅ 成功写入{len(stock_data)}条数据到stock.stock_detail_calc（calc_dt={end_date}）")
            except Exception as e:
                trans.rollback()  # 失败回滚
                raise e
            finally:
                conn.close()

    except Exception as e:
        print(f"❌ 写入MySQL失败：{str(e)}")
        raise e  # 抛出异常，方便外层捕获（可选）

# 调用函数，执行策略并导出结果
if __name__ == "__main__":

    start_date = '2025-12-01'
    # end_date = '2026-03-16'
    end_date = datetime.datetime.now().strftime("%Y-%m-%d")

    start = time.time()

    # 执行策略，获取符合条件的目标天数据
    stock_valid_target = process_stock_full_strategy_with_return_target(start_date, end_date)

    # 查看结果并导出CSV
    if not stock_valid_target.empty:
        print("\n=== 符合所有策略的目标天记录预览 ===")
        print(stock_valid_target.head(10))

        # 导出CSV（解决中文乱码）
        csv_file_path = f"../file/k_line_three_times_result_v2_{end_date}.csv"
        stock_valid_target.to_csv(
            csv_file_path,
            index=False,
            encoding='utf-8-sig'
        )
        print(f"\nCSV文件导出成功！保存路径：{csv_file_path}")

        # 新增：调用写入MySQL函数
        insert_data_to_mysql(stock_valid_target, end_date)
    else:
        print("\n❌ 无有效数据，跳过CSV导出和MySQL写入")

    end = time.time()
    cost_seconds = end - start

    # 4. 转成分钟（1分钟=60秒）
    cost_minutes = cost_seconds / 60

    # 5. 输出结果（美观格式）
    print(f"耗时：{cost_seconds:.2f} 秒")
    print(f"耗时：{cost_minutes:.2f} 分钟")