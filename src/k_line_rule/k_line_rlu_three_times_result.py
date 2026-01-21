import pandas as pd
from sqlalchemy import create_engine

# 现有 mysql 建表语句如下，这张表是每天大 A 股市每个交易日每只股票的每日数据情况，表中字段情况和描述如下
#
# create table stock.stock_detail (
#     dt varchar(10)                  comment '日期，格式 yyyy-MM-dd',
#     code varchar(6)                 comment '股票代码',
#     price_open double               comment '开盘价',
#     price_close double              comment '收盘价',
#     price_highest double            comment '最高价',
#     price_lowest double             comment '最低价',
#     trade double                    comment '成交量(总手)',
#     trade_amount double             comment '成交额',
#     amplitude double                comment '振幅',
#     rise double                     comment '收盘涨幅',
#     amount_increase_decrease double comment '涨跌额',
#     turnover_rate double            comment '换手率',
#     stock_code varchar(6)           comment '股票代码',
#     stock_name varchar(100)         comment '股票名称'
# );
#
#
# 表中现在有 2025-01-01 ~ 2026-01-16 范围的数据，现在我给出一个策略，请帮我依据这个策略，帮我写出一个 sql，将符合策略的股票帮我筛选出来
#
# 策略规则：
# 在开始日期和结束日期范围内，单只股票从开始日期算起，突然有一天的成交额是前五日的成交额平均值的三倍以上，且(收盘价 - 开盘价)>0, 股票中的术语也就是当天收阳线，我把这一天称为目标天，
# 计算出目标天后，以目标天为起始日期，观察后面3天是否有成交额比目标天还大的，如果有，取最大一天成交额的为分歧天，如果没有，目标天就是分歧天，
# 现在给出分歧天中一个指标的定义，我们定义为分歧天最高实体价(disagreementMaxPrice)，如果开盘价>收盘价，则 disagreementMaxPrice=开盘价，否则 disagreementMaxPrice=收盘价
# 查找各个股票后续天 收盘价 >= disagreementMaxPrice 的股票
#
# 一个股票在开始日期和结束日期范围内可以有多个，有点类似于动态窗口的概念，
#
#
# 请帮我找出每只股票符合这种策略规则的股票， 数据输出还是按照上述 mysql 表的字段结构来
# 帮我使用 python 代码实现

from src.utils import constants
def process_stock_full_strategy(start_date: str, end_date: str):
    """
    从MySQL读取股票数据，实现全流程策略逻辑，返回符合条件的股票记录（保留原表结构）
    :param start_date: 开始日期（格式：yyyy-MM-dd）
    :param end_date: 结束日期（格式：yyyy-MM-dd）
    :return: 符合完整策略的股票数据（DataFrame）
    """
    # 1. 配置MySQL数据库连接参数（替换为你的实际配置）
    db_config = constants.db_config

    try:
        # 2. 构建SQLAlchemy引擎（符合pandas推荐规范，消除read_sql警告）
        engine_url = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@" \
                     f"{db_config['host']}:{db_config['port']}/{db_config['database']}?charset={db_config['charset']}"
        engine = create_engine(engine_url)

        # 3. 定义SQL查询语句（读取原始数据，%转义为%%避免与参数占位符%s冲突）
        sql_query = f'''
        select 
            * 
        from stock.stock_detail
        where dt>='{start_date}' 
            and dt<='{end_date}' 
            and code not like '688%%'
            and code not like '920%%' 
        order by code, dt;
        '''

        # 4. 读取MySQL数据到DataFrame（参数化查询，避免SQL注入和字符解析冲突）
        df = pd.read_sql(sql_query, engine)
        if df.empty:
            print("提示：未查询到指定日期范围内的原始股票数据")
            return pd.DataFrame()

        # 5. 数据预处理：转换日期格式+排序，确保后续计算准确
        df['dt_date'] = pd.to_datetime(df['dt'], format='%Y-%m-%d')
        df = df.sort_values(by=['code', 'dt_date']).reset_index(drop=True)

        # 6. 按股票分组，计算前5日成交额平均值（筛选目标天的前置条件）
        def calculate_pre5d_trade_avg(group):
            """计算单只股票的前5日成交额平均值（排除当日，确保数据完整）"""
            group['pre5d_trade_amount_avg'] = group['trade_amount'].shift(1).rolling(
                window=5, min_periods=5  # min_periods=5：确保前5日数据完整，无空值
            ).mean()
            return group

        # 分组计算，添加include_groups=False消除pandas FutureWarning
        # df = df.groupby('code', include_groups=False).apply(calculate_pre5d_trade_avg).reset_index(drop=True)
        df = df.groupby('code').apply(calculate_pre5d_trade_avg).reset_index(drop=True)

        # 7. 筛选「目标天」记录（应用目标天核心条件）
        target_df = df[
            (df['trade_amount'] > 3 * df['pre5d_trade_amount_avg']) &  # 成交额>前5日均值×3
            (df['pre5d_trade_amount_avg'].notna()) &  # 前5日数据完整，排除空值
            ((df['price_close'] - df['price_open']) > 0)  # 收阳线，收盘价>开盘价
            ].copy()

        if target_df.empty:
            print("提示：未筛选到符合条件的目标天数据")
            return pd.DataFrame()

        # 重命名目标天相关字段，便于后续分歧天计算
        target_df.rename(
            columns={
                'trade_amount': 'target_trade_amount',
                'price_open': 'target_open',
                'price_close': 'target_close',
                'dt': 'target_dt',
                'dt_date': 'target_dt_date'
            },
            inplace=True
        )

        # 8. 确定「分歧天」+ 计算「分歧天最高实体价」（核心步骤）
        disagreement_list = []
        for _, target_row in target_df.iterrows():
            # 提取目标天核心信息
            code = target_row['code']
            target_dt_date = target_row['target_dt_date']
            target_trade_amount = target_row['target_trade_amount']
            target_open = target_row['target_open']
            target_close = target_row['target_close']

            # 获取该股票目标天后的3个交易日数据（交易日，非自然日）
            follow_df = df[
                (df['code'] == code) &
                (df['dt_date'] > target_dt_date)
                ].sort_values(by='dt_date').head(3).copy()  # head(3)：仅取后续3个交易日

            # 确定分歧天核心信息
            if not follow_df.empty:
                # 筛选后续3天中成交额>目标天的记录
                bigger_follow_df = follow_df[follow_df['trade_amount'] > target_trade_amount].copy()

                if not bigger_follow_df.empty:
                    # 取成交额最大的记录（最大值相同取最早日期，符合策略要求）
                    bigger_follow_df = bigger_follow_df.sort_values(
                        by=['trade_amount', 'dt_date'],
                        ascending=[False, True]  # 先按成交额降序，再按日期升序
                    ).reset_index(drop=True)

                    # 提取成交额最大的分歧天数据
                    disagreement_row = bigger_follow_df.iloc[0]
                    disagreement_dt = disagreement_row['dt']
                    disagreement_dt_date = disagreement_row['dt_date']
                    disagreement_open = disagreement_row['price_open']
                    disagreement_close = disagreement_row['price_close']
                else:
                    # 后续3天无成交额>目标天的记录，分歧天=目标天
                    disagreement_dt = target_row['target_dt']
                    disagreement_dt_date = target_row['target_dt_date']
                    disagreement_open = target_row['target_open']
                    disagreement_close = target_row['target_close']
            else:
                # 无后续交易日，分歧天=目标天
                disagreement_dt = target_row['target_dt']
                disagreement_dt_date = target_row['target_dt_date']
                disagreement_open = target_row['target_open']
                disagreement_close = target_row['target_close']

            # 计算分歧天最高实体价（disagreementMaxPrice）
            disagreement_max_price = disagreement_open if disagreement_open > disagreement_close else disagreement_close

            # 存储分歧天核心信息（用于后续最终结果筛选）
            disagreement_list.append({
                'code': code,
                'disagreement_dt': disagreement_dt,
                'disagreement_dt_date': disagreement_dt_date,
                'disagreementMaxPrice': disagreement_max_price
            })

        # 转换为DataFrame并去重，避免重复数据
        disagreement_df = pd.DataFrame(disagreement_list).drop_duplicates()

        # 9. 筛选最终结果（分歧天后收盘价≥分歧天最高实体价，保留原表结构）
        # 关联分歧天数据与原始数据
        result_df = df.merge(disagreement_df, on='code', how='left')

        # 应用最终筛选条件
        final_result = result_df[
            (result_df['disagreement_dt_date'].notna()) &  # 分歧天数据完整
            (result_df['dt_date'] >= result_df['disagreement_dt_date']) &  # 交易日在分歧天之后（含分歧天）
            (result_df['price_close'] >= result_df['disagreementMaxPrice'])  # 收盘价≥分歧天最高实体价
            ].copy()

        # 10. 保留原表字段结构（删除所有辅助字段，仅保留stock_detail表的原始字段）
        original_table_columns = [
            'dt', 'code', 'price_open', 'price_close', 'price_highest',
            'price_lowest', 'trade', 'trade_amount', 'amplitude', 'rise',
            'amount_increase_decrease', 'turnover_rate', 'stock_code', 'stock_name'
        ]
        final_result = final_result[original_table_columns].drop_duplicates()

        # 11. 打印结果统计信息
        print(f"全流程策略执行完成！符合条件的股票记录共 {len(final_result)} 条")
        return final_result

    except Exception as e:
        print(f"执行失败：{str(e)}")
        return pd.DataFrame()


# 12. 调用函数，执行全流程策略并可选导出CSV
if __name__ == "__main__":

    # 定义起止日期（可根据需求修改，格式：yyyy-MM-dd）
    START_DATE = '2025-11-01'
    END_DATE = '2026-01-16'

    # 执行全流程策略，获取最终结果
    stock_final_result = process_stock_full_strategy(START_DATE, END_DATE)

    # 可选：查看前10条结果预览
    if not stock_final_result.empty:
        print("\n=== 前10条符合策略的记录预览 ===")
        print(stock_final_result.head(10))

        # 可选：导出为CSV文件（解决中文乱码，不导出行索引，保留原表结构）
        csv_file_path = "../file/k_line_rule_three_times_result.csv"
        stock_final_result.to_csv(
            csv_file_path,
            index=False,  # 不导出pandas行索引，保证格式整洁
            encoding='utf-8-sig'  # 兼容Windows Excel，避免中文乱码
        )
        print(f"\nCSV文件导出成功！保存路径：{csv_file_path}")