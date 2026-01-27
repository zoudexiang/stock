"""
@Author  : zoudexiag
@Date    : 2026/1/27
@Time    : 11:34
@Desc    : 非常实用的“盘后选股”逻辑。将今天（2026-01-26）作为确定的“返回目标天”，反向回溯寻找过去 50 个交易日（约 2 个半月）左右是否存在满足条件的“目标天”和“分歧天”
           50 个交易日可以捕捉到更长周期的“挖坑回升”或“平台整理后突破”的形态
           这种逻辑的优势在于：它不再是漫无目的地筛选历史，而是针对性地寻找今天发出“买入/确认信号”的股票
"""

import pandas as pd
from src.utils import constants
from sqlalchemy import create_engine

def screen_stocks_backwards_and_export(target_return_dt):

    # 1. 数据库配置
    db_config = constants.db_config

    try:
        # 2. 创建数据库连接
        engine_url = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@" \
                     f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
        engine = create_engine(engine_url)

        # 3. 构建 SQL：为了效率，我们只取 target_return_dt 及其前 70 天的数据（确保 50 个交易日及 5 日均线计算）
        # 同时过滤掉 ST、科创板等
        sql_query = f"""
        select 
            * 
        from stock.stock_detail 
        where dt <= '{target_return_dt}' 
          and dt >= date_sub('{target_return_dt}', interval 90 day)
          and code not like '688%%' 
          and code not like '920%%' 
          and stock_name not like '%%st%%'
        order by code, dt;
        """

        print(f"正在从数据库读取数据 (基准日: {target_return_dt})...")
        df = pd.read_sql(sql_query, engine)

        if df.empty:
            print("未查询到数据。")
            return

        # 4. 数据预处理
        df['dt_date'] = pd.to_datetime(df['dt'])
        df = df.sort_values(['code', 'dt_date']).reset_index(drop=True)

        # 计算前5日成交额均值
        df['pre5d_avg'] = df.groupby('code')['trade_amount'].transform(
            lambda x: x.shift(1).rolling(5).mean()
        )

        valid_target_list = []

        # 5. 执行核心回溯策略 (回溯 50 个交易日)
        print("开始策略筛选...")
        for code, group in df.groupby('code'):
            group = group.reset_index(drop=True)

            # 定位“今日”（返回目标天）
            today_idx_list = group[group['dt'] == target_return_dt].index
            if len(today_idx_list) == 0:
                continue

            ret_idx = today_idx_list[0]
            ret_row = group.iloc[ret_idx]

            # 回溯范围：从 ret_idx-3 开始往前搜 50 个交易日
            start_search = max(0, ret_idx - 50)
            end_search = ret_idx - 2

            # 倒序遍历，寻找最近的一个符合条件的目标天
            for idx in range(end_search - 1, start_search - 1, -1):
                target_row = group.iloc[idx]

                # 规则 1：目标天校验 (3倍成交额 & 阳线)
                if not (target_row['trade_amount'] > 3 * target_row['pre5d_avg'] and
                        target_row['price_close'] > target_row['price_open']):
                    continue

                # 规则 2：确定分歧天 (目标天后3日内)
                follow_slice = group.iloc[idx + 1: idx + 4]
                if not follow_slice.empty and follow_slice['trade_amount'].max() > target_row['trade_amount']:
                    dis_row = follow_slice.loc[follow_slice['trade_amount'].idxmax()]
                else:
                    dis_row = target_row

                dis_max_price = max(dis_row['price_open'], dis_row['price_close'])

                # 规则 4 & 5 组合校验
                # A. 今日价格突破/达到分歧天最高实体价
                if ret_row['price_close'] < dis_max_price:
                    continue

                # B. 中间过程约束：[目标天+1, 今日-1] 期间收盘价未曾突破过 dis_max_price
                middle_df = group.iloc[idx + 1: ret_idx]
                if not middle_df.empty and middle_df['price_close'].max() > dis_max_price:
                    continue

                # C. 规则 5：今日成交额 >= 分歧天成交额 * 0.67
                if ret_row['trade_amount'] < (dis_row['trade_amount'] * 0.67):
                    continue

                # 校验通过，记录目标天原始数据
                valid_target_list.append(target_row)
                break

        # 6. 结果处理与导出
        if valid_target_list:
            result_df = pd.DataFrame(valid_target_list)

            # 仅保留原始表字段
            original_cols = [
                'dt', 'code', 'price_open', 'price_close', 'price_highest',
                'price_lowest', 'trade', 'trade_amount', 'amplitude', 'rise',
                'amount_increase_decrease', 'turnover_rate', 'stock_code', 'stock_name'
            ]
            final_output = result_df[original_cols].drop_duplicates()

            # 生成文件名并导出
            file_name = f"../file/k_line_rule_three_times_backtracking_{target_return_dt}.csv"
            final_output.to_csv(file_name, index=False, encoding='utf-8-sig')

            print(f"筛选完成！共找到 {len(final_output)} 只符合条件的股票。")
            print(f"结果已导出至: {file_name}")
            return final_output
        else:
            print(f"在 {target_return_dt} 未筛选到符合条件的股票。")
            return pd.DataFrame()

    except Exception as e:
        print(f"程序运行出错: {str(e)}")
        return pd.DataFrame()


# 执行
if __name__ == "__main__":

    # 设定盘后选股的日期(返回目标天)
    target_date = '2026-01-26'
    screen_stocks_backwards_and_export(target_date)

    # 纯 sql 版本替代 python 版本(无优化版)
    sql_ori = f'''
    with base_data as (
        select
            *,
            (
                lag(trade_amount, 1) over w +
                lag(trade_amount, 2) over w +
                lag(trade_amount, 3) over w +
                lag(trade_amount, 4) over w +
                lag(trade_amount, 5) over w
            ) / 5 as avg_5d,
            row_number() over w as rn
        from stock.stock_detail
        where dt>='2025-11-01'
          and dt<='2026-01-26'
          and code not like '688%'
          and code not like '920%'
          -- 建议顺便过滤掉 ST 股，这类股票波动异常，通常不符合该策略
          and lower(stock_name) not like '%st%'
        window w as (partition by code order by dt)

    ),

    today_anchor as (
        -- 锁定今天的基准数据
        select
            *
        from base_data
        where dt = '2026-01-26'
    )

    select
        -- 目标天日期，即发生“三倍放量”且“收阳线”的那一天的日期
        t.dt as target_dt,
        t.code,
        t.stock_name,
        t.price_open,
        t.price_close,
        t.trade_amount,
        -- 返回目标天日期
        '2026-01-26' as return_target_dt
    from today_anchor r
    join base_data t on r.code = t.code
        -- 规则4：至少间隔2天
        and cast(t.rn as signed) <= cast(r.rn as signed) - 3
        -- 回溯约50个交易日
        and cast(t.rn as signed) >= cast(r.rn as signed) - 53
    join lateral (
        -- 确定分歧天及其最高实体价
        select
            case
                when b.trade_amount > t.trade_amount then b.trade_amount
                else t.trade_amount end as dis_trade_amt,
            case
                when b.trade_amount > t.trade_amount then greatest(b.price_open, b.price_close)
                else greatest(t.price_open, t.price_close) end as dis_max_price
        from (select 1) dummy
        left join base_data b on b.code = t.code and b.rn > t.rn and b.rn <= t.rn + 3
        order by b.trade_amount desc, b.dt asc
        limit 1
    ) dis on true
    where
        -- 1. 目标天校验
        t.trade_amount > 3 * t.avg_5d
        and t.price_close > t.price_open
        -- 2. 今日（返回天）价格校验
        and r.price_close >= dis.dis_max_price
        -- 3. 成交额规则 5 校验
        and r.trade_amount >= dis.dis_trade_amt * 0.67
        -- 4. 中间过程约束：中间任何一天收盘价不能超过分歧天最高实体价
        and not exists (
            select 1 from base_data m
            where m.code = t.code
              and m.rn > t.rn
              and m.rn < r.rn
              and m.price_close > dis.dis_max_price
        );
    '''

    sql_optimization = f'''
    with base_data as (
        -- 1. 提前过滤掉不需要的股票，减少窗口函数计算量
        select
            *,
            (
                lag(trade_amount, 1) over w +
                lag(trade_amount, 2) over w +
                lag(trade_amount, 3) over w +
                lag(trade_amount, 4) over w +
                lag(trade_amount, 5) over w
            ) / 5 as avg_5d,
            row_number() over w as rn
        from stock.stock_detail
        where dt>='2025-11-01'
          and dt<='2026-01-26'
          and code not like '688%'
          and code not like '920%'
          and lower(stock_name) not like '%st%'
        window w as (partition by code order by dt)
    ),
    
    today_anchor as (
        -- 2. 锁定今日数据作为锚点
        select
            *
        from base_data
        where dt = '2026-01-26'
    ),
    
    potential_targets as (
        -- 3. 【核心优化】先初步筛选出符合规则1的目标天，极大缩小后续 JOIN 的数据量
        select
            b.*,
            r.rn as today_rn,
            r.price_close as today_close,
            r.trade_amount as today_trade_amt
        from today_anchor r
        join base_data b on r.code = b.code
          -- 使用 cast 防止负数溢出报错
          and cast(b.rn as signed) between cast(r.rn as signed) - 53 and cast(r.rn as signed) - 3
        where b.trade_amount > 3 * b.avg_5d
          and b.price_close > b.price_open
    )
    -- 4. 最后再对极少数初步入选的股票进行复杂的“分歧天”和“中间价”校验
    select
        p.dt as target_dt,
        p.code,
        p.stock_name,
        p.price_open,
        p.price_close,
        p.trade_amount,
        '2026-01-26' as return_target_dt
    from potential_targets p
    join lateral (
        select
            case
                when b.trade_amount > p.trade_amount then b.trade_amount
                else p.trade_amount end as dis_trade_amt,
            case
                when b.trade_amount > p.trade_amount then greatest(b.price_open, b.price_close)
                else greatest(p.price_open, p.price_close) end as dis_max_price
        from (select 1) dummy
        left join base_data b on b.code = p.code and b.rn > p.rn and b.rn <= p.rn + 3
        order by b.trade_amount desc, b.dt asc
        limit 1
    ) dis on true
    where
        p.today_close >= dis.dis_max_price
        and p.today_trade_amt >= dis.dis_trade_amt * 0.67
        and not exists (
            select 1 from base_data m
            where m.code = p.code
              and m.rn > p.rn
              and m.rn < p.today_rn
              and m.price_close > dis.dis_max_price
        );
    '''