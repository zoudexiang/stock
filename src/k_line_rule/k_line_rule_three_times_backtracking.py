import pandas as pd
from src.utils import constants
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


def screen_stocks_backwards_and_export(target_return_dt):
    # 1. 数据库配置
    db_config = constants.db_config

    try:
        # 2. 创建数据库连接
        engine_url = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@" \
                     f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
        engine = create_engine(engine_url)

        # 创建会话
        Session = sessionmaker(bind=engine)
        session = Session()

        # 3. 构建 SQL
        sql_query = f"""
        select 
            dt, 
            a.code, 
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
            where dt <= '{target_return_dt}' 
              and dt >= date_sub('{target_return_dt}', interval 90 day)
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
        """

        print(f"正在从数据库读取数据 (基准日: {target_return_dt})...")
        df = pd.read_sql(sql_query, engine)

        if df.empty:
            print("未查询到数据。")
            return

        # 4. 数据预处理
        df['dt_date'] = pd.to_datetime(df['dt'])
        df = df.sort_values(['code', 'dt_date']).reset_index(drop=True)

        df['pre5d_avg'] = df.groupby('code')['trade_amount'].transform(
            lambda x: x.shift(1).rolling(5).mean()
        )

        valid_target_list = []

        print("开始策略筛选...")
        for code, group in df.groupby('code'):
            group = group.reset_index(drop=True)

            today_idx_list = group[group['dt'] == target_return_dt].index
            if len(today_idx_list) == 0:
                continue

            ret_idx = today_idx_list[0]
            ret_row = group.iloc[ret_idx]

            start_search = max(0, ret_idx - 50)
            end_search = ret_idx - 2

            for idx in range(end_search - 1, start_search - 1, -1):
                target_row = group.iloc[idx]

                if not (target_row['trade_amount'] > 3 * target_row['pre5d_avg'] and
                        target_row['price_close'] > target_row['price_open']):
                    continue

                follow_slice = group.iloc[idx + 1: idx + 4]
                if not follow_slice.empty and follow_slice['trade_amount'].max() > target_row['trade_amount']:
                    dis_row = follow_slice.loc[follow_slice['trade_amount'].idxmax()]
                else:
                    dis_row = target_row

                dis_max_price = max(dis_row['price_open'], dis_row['price_close'])

                if ret_row['price_close'] < dis_max_price:
                    continue

                middle_df = group.iloc[idx + 1: ret_idx]
                if not middle_df.empty and middle_df['price_close'].max() > dis_max_price:
                    continue

                if ret_row['trade_amount'] < (dis_row['trade_amount'] * 0.67):
                    continue

                valid_target_list.append(target_row)
                break

        # 6. 写入 MySQL
        if valid_target_list:
            result_df = pd.DataFrame(valid_target_list)

            original_cols = [
                'dt', 'code', 'stock_name', 'price_open', 'price_close', 'price_highest',
                'price_lowest', 'trade', 'trade_amount', 'amplitude', 'rise',
                'amount_increase_decrease', 'turnover_rate'
            ]
            final_output = result_df[original_cols].drop_duplicates()

            # 删除当天数据
            clear_sql = text(f"truncate table stock_detail_calc_backtracking")
            session.execute(clear_sql)
            session.commit()

            # 写入数据
            final_output.to_sql(
                name='stock_detail_calc_backtracking',
                con=engine,
                if_exists='append',
                index=False,
                chunksize=1000
            )

            print(f"筛选完成！共找到 {len(final_output)} 只符合条件的股票。")
            print(f"✅ 结果已成功写入 MySQL 表：stock_detail_calc_backtracking")

            session.close()
            return final_output
        else:
            print(f"在 {target_return_dt} 未筛选到符合条件的股票。")
            session.close()
            return pd.DataFrame()

    except Exception as e:
        print(f"程序运行出错: {str(e)}")
        return pd.DataFrame()


# 执行
if __name__ == "__main__":
    target_date = '2026-03-26'
    screen_stocks_backwards_and_export(target_date)