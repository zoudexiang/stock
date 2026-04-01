import pandas as pd
from sqlalchemy import create_engine, text
import datetime
import warnings

from src.utils import constants

# 将同花顺下载的 xlsx 文件写入到 stock_detail 表
# 将同花顺下载的 xlsx 文件写入到 dim_stock_tag 表

warnings.filterwarnings('ignore')  # 忽略Excel读取的无关警告


def import_xls_to_stock_detail_tmp(xls_file_path, dt, db_config):
    """
    将 Table.xls 的指定字段导入 MySQL 表 stock.stock_detail_tmp
    核心规则：
    1. 仅导入映射字段，多余字段忽略
    2. 导入前清空表（truncate）
    3. dt 字段赋值为当前系统日期（yyyy-MM-dd）
    :param xls_file_path: Table.xls 文件的绝对/相对路径
    :param db_config: MySQL 连接配置字典
    """
    # 1. 定义字段映射关系（Excel列名 → MySQL表字段）
    field_mapping = {
        '代码': 'code',
        '名称': 'stock_name',
        '开盘': 'price_open',
        '现价': 'price_close',
        '最高': 'price_highest',
        '最低': 'price_lowest',
        '总手': 'trade',
        '总金额': 'trade_amount',
        '振幅': 'amplitude',
        '涨幅': 'rise',
        '涨跌': 'amount_increase_decrease',
        '换手': 'turnover_rate'
    }

    # 2. 读取 Excel 文件（兼容 .xls 格式）
    try:
        print(f"🔍 开始读取 Excel 文件：{xls_file_path}")
        # 读取第一个 sheet，首行作为列名
        df = pd.read_excel(xls_file_path, sheet_name=0, header=0)

        # ========== 关键修改1：自动去除列名首尾空格 ==========
        df.columns = df.columns.str.strip()  # 去除所有列名的前置/后置空格（包括"    名称"→"名称"）
        print(f"ℹ️  列名去空格后：{df.columns.tolist()}")

        if df.empty:
            print("❌ 错误：Excel 文件中无数据，导入终止")
            return
        print(f"✅ 成功读取 Excel，共 {len(df)} 行原始数据")
    except Exception as e:
        print(f"❌ 读取 Excel 失败：{str(e)}")
        return

    # 3. 数据清洗：筛选字段 + 重命名 + 补充dt
    try:
        # 筛选Excel中存在的映射字段（避免列名缺失报错）
        valid_excel_cols = [col for col in field_mapping.keys() if col in df.columns]
        if not valid_excel_cols:
            print("❌ 错误：Excel 中无匹配的目标字段，导入终止")
            return

        # 筛选数据并按映射关系重命名
        df_clean = df[valid_excel_cols].rename(columns=field_mapping)

        # 补充 dt 字段（当前系统日期）
        df_clean['dt'] = dt

        # 补充缺失的映射字段（赋值为None，保证表结构完整）
        for mysql_col in field_mapping.values():
            if mysql_col not in df_clean.columns:
                df_clean[mysql_col] = None

        # 按MySQL表字段顺序整理最终数据
        final_mysql_cols = [
            'dt', 'code', 'stock_name', 'price_open', 'price_close',
            'price_highest', 'price_lowest', 'trade', 'trade_amount',
            'amplitude', 'rise', 'amount_increase_decrease', 'turnover_rate'
        ]
        df_final = df_clean[final_mysql_cols].copy()

        # 数据类型适配（关键！避免写入MySQL时报类型错误）
        # 股票代码转为字符串（防止00开头代码丢失前导0）
        df_final['code'] = df_final['code'].astype(str).str.strip()
        # 数值字段转为 double，非数字值转为 NaN（MySQL 中存为 NULL）
        numeric_cols = [
            'price_open', 'price_close', 'price_highest', 'price_lowest',
            'trade', 'trade_amount', 'amplitude', 'rise',
            'amount_increase_decrease', 'turnover_rate'
        ]
        for col in numeric_cols:
            df_final[col] = pd.to_numeric(df_final[col], errors='coerce')

        print(f"✅ 数据清洗完成，待导入 {len(df_final)} 行有效数据")
    except Exception as e:
        print(f"❌ 数据清洗失败：{str(e)}")
        return

    # 4. 连接 MySQL 执行导入（先 truncate，再写入）
    try:
        # 构建 SQLAlchemy 引擎
        engine_url = (
            f"mysql+pymysql://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['database']}?charset=utf8mb4"
        )
        engine = create_engine(engine_url, pool_size=10, pool_recycle=3600)

        with engine.connect() as conn:
            # 开启事务，保证操作原子性
            trans = conn.begin()
            try:
                # 步骤1：清空表（truncate）
                truncate_sql = text("truncate table stock.stock_detail_tmp")
                conn.execute(truncate_sql)
                print("🗑️  已清空 stock.stock_detail_tmp 表")

                # 步骤2：批量写入数据
                df_final.to_sql(
                    name='stock_detail_tmp',
                    con=conn,
                    schema='stock',
                    if_exists='append',
                    index=False,
                    chunksize=1000  # 分批写入，避免大数据量超时
                )
                print(f"✅ 成功导入 {len(df_final)} 条数据到 stock.stock_detail_tmp")

                # 步骤3：删除 stock.stock_detail 表 dt 的数据
                del_sql = text(f"delete from stock.stock_detail where dt='{dt}';")
                conn.execute(del_sql)
                print(f"✅ 成功删除 stock.stock_detail 表 dt={dt} 的数据")

                # 步骤4：将 stock_detail_tmp 表 dt 的数据写入到 stock_detail
                insert_sql = text("""
                                insert into stock.stock_detail
                                select 
                                    dt,
                                    replace(replace(code, 'SZ', ''), 'SH', '') as code,
                                    stock_name,
                                    price_open,
                                    price_close,
                                    price_highest,
                                    price_lowest,
                                    trade,
                                    trade_amount,
                                    round(amplitude * 100, 2) as amplitude,
                                    round(rise * 100, 2) as rise,
                                    amount_increase_decrease,
                                    round(turnover_rate * 100, 2) as turnover_rate
                                from stock.stock_detail_tmp;
                                """)
                conn.execute(insert_sql)
                print(f"✅ 成功写入 stock.stock_detail 表 dt={dt} 的数据")
                # 提交事务
                trans.commit()
            except Exception as e:
                # 失败回滚
                trans.rollback()
                raise e
            finally:
                conn.close()
    except Exception as e:
        print(f"❌ 写入MySQL失败：{str(e)}")
        return

def import_xls_to_dim_stock_tag(xls_file_path, dt, db_config):
    """
    将 Table.xls 的指定字段导入 MySQL 表 stock.stock_detail_tmp
    核心规则：
    1. 仅导入映射字段，多余字段忽略
    2. 导入前清空表（truncate）
    3. dt 字段赋值为当前系统日期（yyyy-MM-dd）
    :param xls_file_path: Table.xls 文件的绝对/相对路径
    :param db_config: MySQL 连接配置字典
    """
    # 1. 定义字段映射关系（Excel列名 → MySQL表字段）
    field_mapping = {
        '代码': 'code',
        '所属行业': 'industry',
        '细分行业': 'industry_detail'
    }

    # 2. 读取 Excel 文件（兼容 .xls 格式）
    try:
        print(f"🔍 开始读取 Excel 文件：{xls_file_path}")
        # 读取第一个 sheet，首行作为列名
        df = pd.read_excel(xls_file_path, sheet_name=0, header=0)

        # ========== 关键修改1：自动去除列名首尾空格 ==========
        df.columns = df.columns.str.strip()  # 去除所有列名的前置/后置空格（包括"    名称"→"名称"）
        print(f"ℹ️  列名去空格后：{df.columns.tolist()}")

        if df.empty:
            print("❌ 错误：Excel 文件中无数据，导入终止")
            return
        print(f"✅ 成功读取 Excel，共 {len(df)} 行原始数据")
    except Exception as e:
        print(f"❌ 读取 Excel 失败：{str(e)}")
        return

    # 3. 数据清洗：筛选字段 + 重命名 + 补充dt
    try:
        # 筛选Excel中存在的映射字段（避免列名缺失报错）
        valid_excel_cols = [col for col in field_mapping.keys() if col in df.columns]
        if not valid_excel_cols:
            print("❌ 错误：Excel 中无匹配的目标字段，导入终止")
            return

        # 筛选数据并按映射关系重命名
        df_clean = df[valid_excel_cols].rename(columns=field_mapping)

        # 补充缺失的映射字段（赋值为None，保证表结构完整）
        for mysql_col in field_mapping.values():
            if mysql_col not in df_clean.columns:
                df_clean[mysql_col] = None

        # 按 MySQL 表字段顺序整理最终数据
        final_mysql_cols = ['code', 'industry', 'industry_detail']
        df_final = df_clean[final_mysql_cols].copy()

        # 数据类型适配（关键！避免写入MySQL时报类型错误）
        # 股票代码转为字符串（防止00开头代码丢失前导0）
        df_final['code'] = df_final['code'].astype(str).str.strip()
        print(f"✅ 数据清洗完成，待导入 {len(df_final)} 行有效数据")
    except Exception as e:
        print(f"❌ 数据清洗失败：{str(e)}")
        return

    # 4. 连接 MySQL 执行导入（先 truncate，再写入）
    try:
        # 构建 SQLAlchemy 引擎
        engine_url = (
            f"mysql+pymysql://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['database']}?charset=utf8mb4"
        )
        engine = create_engine(engine_url, pool_size=10, pool_recycle=3600)

        with engine.connect() as conn:
            # 开启事务，保证操作原子性
            trans = conn.begin()
            try:
                # 步骤1：清空表（truncate）
                truncate_sql = text("truncate table stock.dim_stock_tag")
                conn.execute(truncate_sql)
                print("🗑️  已清空 stock.dim_stock_tag 表")

                # 步骤2：批量写入数据
                df_final.to_sql(
                    name='dim_stock_tag',
                    con=conn,
                    schema='stock',
                    if_exists='append',
                    index=False,
                    chunksize=1000  # 分批写入，避免大数据量超时
                )
                print(f"✅ 成功导入 {len(df_final)} 条数据到 stock.dim_stock_tag")
                # 提交事务
                trans.commit()
            except Exception as e:
                # 失败回滚
                trans.rollback()
                raise e
            finally:
                conn.close()
    except Exception as e:
        print(f"❌ 写入MySQL失败：{str(e)}")
        return


# 主函数调用（修改配置后直接运行）
if __name__ == "__main__":

    # 获取当前系统日期（赋值给dt字段）
    # dt = '2026-03-20'
    dt = datetime.datetime.now().strftime("%Y-%m-%d")
    print(f"📅 本次导入的 dt 字段值：{dt}")
    import_xls_to_stock_detail_tmp("C:\\Users\\HR\\Desktop\\工作簿1.xlsx", dt, constants.db_config)
    print(f"》》》》》》》》》》》》 开始执行操作 dim_stock_tag 逻辑 》》》》》》》》》》》》")
    import_xls_to_dim_stock_tag("C:\\Users\\HR\\Desktop\\工作簿1.xlsx", dt, constants.db_config)