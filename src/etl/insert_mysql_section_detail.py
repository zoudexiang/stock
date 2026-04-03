import pandas as pd
from sqlalchemy import create_engine, text
import datetime
import warnings

from src.utils import constants

# 将同花顺下载的 xlsx 文件写入到 stock_detail 表

warnings.filterwarnings('ignore')  # 忽略Excel读取的无关警告

# create table stock.section_detail (
#     dt varchar(10)                  comment '日期，格式 yyyy-MM-dd',
#     section_name varchar(100)       comment '版块名称',
#     rise double                     comment '收盘涨幅',
#     rise_1min double                comment '1分钟涨速',
#     rise_4min double                comment '4分钟涨速',
#     main_force double               comment '主力净量',
#     main_force_amount double        comment '主力金额',
#     up_num int                      comment '涨停数',
#     add_num int                      comment '涨家数',
#     down_num int                    comment '跌家数',
#     leader_stock varchar(100)       comment '领涨股',
#     rise_5day double                comment '5日涨幅',
#     rise_10day double               comment '10日涨幅',
#     rise_20day double                comment '20日涨幅',
#     concept_parse varchar(1000)     comment '概念解析',
#     create_date varchar(100)        comment '创建日期',
#     from_year double                comment '年初至今',
#     from_20160127 double            comment '20160127至今',
#     ratio double                    comment '量比',
#     trade double                    comment '成交量(总手)',
#     trade_amount double             comment '成交额',
#     total_amount double             comment '总市值',
#     trading_market_capitalization double comment '流通市值'
# );

def import_xls_to_section_detail(xls_file_path, dt, db_config):
    """
    将 Table.xls 的指定字段导入 MySQL 表 stock.section_detail
    核心规则：
    1. 仅导入映射字段，多余字段忽略
    2. 导入前清空表删除 dt 日期数据
    3. dt 字段赋值为当前系统日期（yyyy-MM-dd）
    :param xls_file_path: Table.xls 文件的绝对/相对路径
    :param db_config: MySQL 连接配置字典
    """
    # 1. 定义字段映射关系（Excel列名 → MySQL表字段）
    field_mapping = {
        '板块名称': 'section_name',
        '涨幅': 'rise',
        '1分钟涨速': 'rise_1min',
        '4分钟涨速': 'rise_4min',
        '主力净量': 'main_force',
        '主力金额': 'main_force_amount',
        '涨停数': 'up_num',
        '涨家数': 'add_num',
        '跌家数': 'down_num',
        '领涨股': 'leader_stock',
        '5日涨幅': 'rise_5day',
        '10日涨幅': 'rise_10day',
        '20日涨幅': 'rise_20day',
        '概念解析': 'concept_parse',
        '创建日期': 'create_date',
        '年初至今': 'from_year',
        '20160127至今': 'from_20160127',
        '量比': 'ratio',
        '总手': 'trade',
        '总金额': 'trade_amount',
        '总市值': 'total_amount',
        '流通市值': 'trading_market_capitalization'
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

        # ====================== ✅ 核心新增：过滤 ratio 为 '--' 的数据 ======================
        print(f"📊 过滤前数据行数：{len(df_clean)}")
        df_clean = df_clean[df_clean["ratio"] != "--"]  # 过滤量比为 -- 的行

        # 补充 dt 字段（当前系统日期）
        df_clean['dt'] = dt

        # 补充缺失的映射字段（赋值为None，保证表结构完整）
        for mysql_col in field_mapping.values():
            if mysql_col not in df_clean.columns:
                df_clean[mysql_col] = None

        # 按 MySQL 表字段顺序整理最终数据
        final_mysql_cols = [
            'dt', 'section_name', 'rise', 'rise_1min', 'rise_4min', 'main_force', 'main_force_amount', 'up_num', 'add_num',
            'down_num', 'leader_stock', 'rise_5day', 'rise_10day', 'rise_20day', 'concept_parse', 'create_date',
            'from_year', 'from_20160127', 'ratio', 'trade', 'trade_amount', 'total_amount', 'trading_market_capitalization'
        ]
        df_final = df_clean[final_mysql_cols].copy()

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
                del_sql = text(f"delete from stock.section_detail where dt='{dt}';")
                conn.execute(del_sql)
                print(f"🗑️  已删除 stock.section_detail 表 {dt} 数据")

                # 步骤2：批量写入数据
                df_final.to_sql(
                    name='section_detail',
                    con=conn,
                    schema='stock',
                    if_exists='append',
                    index=False,
                    chunksize=1000  # 分批写入，避免大数据量超时
                )
                print(f"✅ 成功导入 {len(df_final)} 条数据到 stock.section_detail")
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
    print(f"》》》》》》》》》》》》 开始执行操作 section_detail 逻辑 》》》》》》》》》》》》")
    import_xls_to_section_detail("C:\\Users\\HR\\Desktop\\工作簿1.xlsx", dt, constants.db_config)