"""
@Author  : zoudexiag
@Date    : 2026/6/10
@Time    : 20:00
@Desc    : 每日热榜 top 100
"""
from sqlalchemy import create_engine, text
from datetime import datetime

from src.utils import constants


def insert_mysql_stock_hot(dt, raw_str, db_config):
    # 1. 拆分处理
    code_list = []
    items = raw_str.split(",")
    for idx, item in enumerate(items, start=1):
        # 按.分割，取前面股票代码
        stock_code = item.split(".")[0]
        code_list.append({
            "dt": dt,
            "seq": idx,
            "stock_code": stock_code
        })

    # 3. 数据库连接（替换成你自己的MySQL账号信息）
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
            del_sql = text(f"delete from stock.dim_stock_hot where dt='{dt}';")
            conn.execute(del_sql)
            print(f"🗑️  已删除 stock.dim_stock_hot 表 {dt} 数据")

            import pandas as pd
            df = pd.DataFrame(code_list)

            # 步骤2：批量写入数据
            df.to_sql(
                name='dim_stock_hot',
                con=conn,
                schema='stock',
                if_exists='append',
                index=False,
                chunksize=1000  # 分批写入，避免大数据量超时
            )
            print(f"✅ 成功导入 {len(df)} 条数据到 stock.dim_stock_hot")
            # 提交事务
            trans.commit()
        except Exception as e:
            # 失败回滚
            trans.rollback()
            raise e
        finally:
            conn.close()

if __name__ == '__main__':

    dt = datetime.now().strftime("%Y-%m-%d")

    # 待处理原始字符串
    raw_str = "002354.SZ,600378.SH,601012.SH,603629.SH,600487.SH,002361.SZ,002421.SZ,000725.SZ,605589.SH,002636.SZ,002585.SZ,600522.SH,600396.SH,601991.SH,688146.SH,600869.SH,600367.SH,002971.SZ,605006.SH,002669.SZ,600116.SH,600642.SH,600036.SH,600900.SH,600151.SH,600728.SH,600661.SH,600881.SH,600118.SH,600601.SH,600386.SH,600009.SH,600671.SH,600871.SH,600010.SH,600609.SH,600198.SH,600016.SH,600718.SH,600630.SH,600008.SH,600824.SH,600007.SH,600667.SH,600831.SH,600600.SH,600006.SH,600649.SH,600001.SH,600666.SH,600000.SH,600648.SH,600003.SH,600641.SH,600005.SH,600643.SH,600004.SH,600640.SH,600011.SH,600639.SH,600015.SH,600638.SH,600014.SH,600637.SH,600013.SH,600636.SH,600012.SH,600635.SH,600019.SH,600634.SH,600018.SH,600633.SH,600017.SH,600632.SH,600020.SH,600631.SH,600021.SH,600629.SH,600022.SH,600628.SH,600023.SH,600627.SH,600024.SH,600626.SH,600025.SH,600625.SH,600026.SH,600624.SH,600027.SH,600623.SH,600028.SH,600622.SH,600029.SH,600621.SH,600030.SH,600620.SH,600031.SH,600619.SH,600032.SH,600618.SH,600033.SH,600617.SH,600034.SH,600616.SH,600035.SH,600615.SH,600037.SH,600614.SH,600038.SH,600613.SH,600039.SH,600612.SH,600040.SH,600611.SH,600041.SH,600610.SH,600042.SH,600608.SH,600043.SH,600607.SH,600044.SH,600606.SH,600045.SH,600605.SH,600046.SH,600604.SH,600047.SH,600603.SH,600048.SH,600602.SH,600049.SH,600601.SH,600050.SH,600600.SH"
    insert_mysql_stock_hot(dt, raw_str, constants.db_config)