import json
import time
from threading import Thread

import requests
from datetime import datetime

from rec import pick_stock_large, pick_stock_huge
from utils.mysql import connect

cnx = connect()


def spider(pz):
    fid = 'f62'  # 排序字段
    po = 1  # 1正序 / 0倒序
    # pz = 1  # 行数
    pn = 1  # 页码
    np = 1  # 1 list结构 / 0 dict结构
    fltt = 2  # 浮点数小数位

    # cb = 'jQuery112304274f861054022054_1729218285053'
    # invt = 0  # =4则格式不同
    # ut = 'b2884a393a59ad64002292a3e90d46a5'  # 非必填项，意义不明

    # fs = 'm:0+t:6+f:!2,m:0+t:13+f:!2,m:0+t:80+f:!2,m:1+t:2+f:!2,m:1+t:23+f:!2,m:0+t:7+f:!2,m:1+t:3+f:!2'
    fs = 'm:1+t:2+f:!2,m:0+t:6+f:!2,m:0+t:13+f:!2'  # 过滤板块，详见fs.txt

    # 字段
    fields = 'f12,f15'

    url = f'https://push2.eastmoney.com/api/qt/clist/get' \
          f'?pn={pn}&pz={pz}&fid={fid}&po={po}&np={np}&fltt={fltt}&fs={fs}' \
          f'&fields={fields}' \
        # f'cb={cb}&&invt={invt}&ut={ut}'

    response = requests.get(url)
    datas = json.loads(response.text)

    for item in datas['data']['diff']:
        if item['f15']:
            print(item['f12'])

    return datas['data']


def store(data):
    with cnx.cursor() as cursor:
        # 遍历数据列表并插入到数据库表中
        sql = """create table huge_10230955 
        as select created_at,f12,f14,f3,f10,f64,f65,f66,f67,f68,f69 
        from snapshot 
        where created_at between '2024-10-23 09:55:00' - INTERVAL 1 MINUTE and '2024-10-23 09:55:00';
        """
        # cursor.execute(sql, tuple(item.values()))
            # 提交事务
        cnx.commit()


def run():
    page_size = 2
    while True:
        result = spider(page_size)
        if result['total'] != page_size:
            page_size = result['total']
        store(result['diff'])
        # print(f"{datetime.now()} finish store, data size is {len(result['diff'])}.")
        # Thread(target=pick_stock_large).start()
        # Thread(target=pick_stock_huge).start()
        time.sleep(2)


if __name__ == '__main__':
    run()
