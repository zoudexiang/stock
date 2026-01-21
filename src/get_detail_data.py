import json
import time
from threading import Thread

import requests
from datetime import datetime

from rec import pick_stock_large, pick_stock_huge, do_stock
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
    fields = 'f12,f14,f2,f3,f5,f6,f8,f9,f10,' \
             'f20,f21,f62,f63,f184,' \
             'f64,f65,f66,f67,f68,f69,' \
             'f70,f71,f72,f73,f74,f75,' \
             'f76,f77,f78,f79,f80,f81,' \
             'f82,f83,f84,f85,f86,f87,' \
             'f1,f13'

    url = f'https://push2.eastmoney.com/api/qt/clist/get' \
          f'?pn={pn}&pz={pz}&fid={fid}&po={po}&np={np}&fltt={fltt}&fs={fs}' \
          f'&fields={fields}' \
        # f'cb={cb}&&invt={invt}&ut={ut}'

    response = requests.get(url)
    datas = json.loads(response.text)

    return datas['data']


def store(data):
    with cnx.cursor() as cursor:
        # 遍历数据列表并插入到数据库表中
        for item in data:
            sql = """  
            INSERT INTO stock_detail_data_di (
                f1, f2, f3, f5, f6, f8, f9, f10, f12, f13, f14, f20, f21, f62, f63, f64, f65, f66, f67, f68, f69, 
                f70, f71, f72, f73, f74, f75, f76, f77, f78, f79, f80, f81, f82, f83, f84, f85, f86, f87, f184
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            cursor.execute(sql, tuple(item.values()))

            # 提交事务
        cnx.commit()


if __name__ == '__main__':

    now = datetime.now()
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    today = now.strftime("%Y-%m-%d")

    prefix_morning = ' 09:20:00'
    suffix_morning = ' 11:31:00'

    prefix_afternoon = ' 12:59:00'
    suffix_afternoon = ' 15:01:00'

    print(now)
    try:
        page_size = 2
        while True:
            # 1、需要抓取 data 的时间段
            if (now_str >= today + prefix_morning and now_str <= today + suffix_morning) or (now_str >= today + prefix_afternoon and now_str <= today + suffix_afternoon):
                result = spider(page_size)
                if result['total'] != page_size:
                    page_size = result['total']
                store(result['diff'])
                print(f"now_str = {now_str} -> {datetime.now()} finish store, data size is {len(result['diff'])}.")
                # Thread(target=do_stock).start()
                # Thread(target=pick_stock_huge).start()
                time.sleep(30)
            # 2、中午休市
            elif now_str > today + suffix_morning and now_str < today + prefix_afternoon:
                time.sleep(60)
                print(now_str)
            # 3、当日结束
            else:
                break;
            now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    finally:
        cnx.close()
