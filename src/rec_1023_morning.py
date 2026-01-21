from datetime import datetime, timedelta
from utils.feishu import send_message
from utils.mysql import connect


def pick_stock_large(start_time=None):
    if not start_time:
        start_time = datetime.now()
    print(f'{start_time} picking stock by large order')
    cnx = None
    try:
        cnx = connect()
        cur = cnx.cursor()
        last_1_min_query = f"""
            select created_at,f12,f14,f3,f10,f64+f70 as f60,f65+f71 as f61,f62,
                ROUND((f64 + f70)*100/f6,2) as f182,
                ROUND((f65 + f71)*100/f6,2) as f183,f184  
            from snapshot 
            where created_at between  %s - INTERVAL 1 MINUTE and %s
            and f10 > 3 and f67 > 5 and f3 < 7 and f69 > 0 
            and f64 > 8000000 and f66 > 1000000
            and ROUND((f64 + f70)*100/f6,2) / (ROUND((f65 + f71)*100/f6,2)+0.01) > 1.5
                """
        cur.execute(last_1_min_query, (start_time, start_time))
        results = cur.fetchall()
        last_1_min = {t[1]: t for t in results}
        print(f'pick_stock_large length of last_1_min {len(last_1_min)}')

        las_2_min_query = f"""
            select created_at,f12,f14,f3,f10,f64+f70 as f60,f65+f71 as f61,f62,
                ROUND((f64 + f70)*100/f6,2) as f182,
                ROUND((f65 + f71)*100/f6,2) as f183,f184  
            from snapshot 
            where created_at between  %s - INTERVAL 2 MINUTE and %s - INTERVAL 1 MINUTE
            """
        cur.execute(las_2_min_query, (start_time, start_time))
        results = cur.fetchall()
        last_2_min = {t[1]: t for t in results}
        print(f'pick_stock_large length of last_2_min {len(last_2_min)}')

        result = []
        for key, item in last_1_min.items():
            if key not in last_2_min:
                continue
            item_before = last_2_min[key]
            if float(item[10]) - float(item_before[10]) > 4 or \
                    (float(item[5]) - float(item_before[5]) > 20000000 and
                     (float(item[5]) - float(item_before[5])) / float(item_before[5]) > 0.2):
                result.append(key)
        print(f'pick_stock_large length of result {len(result)}')
        if result:
            send_message(f'主力异动 {start_time}', ' , '.join(result))
    finally:
        cnx.close()


def pick_stock_huge(start_time=None):
    if not start_time:
        start_time = datetime.now()
    print(f'{start_time} picking stock by huge order')
    cnx = None
    try:
        cnx = connect()
        cur = cnx.cursor()
        last_1_min_query = f"""
            select created_at,f12,f14,f3,f10,f64,f65,f66,f67,f68,f69 
            from snapshot 
            where created_at between  %s - INTERVAL 1 MINUTE and %s
            and f10 > 4 and f67 > 5 and f3 < 7 
            and f69 > 0 and f64 > 8000000 and f66 > 1000000
            and f67 / (f68+0.01) > 1.5
                """
        cur.execute(last_1_min_query, (start_time, start_time))
        results = cur.fetchall()
        last_1_min = {t[1]: t for t in results}
        print(f'pick_stock_huge length of last_1_min {len(last_1_min)}')

        las_2_min_query = f"""
            select created_at,f12,f14,f3,f10,f64,f65,f66,f67,f68,f69 
            from snapshot 
            where created_at between  %s - INTERVAL 2 MINUTE and %s - INTERVAL 1 MINUTE
            """
        cur.execute(las_2_min_query, (start_time, start_time))
        results = cur.fetchall()
        last_2_min = {t[1]: t for t in results}
        print(f'pick_stock_huge length of last_2_min {len(last_2_min)}')

        result = []
        for key, item in last_1_min.items():
            if key not in last_2_min:
                continue
            item_before = last_2_min[key]
            if float(item[10]) - float(item_before[10]) > 4 or \
                    (float(item[5]) - float(item_before[5]) > 15000000 and
                     (float(item[5]) - float(item_before[5])) / float(item_before[5]) > 0.3):
                result.append(key)
        print(f'pick_stock_huge length of result {len(result)}')
        if result:
            send_message(f'超大单异动 {start_time}', ' , '.join(result))
    finally:
        cnx.close()


def is_serial_increment(scores):
    if len(scores) < 7:
        return False

    # 找出最高分和最低分及其索引
    max_score = max(scores)
    min_score = min(scores)
    max_index = scores.index(max_score)
    min_index = scores.index(min_score)

    trimmed_scores = [score for i, score in enumerate(scores) if i != max_index and i != min_index]

    # 检查是否不断增长
    is_increasing = all(current > previous for previous, current in zip(trimmed_scores, trimmed_scores[1:]))

    if is_increasing:
        print("去掉最高分和最低分后的列表:", trimmed_scores)
        print("是否不断增长:", is_increasing)
        return True
    return False


def pick_stock_serial(start_time=None):
    if not start_time:
        start_time = datetime.now()
    print(f'{start_time} picking stock by huge order')

    cnx = None
    try:
        cnx = connect()
        cur = cnx.cursor()
        last_1_min_query = f"""
                select f12
                from snapshot 
                where created_at between  %s - INTERVAL 1 MINUTE and %s
                and f10 > 2 and f3 < 7 and f69 > 5
                    """
        cur.execute(last_1_min_query, (start_time, start_time))
        results = cur.fetchall()
        last_1_min = [t[0] for t in results]
        print(f'pick_stock_huge length of last_1_min {len(last_1_min)}')
        result = []
        for st in last_1_min:
            serial_query = f"""
            select f65, f69 
            from snapshot 
            where f12 = %s and created_at < %s
            order by created_at desc limit 7"""
            cur.execute(serial_query, (st, start_time))
            results = cur.fetchall()
            list1 = [t[0] for t in reversed(results)]
            list2 = [t[1] for t in reversed(results)]
            if is_serial_increment(list1) and is_serial_increment(list2):
                result.append(st)
            print(f'pick_stock_huge length of result {len(result)}')
        if result:
            send_message(f'买量持续增长5分钟 {start_time}', ','.join(result))
    finally:
        cnx.close()


if __name__ == '__main__':
    from datetime import datetime, timedelta
    import time

    begin = datetime.strptime("2024-10-22 09:38", "%Y-%m-%d %H:%M")

    while True:
        print(begin.strftime("%Y-%m-%d %H:%M"))
        # pick_stock_serial(begin)
        # pick_stock_large(begin)
        pick_stock_huge(begin)
        begin += timedelta(minutes=1)
        time.sleep(2)

