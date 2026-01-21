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
                     (float(item[5]) - float(item_before[5])) / float(item_before[5]) > 0.1):
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
                     (float(item[5]) - float(item_before[5])) / float(item_before[5]) > 0.1):
                result.append(key)
        print(f'pick_stock_huge length of result {len(result)}')
        if result:
            send_message(f'超大单异动 {start_time}', ' , '.join(result))
    finally:
        cnx.close()


# def is_serial_increment(scores):
#     if len(scores) < 7:
#         return False
#
#     # 找出最高分和最低分及其索引
#     max_score = max(scores)
#     min_score = min(scores)
#     max_index = scores.index(max_score)
#     min_index = scores.index(min_score)
#
#     trimmed_scores = [score for i, score in enumerate(scores) if i != max_index and i != min_index]
#
#     # 检查是否不断增长
#     is_increasing = all(current > previous for previous, current in zip(trimmed_scores, trimmed_scores[1:]))
#
#     if is_increasing:
#         print("去掉最高分和最低分后的列表:", trimmed_scores)
#         print("是否不断增长:", is_increasing)
#         return True
#     return False
#
#
# def pick_stock_serial(start_time=None):
#     if not start_time:
#         start_time = datetime.now()
#     print(f'{start_time} picking stock by huge order')
#
#     cnx = None
#     try:
#         cnx = connect()
#         cur = cnx.cursor()
#         last_1_min_query = f"""
#                 select f12
#                 from snapshot
#                 where created_at between  %s - INTERVAL 1 MINUTE and %s
#                 and f10 > 2 and f3 < 7 and f69 > 5
#                     """
#         cur.execute(last_1_min_query, (start_time, start_time))
#         results = cur.fetchall()
#         last_1_min = [t[0] for t in results]
#         print(f'pick_stock_huge length of last_1_min {len(last_1_min)}')
#         result = []
#         for st in last_1_min:
#             serial_query = f"""
#             select f65, f69
#             from snapshot
#             where f12 = %s and created_at < %s
#             order by created_at desc limit 7"""
#             cur.execute(serial_query, (st, start_time))
#             results = cur.fetchall()
#             list1 = [t[0] for t in reversed(results)]
#             list2 = [t[1] for t in reversed(results)]
#             if is_serial_increment(list1) and is_serial_increment(list2):
#                 result.append(st)
#             print(f'pick_stock_huge length of result {len(result)}')
#         if result:
#             send_message(f'买量持续增长5分钟 {start_time}', ','.join(result))
#     finally:
#         cnx.close()


def print_amount_rate(start_time=None):
    if not start_time:
        start_time = datetime.now()

    suffix = start_time.strftime("%m%d%H%M")
    cnx = None
    try:
        cnx = connect()
        cur = cnx.cursor()
        # cur.execute('drop table st_tmp')
        cur.execute(f"""
        create table st_ss_{suffix} 
        as 
            select * 
        from snapshot_20241023 
        where created_at between %s - INTERVAL 1 MINUTE and %s;
        """, (start_time, start_time))

        # cur.execute(f"""
        # create table large_{suffix}
        # as
        #     select created_at,f12,f14,f3,f10,f64+f70 as f60,f65+f71 as f61,f62,
        #         ROUND((f64 + f70)*100/(f6 + 0.01),2) as f182,
        #         ROUND((f65 + f71)*100/(f6 + 0.01),2) as f183,f184
        # from snapshot_20241023
        # where created_at between %s - INTERVAL 1 MINUTE and %s;
        # """, (start_time, start_time))

        cnx.commit()
    finally:
        cnx.close()


def pool_clear(cnx, cur, start_time):
    cur.execute(f"""
            select f12
            from snapshot a
            where created_at between %s - INTERVAL 1 minute and %s
            and f12 in (select f12 from pool)
            and a.f184 < 5
            ;
            """, (start_time, start_time))

    # 从pool中剔除
    rows = cur.fetchall()
    # f12s = ','.join(map(str, (row[0] for row in rows)))
    for row in rows:
        cur.execute("delete from pool where f12 = %s;", row[0])
    cnx.commit()


def pool_promote(cnx, cur, start_time):
    # 再次拉升1个点
    cur.execute(f"""
            select a.f12
            from snapshot a
            left join pool b
            on a.f12 = b.f12
            where a.created_at between %s - INTERVAL 30 second and %s
            and a.f12 in (select b.f12 from pool b)
            and a.f3 >= b.f3 + 1
            and a.f12 not in (select f12 from alarm)
            ;
            """, (start_time, start_time))

    # 发送消息，记录发送记录，避免重复告警
    rows = cur.fetchall()
    for row in rows:
        sql = f"""INSERT ignore INTO alarm (f12) VALUES (%s)"""
        cur.execute(sql, (row[0]))
    cnx.commit()
    f12s = '\n'.join(map(str, (row[0] for row in rows)))
    if f12s:
        send_message(f'值得关注 {start_time}', f12s)

    # 股票走弱，调低价格
    cur.execute(f"""
            select a.f12, a.f3
            from snapshot a
            left join pool b
            on a.f12 = b.f12
            where a.created_at between %s - INTERVAL 30 second and %s
            and a.f12 in (select b.f12 from pool b)
            and a.f3 < b.f3
            and a.f12 not in (select f12 from alarm)
            ;
            """, (start_time, start_time))

    rows = cur.fetchall()
    for row in rows:
        sql = f"""update pool set f3 = %s where f12 = %s and f3 > %s"""
        cur.execute(sql, (row[1], row[0], row[1]))
    cnx.commit()


def pool_supply(cnx, cur, start_time):
    # 选股策略
    cur.execute(f"""
                select a.f12, a.f3 from snapshot a
                where a.created_at between %s - INTERVAL 30 second and %s
                and a.f3 < 5 
                -- and b.f3 > 8 
    --             and a.f3 > 0
                and a.f64 > 6000000 
--                 and a.f65 > 1000000
                and a.f66 > 6000000 
                -- and f182 > 20 
                -- and f184 > 10
                -- and f60 > 20000000 
                -- and f61 >10000000
                and a.f10 > 5 
                and a.f67 / (a.f68+0.01) > 4
                and a.f67 > 8
                and a.f12 not in (select * from blacklist b)
    --             order by b.f3 desc
                """, (start_time, start_time))

    rows = cur.fetchall()
    for row in rows:
        sql = f"""INSERT ignore INTO pool (f12, f3) VALUES (%s, %s)"""
        cur.execute(sql, (row[0], row[1]))

    cnx.commit()


def do_stock(start_time=None):

    if not start_time:
        start_time = datetime.now()
    print(f"{start_time} start do_stock.")
    cnx = None
    try:
        cnx = connect()
        cur = cnx.cursor()

        # 找出买量下降的票，踢出观察池
        pool_clear(cnx, cur, start_time)

        # 找出pool中，再次上涨1个点的票，确认买入
        pool_promote(cnx, cur, start_time)

        # 补充pool
        pool_supply(cnx, cur, start_time)

    finally:
        cnx.close()


if __name__ == '__main__':
    # from datetime import datetime, timedelta
    import time

    begin = datetime.strptime("2024-10-29 09:30:15", "%Y-%m-%d %H:%M:%S")

    while True:
        print(begin.strftime("%Y-%m-%d %H:%M"))
        # pick_stock_serial(begin)
        # pick_stock_large(begin)
        # pick_stock_huge(begin)
        do_stock(begin)
        # do_stock()
        begin += timedelta(seconds=30)
        time.sleep(0.5)
