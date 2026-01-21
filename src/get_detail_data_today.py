import json
import time
import random
import http.client  # 新增：导入底层http异常类
from threading import Thread

import requests
from datetime import datetime
import pymysql
from utils.mysql import connect as cnx

# 初始化数据库连接（执行函数，得到连接对象）
# cnx = get_mysql_conn()
if not cnx:
    print("数据库连接失败，程序退出")
    exit(1)

# -------------------------- 2. 优化请求配置（突破风控） --------------------------
# 复用会话+完整请求头（模拟真实浏览器）
session = requests.Session()
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Referer': 'https://www.eastmoney.com/',
    'Origin': 'https://www.eastmoney.com',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Cache-Control': 'no-cache',
    'Pragma': 'no-cache',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    # 可选：从浏览器复制真实Cookie（关键！）
    # 'Cookie': '你的东方财富Cookie'
}
session.headers.update(headers)
# 禁用请求连接池（避免连接复用被风控）
session.adapters.DEFAULT_POOLSIZE = 1


def spider_with_retry(page_num, page_size, fs, max_retry=5):
    """
    修复异常捕获 + 强化重试策略
    :param page_num: 页码
    :param page_size: 每页条数（最大100）
    :param fs: 板块筛选参数
    :param max_retry: 最大重试次数
    :return: 接口数据/None
    """
    fid = 'f62'
    po = 1
    np = 1
    fltt = 2

    fields = 'f12,f14,f2,f3,f5,f6,f8,f9,f10,' \
             'f20,f21,f62,f63,f184,' \
             'f64,f65,f66,f67,f68,f69,' \
             'f70,f71,f72,f73,f74,f75,' \
             'f76,f77,f78,f79,f80,f81,' \
             'f82,f83,f84,f85,f86,f87,' \
             'f1,f13'

    # 拼接URL（添加随机参数，避免URL固定被风控）
    random_param = random.randint(100000, 999999)
    url = f'https://push2.eastmoney.com/api/qt/clist/get' \
          f'?pn={page_num}&pz={page_size}&fid={fid}&po={po}&np={np}&fltt={fltt}&fs={fs}' \
          f'&fields={fields}&_={random_param}'

    retry_count = 0
    while retry_count < max_retry:
        try:
            # 随机延时（2-5秒，模拟人工操作）
            time.sleep(random.uniform(2, 5))
            # 发送请求（关闭重定向+强制不使用缓存）
            response = session.get(
                url,
                timeout=20,
                allow_redirects=False,
                headers={'Cache-Control': 'no-cache'}
            )
            # 处理gzip压缩响应
            response.encoding = 'utf-8'
            if response.status_code != 200:
                raise Exception(f"HTTP状态码异常：{response.status_code}")
            # 解析JSON
            datas = json.loads(response.text)
            return datas.get('data', None)
        # -------------------------- 修复异常捕获 --------------------------
        except (requests.exceptions.ConnectionError, http.client.RemoteDisconnected):
            retry_count += 1
            delay = random.uniform(3, 6)
            print(f"第{page_num}页连接断开，{delay}秒后重试第{retry_count}次...")
            time.sleep(delay)
        except requests.exceptions.Timeout:
            retry_count += 1
            delay = random.uniform(3, 6)
            print(f"第{page_num}页请求超时，{delay}秒后重试第{retry_count}次...")
            time.sleep(delay)
        except Exception as e:
            retry_count += 1
            delay = random.uniform(3, 6)
            print(f"第{page_num}页异常：{str(e)[:50]}，{delay}秒后重试第{retry_count}次...")
            time.sleep(delay)

    print(f"第{page_num}页请求失败（已重试{max_retry}次），跳过")
    return None


def store_batch(data_list):
    """批量入库（兼容空数据+异常处理）"""
    if not data_list:
        print("无数据可插入")
        return
    if not cnx:
        print("数据库连接失效，跳过入库")
        return

    with cnx.cursor() as cursor:
        sql = """  
        insert into stock_last_record_di (
            f1, f2, f3, f5, f6, f8, f9, f10, f12, f13, f14, f20, f21, f62, f63, f64, f65, f66, f67, f68, f69, 
            f70, f71, f72, f73, f74, f75, f76, f77, f78, f79, f80, f81, f82, f83, f84, f85, f86, f87, f184
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        # 构造参数（补全缺失字段为None）
        params = []
        field_list = [f.strip() for f in sql.split('VALUES')[0].split('(')[1].split(')')[0].split(',')]
        for item in data_list:
            row = [item.get(f, None) for f in field_list]
            params.append(tuple(row))

        # 分批插入（每300条提交一次，降低压力）
        batch_size = 300
        total_inserted = 0
        for i in range(0, len(params), batch_size):
            batch = params[i:i + batch_size]
            try:
                cursor.executemany(sql, batch)
                cnx.commit()
                total_inserted += len(batch)
                print(f"已插入{total_inserted}/{len(params)}条数据")
            except Exception as e:
                print(f"批量插入失败：{e}")
                cnx.rollback()
    print(f"入库完成，总计插入{total_inserted}条有效数据")


# -------------------------- 主程序 --------------------------
if __name__ == '__main__':
    now = datetime.now()
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    total_data = []

    try:
        # 全A股筛选参数（覆盖主板/创业板/科创板/北交所）
        fs = (
            'm:1+t:2+f:!2,'  # 沪市主板
            'm:0+t:6+f:!2,'  # 深市主板
            'm:0+t:80+f:!2,'  # 创业板
            'm:1+t:23+f:!2,'  # 科创板
            'm:0+t:81+f:!2,'  # 北交所
            'm:0+t:7+f:!2'  # 中小板（兼容）
        )

        page_size = 100  # 接口最大每页100条
        page_num = 1
        total_count = 0

        # 第一步：获取总条数（带重试）
        print("【1/3】正在获取全A股总条数...")
        first_page = spider_with_retry(page_num, page_size, fs)
        if not first_page or 'total' not in first_page:
            print("获取总条数失败，尝试仅拉取沪市主板...")
            fs = 'm:1+t:2+f:!2'  # 降级策略：只拉沪市
            first_page = spider_with_retry(page_num, page_size, fs)
            if not first_page or 'total' not in first_page:
                print("接口请求失败，请检查Cookie/网络后重试")
                exit(1)

        total_count = first_page['total']
        print(f"【成功】全A股总条数：{total_count}")

        # 第二步：分页拉取所有数据
        print("\n【2/3】开始分页拉取数据...")
        max_page = (total_count // page_size) + 2  # 多算2页，避免漏数
        while page_num <= max_page and len(total_data) < total_count:
            print(f"\n----- 拉取第{page_num}/{max_page}页 -----")
            page_data = spider_with_retry(page_num, page_size, fs)
            if page_data and 'diff' in page_data and page_data['diff']:
                batch_data = page_data['diff']
                total_data.extend(batch_data)
                print(f"第{page_num}页拉取成功，累计数据：{len(total_data)}条")
            else:
                print(f"第{page_num}页无数据")

            page_num += 1
            # 每页拉取后延长延时（核心：避开风控）
            time.sleep(random.uniform(3, 7))

        # 第三步：批量入库
        print(f"\n【3/3】开始入库（总计拉取{len(total_data)}条数据）")
        store_batch(total_data)
        print(f"\n===== 程序完成 =====")
        print(f"时间：{now_str}")
        print(f"拉取总条数：{len(total_data)}")
        # print(f"入库总条数：{total_inserted if 'total_inserted' in locals() else 0}")

    except Exception as e:
        print(f"\n程序执行异常：{e}")
        if cnx:
            cnx.rollback()  # 异常时回滚事务
    finally:
        # 关闭资源
        session.close()
        if cnx and not cnx._closed:
            cnx.close()
            print("\n数据库连接已关闭")