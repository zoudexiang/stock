import requests
import pandas as pd
import os
import time
import random


class EastMoneyStableFetcher:
    def __init__(self, output_dir='./file'):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

        # 【风险规避 1】：建立标准字段字典，防止 f 编号混淆
        self.FIELD_MAP = {
            "f12": "code",  # 股票代码
            "f14": "stock_name",  # 股票名称
            "f17": "price_open",  # 开盘价
            "f2": "price_close",  # 最新价/收盘价
            "f15": "price_highest",  # 最高价
            "f16": "price_lowest",  # 最低价
            "f5": "trade",  # 成交量(手)
            "f6": "trade_amount",  # 成交额(元)
            "f7": "amplitude",  # 振幅(%)
            "f3": "rise",  # 涨跌幅(%)
            "f4": "amount_increase_decrease",  # 涨跌额
            "f8": "turnover_rate"  # 换手率(%)
        }

    def fetch_market_data(self, target_date):
        dt_clean = target_date.replace('-', '')
        file_path = os.path.join(self.output_dir, f'stock_detail_{dt_clean}.csv')

        all_results = []
        current_page = 1
        page_size = 250  # 适当增加单页数量，减少请求总次数

        print(f"🚀 启动稳定版抓取引擎... 日期: {target_date}")

        while True:
            # 【风险规避 2】：fs 参数覆盖沪深京全市场 (m:0 沪, m:1 深, m:0 t:81 京)
            fs_param = "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048"

            params = {
                "pn": current_page,
                "pz": page_size,
                "po": "1",
                "np": "1",
                "ut": "bd1d9ddb040897f3526046f409581454",
                "fltt": "2",
                "invt": "2",
                "fid": "f3",
                "fs": fs_param,
                "fields": ",".join(self.FIELD_MAP.keys())
            }

            try:
                # 增加更严谨的 Headers
                headers = {
                    "Referer": "https://quote.eastmoney.com/",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                }

                resp = requests.get("https://push2.eastmoney.com/api/qt/clist/get", params=params, headers=headers, timeout=15)
                resp.raise_for_status()  # 检查 HTTP 状态码
                data = resp.json()

                stocks = data.get("data", {}).get("diff", [])
                if not stocks:  # 抓取完毕
                    break

                for s in stocks:
                    # 【风险规避 3】：动态解析字段，容错处理缺失值
                    record = {
                        'dt': target_date,
                        'code': str(s.get('f12')).zfill(6),  # 强制补全 6 位
                        'price_open': self._clean_val(s.get('f17')),
                        'price_close': self._clean_val(s.get('f2')),
                        'price_highest': self._clean_val(s.get('f15')),
                        'price_lowest': self._clean_val(s.get('f16')),
                        'trade': self._clean_val(s.get('f5')),
                        'trade_amount': self._clean_val(s.get('f6')),
                        'amplitude': self._clean_val(s.get('f7')),
                        'rise': self._clean_val(s.get('f3')),
                        'amount_increase_decrease': self._clean_val(s.get('f4')),
                        'turnover_rate': self._clean_val(s.get('f8')),
                        'stock_code': str(s.get('f12')).zfill(6),
                        'stock_name': s.get('f14', 'Unknown')
                    }
                    all_results.append(record)

                print(f"📦 已处理批次 {current_page}，累计 {len(all_results)} 条...")
                current_page += 1
                time.sleep(random.uniform(0.8, 1.5))  # 绅士爬取

            except Exception as e:
                print(f"⚠️ 批次 {current_page} 发生错误: {e}")
                break

        # 【风险规避 4】：最终输出字段强校验
        if all_results:
            df = pd.DataFrame(all_results)
            col_order = [
                'dt', 'code', 'price_open', 'price_close', 'price_highest', 'price_lowest',
                'trade', 'trade_amount', 'amplitude', 'rise', 'amount_increase_decrease',
                'turnover_rate', 'stock_code', 'stock_name'
            ]
            # 确保即使接口漏掉字段，DataFrame 也会补全列
            for col in col_order:
                if col not in df.columns:
                    df[col] = 0.0

            df = df[col_order]  # 严格排序
            df.to_csv(file_path, index=False, encoding='utf-8-sig')
            print(f"✅ 抓取圆满完成，文件保存在: {file_path}")
        else:
            print("❌ 未能获取到任何数据，请检查网络或确认是否为开市期间。")

    def _clean_val(self, val):
        """清洗接口返回的 '-' 或 None 等异常值"""
        if val is None or val == "-":
            return 0.0
        try:
            return float(val)
        except:
            return 0.0


if __name__ == '__main__':
    # 2026-01-26 数据获取
    fetcher = EastMoneyStableFetcher()
    fetcher.fetch_market_data('2026-01-21')