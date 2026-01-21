import requests
import pandas as pd
import os
import time
import random


class EastMoneyStableFetcher:
    def __init__(self, output_dir='./file'):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        self.FIELD_MAP = {
            "f12": "code", "f14": "stock_name", "f17": "price_open",
            "f2": "price_close", "f15": "price_highest", "f16": "price_lowest",
            "f5": "trade", "f6": "trade_amount", "f7": "amplitude",
            "f3": "rise", "f4": "amount_increase_decrease", "f8": "turnover_rate"
        }

    def fetch_market_data(self, target_date):
        dt_clean = target_date.replace('-', '')
        file_path = os.path.join(self.output_dir, f'stock_detail_{dt_clean}.csv')

        all_results = []
        seen_codes = set()  # 用于去重和终止判断
        current_page = 1
        page_size = 100  # 强制设为 100，适配服务器实际限速

        print(f"🚀 启动稳定版抓取引擎... 日期: {target_date}")

        while True:
            fs_param = "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048"
            params = {
                "pn": current_page,
                "pz": page_size,
                "po": "1", "np": "1",
                "ut": "bd1d9ddb040897f3526046f409581454",
                "fltt": "2", "invt": "2", "fid": "f3",
                "fs": fs_param,
                "fields": ",".join(self.FIELD_MAP.keys())
            }

            try:
                headers = {
                    "Referer": "https://quote.eastmoney.com/",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                }

                resp = requests.get("https://push2.eastmoney.com/api/qt/clist/get", params=params, headers=headers,
                                    timeout=15)
                resp.raise_for_status()
                full_data = resp.json()

                # 严谨的 NoneType 检查
                if not full_data or full_data.get("data") is None:
                    print(f"🏁 第 {current_page} 页无数据，抓取结束。")
                    break

                stocks = full_data.get("data", {}).get("diff", [])
                if not stocks:
                    break

                new_count_in_page = 0
                for s in stocks:
                    code = str(s.get('f12')).zfill(6)

                    # 如果代码已经抓过，则跳过
                    if code in seen_codes:
                        continue

                    seen_codes.add(code)
                    new_count_in_page += 1

                    record = {
                        'dt': target_date,
                        'code': code,
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
                        'stock_code': code,
                        'stock_name': s.get('f14', 'Unknown')
                    }
                    all_results.append(record)

                # 如果整页数据都是重复的，说明已经到末尾并开始循环返回了
                if new_count_in_page == 0:
                    print(f"🏁 检测到数据完全重复，说明已抓完全部市场。")
                    break

                print(f"📦 已处理批次 {current_page}，新增 {new_count_in_page} 条，当前总计 {len(all_results)} 条...")
                current_page += 1
                time.sleep(random.uniform(0.3, 0.6))

            except Exception as e:
                print(f"⚠️ 批次 {current_page} 运行异常: {e}")
                break

        # 最终输出字段强校验
        if all_results:
            df = pd.DataFrame(all_results)
            col_order = [
                'dt', 'code', 'price_open', 'price_close', 'price_highest', 'price_lowest',
                'trade', 'trade_amount', 'amplitude', 'rise', 'amount_increase_decrease',
                'turnover_rate', 'stock_code', 'stock_name'
            ]
            for col in col_order:
                if col not in df.columns:
                    df[col] = 0.0

            df[col_order].to_csv(file_path, index=False, encoding='utf-8-sig')
            # 修正了这里：将 all_records 改为 len(all_results)
            print(f"✅ 抓取圆满完成，总计有效数据: {len(all_results)} 条")
            print(f"📂 文件保存在: {file_path}")
        else:
            print("❌ 未能获取到数据。")

    def _clean_val(self, val):
        if val is None or val == "-":
            return 0.0
        try:
            return float(val)
        except:
            return 0.0


if __name__ == '__main__':
    fetcher = EastMoneyStableFetcher()
    # 2026-01-21 今日数据
    fetcher.fetch_market_data('2026-01-21')