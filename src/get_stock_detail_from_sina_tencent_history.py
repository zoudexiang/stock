import requests
import pandas as pd
import os
import time
import random


class StockDataSinaTencentFetcher:
    def __init__(self, output_dir='./file'):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        self.session = requests.Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://finance.sina.com.cn/"
        }

    def get_all_stock_list(self):
        """利用新浪接口获取全市场 A 股代码清单"""
        print("🔍 正在从新浪财经拉取全市场股票清单...")
        all_stocks = []
        # 每页 80 条，抓取 80 页足以覆盖目前所有 A 股
        for page in range(1, 85):
            url = f"http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page={page}&num=80&sort=symbol&asc=1&node=hs_a"
            try:
                resp = self.session.get(url, headers=self.headers, timeout=10)
                data = resp.json()
                if not data: break

                for item in data:
                    # 转换格式: symbol 为 sh600000, code 为 600000
                    all_stocks.append({
                        'full_code': item['symbol'],
                        'code': item['code'],
                        'name': item['name']
                    })

                if page % 10 == 0:
                    print(f"已获取 {len(all_stocks)} 只股票代码...")
                time.sleep(0.1)
            except Exception as e:
                print(f"第 {page} 页获取失败: {e}")
                break
        print(f"✅ 清单拉取完成，共计 {len(all_stocks)} 只股票。")
        return all_stocks

    def fetch_history_data(self, target_date):
        dt_clean = target_date.replace('-', '').replace('/', '')
        # 腾讯接口日期格式要求：2026-01-20 -> 26-01-20 (部分接口) 或 2026-01-20
        dt_dashed = f"{dt_clean[:4]}-{dt_clean[4:6]}-{dt_clean[6:]}"
        file_path = os.path.join(self.output_dir, f'stock_detail_{dt_clean}.csv')

        stocks = self.get_all_stock_list()
        if not stocks: return

        all_records = []
        total = len(stocks)
        print(f"🚀 正在提取 {dt_dashed} 数据 (双重解析模式)...")

        for i, s in enumerate(stocks):
            full_code = s['full_code']
            pure_code = s['code']

            # 使用腾讯更稳健的 K 线接口
            url = f"http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={full_code},day,{dt_dashed},{dt_dashed},1,qfq"

            try:
                resp = self.session.get(url, timeout=5)
                json_data = resp.json()

                # --- 核心修复：腾讯数据的多级查找逻辑 ---
                data_root = json_data.get('data', {}).get(full_code, {})
                # 依次尝试 qfqday (前复权) -> day (普通日线)
                k_line = data_root.get('qfqday')
                if not k_line:
                    k_line = data_root.get('day')

                if k_line and len(k_line) > 0:
                    line = k_line[0]  # 获取指定日期的那一行

                    # 腾讯数据位：0日期, 1开, 2收, 3高, 4低, 5成交量(手)
                    p_open = float(line[1])
                    p_close = float(line[2])
                    p_high = float(line[3])
                    p_low = float(line[4])
                    trade_vol = float(line[5])

                    # 尝试从 line[6] 提取更多指标（成交额、换手率等）
                    extra = line[6] if len(line) > 6 and isinstance(line[6], dict) else {}

                    all_records.append({
                        'dt': dt_dashed,
                        'code': pure_code,
                        'price_open': p_open,
                        'price_close': p_close,
                        'price_highest': p_high,
                        'price_lowest': p_low,
                        'trade': trade_vol,
                        'trade_amount': float(extra.get('amount', 0)) * 10000 if extra.get('amount') else 0.0,
                        # 腾讯成交额单位通常是万
                        'amplitude': float(extra.get('amplitude', 0)),
                        'rise': float(extra.get('zdf', 0)),
                        'amount_increase_decrease': round(p_close - p_open, 2),
                        'turnover_rate': float(extra.get('turnover', 0)),
                        'stock_code': pure_code,
                        'stock_name': s['name']
                    })

                if (i + 1) % 100 == 0:
                    print(f"📊 进度: {i + 1}/{total} | 成功获取: {len(all_records)} 条")

                # 频率控制
                if i % 10 == 0:
                    time.sleep(random.uniform(0.01, 0.03))

            except Exception as e:
                # print(f"解析出错 {pure_code}: {e}") # 调试时可开启
                continue

        # 保存为 CSV
        if all_records:
            df = pd.DataFrame(all_records)
            cols = ['dt', 'code', 'price_open', 'price_close', 'price_highest', 'price_lowest',
                    'trade', 'trade_amount', 'amplitude', 'rise', 'amount_increase_decrease',
                    'turnover_rate', 'stock_code', 'stock_name']
            df[cols].to_csv(file_path, index=False, encoding='utf-8-sig')
            print(f"✨ 最终采集圆满完成！有效数据: {len(all_records)} 条")
        else:
            print(f"❌ 依然未能获取数据。请尝试查询一个更久之前的日期（如 2026-01-15）测试接口稳定性。")


if __name__ == '__main__':
    fetcher = StockDataSinaTencentFetcher()
    # 执行采集 2026-01-20
    fetcher.fetch_history_data('2026-01-20')