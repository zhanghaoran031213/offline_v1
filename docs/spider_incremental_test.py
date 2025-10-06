import requests
import json
import time
import hashlib
import os
from datetime import datetime
from typing import Dict, Optional, List, Any


# 配置常量 - 集中管理配置项，便于修改
class Config:
    FX_API_URL = "https://fx.cmbchina.com/api/v1/fx/rate"
    DATA_DIR = "fx_data"
    CHECK_INTERVAL = 10  # 检查间隔(秒)
    MAX_FILES = 200  # 最大保留文件数
    CHECK_CLEAN_INTERVAL = 50  # 清理检查间隔
    MAJOR_CURRENCIES = ['USD', 'EUR', 'GBP', 'JPY', 'HKD']  # 主要货币对


class FXDataHandler:
    """外汇数据处理类，封装相关操作"""

    def __init__(self):
        self.history_data: Optional[Dict[str, Any]] = None
        self.file_count = 0
        self.ensure_data_directory()

    def ensure_data_directory(self) -> str:
        """确保数据目录存在"""
        if not os.path.exists(Config.DATA_DIR):
            os.makedirs(Config.DATA_DIR)
            print(f"📁 创建数据目录: {Config.DATA_DIR}")
        return Config.DATA_DIR

    def get_fx_data(self) -> Optional[Dict[str, Any]]:
        """获取外汇数据"""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": "https://fx.cmbchina.com/hq",
            "Origin": "https://fx.cmbchina.com",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        }

        try:
            print("🌐 正在获取外汇数据...")
            response = requests.get(Config.FX_API_URL, headers=headers, timeout=10)
            response.raise_for_status()

            data = response.json()

            # 添加时间戳
            current_time = datetime.now()
            formatted_data = {
                "data": data,
                "update_time": current_time.strftime("%Y-%m-%d %H:%M:%S"),
                "timestamp": int(current_time.timestamp())
            }

            data_count = len(data) if isinstance(data, list) else '未知'
            print(f"✅ 获取外汇数据成功 - 数据条数: {data_count}")
            return formatted_data

        except requests.exceptions.RequestException as e:
            print(f"❌ 网络请求失败: {str(e)}")
            return None
        except json.JSONDecodeError as e:
            print(f"❌ JSON解析失败: {str(e)}")
            return None
        except Exception as e:
            print(f"❌ 获取外汇数据失败: {str(e)}")
            return None

    def is_data_updated(self, new_data: Dict[str, Any], old_data: Dict[str, Any]) -> bool:
        """判断数据是否更新"""
        if not old_data:
            return True
        if not new_data:
            return False

        try:
            # 创建数据的副本，移除时间相关字段
            new_copy = new_data.copy()
            old_copy = old_data.copy()

            # 移除会影响比较的字段
            for key in ['update_time', 'timestamp']:
                new_copy.pop(key, None)
                old_copy.pop(key, None)

            # 计算哈希值进行比较
            new_hash = hashlib.md5(json.dumps(new_copy, sort_keys=True).encode()).hexdigest()
            old_hash = hashlib.md5(json.dumps(old_copy, sort_keys=True).encode()).hexdigest()

            return new_hash != old_hash

        except Exception as e:
            print(f"比较数据时出错: {e}")
            return True

    def save_fx_data(self, data: Dict[str, Any]) -> Optional[str]:
        """保存外汇数据"""
        if not data:
            return None

        try:
            data_dir = self.ensure_data_directory()
            current_time = datetime.now()

            # 创建文件名
            date_str = current_time.strftime("%Y%m%d")
            time_str = current_time.strftime("%H%M%S")
            filename = f"fx_data_{date_str}_{time_str}.json"
            filepath = os.path.join(data_dir, filename)

            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            print(f"💾 数据已保存: {filename}")

            # 显示主要货币对信息
            if 'data' in data and isinstance(data['data'], list):
                print("📊 主要货币对汇率:")
                for item in data['data']:
                    if isinstance(item, dict) and item.get('currencyCode') in Config.MAJOR_CURRENCIES:
                        code = item.get('currencyCode', '未知')
                        buy_rate = item.get('buyRate', '未知')
                        sell_rate = item.get('sellRate', '未知')
                        print(f"   {code}: 买入 {buy_rate} | 卖出 {sell_rate}")

            return filepath

        except Exception as e:
            print(f"❌ 保存失败: {str(e)}")
            return None

    def cleanup_old_files(self) -> None:
        """清理旧文件，只保留最新的配置数量文件"""
        try:
            data_dir = self.ensure_data_directory()
            # 获取所有json文件并过滤
            files = [f for f in os.listdir(data_dir) if f.endswith('.json') and f.startswith('fx_data_')]

            if len(files) > Config.MAX_FILES:
                # 获取文件及其修改时间
                files_with_mtime = []
                for f in files:
                    filepath = os.path.join(data_dir, f)
                    mtime = os.path.getmtime(filepath)
                    files_with_mtime.append((mtime, filepath))

                # 按修改时间排序并删除多余文件
                files_with_mtime.sort()
                files_to_delete = files_with_mtime[:len(files) - Config.MAX_FILES]

                for mtime, filepath in files_to_delete:
                    os.remove(filepath)
                    print(f"🗑️  清理旧文件: {os.path.basename(filepath)}")

        except Exception as e:
            print(f"清理文件时出错: {e}")

    def analyze_api_response(self) -> Optional[Dict[str, Any]]:
        """分析API响应结构"""
        print("🔍 分析API响应结构...")
        test_data = self.get_fx_data()

        if test_data and 'data' in test_data:
            data_list = test_data['data']
            if isinstance(data_list, list) and len(data_list) > 0:
                print(f"📋 数据条数: {len(data_list)}")
                print("📝 数据结构样例:")
                sample_item = data_list[0]
                for key, value in sample_item.items():
                    print(f"   {key}: {value}")

                # 统计货币种类
                currencies = set()
                for item in data_list:
                    if isinstance(item, dict) and 'currencyCode' in item:
                        currencies.add(item['currencyCode'])
                print(f"💰 货币种类: {len(currencies)} 种")
                print(f"💰 货币代码: {', '.join(sorted(list(currencies))[:10])}...")

        return test_data

    def run_monitor(self) -> None:
        """运行监控主循环"""
        print(f"💹 开始外汇数据监控（{Config.CHECK_INTERVAL}秒/次）...")
        print(f"数据保存目录: {os.path.abspath(Config.DATA_DIR)}")

        # 初始分析API结构
        self.history_data = self.analyze_api_response()

        if self.history_data:
            self.save_fx_data(self.history_data)
            self.file_count = 1
            print("✅ 初始数据获取成功")
        else:
            print("❌ 初始数据获取失败，退出程序")
            return

        print(f"\n🔄 开始监控循环（每{Config.CHECK_INTERVAL}秒检查一次）...")
        check_count = 0

        while True:
            try:
                check_count += 1
                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                print(f"\n[{current_time}] 第{check_count}次检查...")

                current_data = self.get_fx_data()

                if current_data:
                    if self.is_data_updated(current_data, self.history_data):
                        filepath = self.save_fx_data(current_data)
                        if filepath:
                            self.file_count += 1
                            self.history_data = current_data
                            print(f"🔄 数据已更新 (第{self.file_count}次保存)")
                            print("💡 汇率数据发生变化")
                        else:
                            print("⚠️  数据有变化但保存失败")
                    else:
                        print("⏸️  数据无变化，跳过保存")
                else:
                    print("❌ 获取数据失败")

                # 定期清理文件
                if check_count % Config.CHECK_CLEAN_INTERVAL == 0:
                    self.cleanup_old_files()

                # 等待下次检查
                print(f"⏰ 等待{Config.CHECK_INTERVAL}秒后再次检查...")
                time.sleep(Config.CHECK_INTERVAL)

            except KeyboardInterrupt:
                print("\n\n👋 用户中断程序")
                break
            except Exception as e:
                print(f"❌ 监控循环出错: {e}")
                time.sleep(Config.CHECK_INTERVAL)  # 出错后等待再重试


def main():
    """主函数"""
    handler = FXDataHandler()
    handler.run_monitor()


if __name__ == "__main__":
    main()