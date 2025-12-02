# -*- coding: utf-8 -*-
"""
ETC大数据平台 - Kafka生产者
功能：读取清洗后的数据，模拟实时数据流，发送到Kafka集群
要求：每秒发送50条数据到 etc-traffic-data 主题
"""

import json
import time
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime
import sys

class ETCKafkaProducer:
    """ETC数据Kafka生产者"""
    
    def __init__(self, bootstrap_servers, topic_name):
        """
        初始化Kafka生产者
        
        参数:
            bootstrap_servers: Kafka集群地址列表
            topic_name: 目标主题名称
        """
        self.topic_name = topic_name
        self.sent_count = 0
        self.error_count = 0
        
        print(f"\n{'='*80}")
        print(f"ETC Kafka生产者初始化")
        print(f"{'='*80}")
        print(f"Kafka集群: {bootstrap_servers}")
        print(f"目标主题: {topic_name}")
        print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*80}\n")
        
        try:
            # 创建Kafka生产者
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                # 消息序列化：将字典转为JSON字符串，再转为字节
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                # 消息确认机制：all表示所有副本都确认后才返回
                acks='all',
                # 重试次数
                retries=3,
                # 批量发送大小（字节）
                batch_size=16384,
                # 缓冲区大小（字节）
                buffer_memory=33554432,
                # 压缩类型
                compression_type='gzip'
            )
            print("✅ Kafka生产者创建成功！\n")
        except Exception as e:
            print(f"❌ Kafka生产者创建失败: {str(e)}")
            sys.exit(1)
    
    def load_data(self, file_paths):
        """
        加载CSV数据文件
        
        参数:
            file_paths: CSV文件路径列表
            
        返回:
            DataFrame: 合并后的数据
        """
        print(f"{'='*80}")
        print(f"加载数据文件")
        print(f"{'='*80}")
        
        all_data = []
        
        for file_path in file_paths:
            try:
                print(f"正在读取: {file_path}")
                df = pd.read_csv(file_path, encoding='utf-8')
                print(f"  ✅ 成功读取 {len(df):,} 条记录")
                all_data.append(df)
            except Exception as e:
                print(f"  ❌ 读取失败: {str(e)}")
                continue
        
        if not all_data:
            print("❌ 没有成功读取任何数据文件！")
            sys.exit(1)
        
        # 合并所有数据
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # 按时间排序（模拟真实时间序列）
        if 'GCSJ' in combined_df.columns:
            combined_df['GCSJ_parsed'] = pd.to_datetime(combined_df['GCSJ'])
            combined_df = combined_df.sort_values('GCSJ_parsed')
            combined_df = combined_df.drop('GCSJ_parsed', axis=1)
        
        print(f"\n{'='*80}")
        print(f"✅ 数据加载完成")
        print(f"{'='*80}")
        print(f"总记录数: {len(combined_df):,}")
        print(f"数据字段: {', '.join(combined_df.columns)}")
        print(f"{'='*80}\n")
        
        return combined_df
    
    def send_message(self, data_dict):
        """
        发送单条消息到Kafka
        
        参数:
            data_dict: 要发送的数据（字典格式）
        """
        try:
            # 异步发送消息
            future = self.producer.send(self.topic_name, value=data_dict)
            
            # 等待发送结果（同步模式，确保消息送达）
            record_metadata = future.get(timeout=10)
            
            self.sent_count += 1
            
            # 每1000条显示一次进度
            if self.sent_count % 1000 == 0:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                      f"已发送: {self.sent_count:,} 条 | "
                      f"分区: {record_metadata.partition} | "
                      f"偏移量: {record_metadata.offset}")
            
            return True
            
        except KafkaError as e:
            self.error_count += 1
            print(f"❌ 发送失败 [{self.error_count}]: {str(e)}")
            return False
        except Exception as e:
            self.error_count += 1
            print(f"❌ 发送异常 [{self.error_count}]: {str(e)}")
            return False
    
    def produce_data(self, dataframe, rate_per_second=50):
        """
        按指定速率发送数据
        
        参数:
            dataframe: 要发送的数据
            rate_per_second: 每秒发送条数（默认50条）
        """
        print(f"{'='*80}")
        print(f"开始发送数据到Kafka")
        print(f"{'='*80}")
        print(f"发送速率: {rate_per_second} 条/秒")
        print(f"预计耗时: {len(dataframe) / rate_per_second / 60:.1f} 分钟")
        print(f"{'='*80}\n")
        
        # 计算每条消息的发送间隔（秒）
        interval = 1.0 / rate_per_second
        
        start_time = time.time()
        last_report_time = start_time
        
        try:
            for idx, row in dataframe.iterrows():
                # 将DataFrame行转为字典
                data_dict = row.to_dict()
                
                # 转换所有值为字符串（避免JSON序列化问题）
                data_dict = {k: str(v) for k, v in data_dict.items()}
                
                # 发送到Kafka
                self.send_message(data_dict)
                
                # 控制发送速率
                time.sleep(interval)
                
                # 每10秒显示一次统计信息
                current_time = time.time()
                if current_time - last_report_time >= 10:
                    elapsed = current_time - start_time
                    rate = self.sent_count / elapsed if elapsed > 0 else 0
                    progress = (self.sent_count / len(dataframe)) * 100
                    
                    print(f"\n{'='*80}")
                    print(f"📊 运行统计")
                    print(f"{'='*80}")
                    print(f"已发送: {self.sent_count:,} / {len(dataframe):,} 条 ({progress:.2f}%)")
                    print(f"失败数: {self.error_count}")
                    print(f"实际速率: {rate:.2f} 条/秒")
                    print(f"运行时长: {elapsed / 60:.2f} 分钟")
                    print(f"{'='*80}\n")
                    
                    last_report_time = current_time
        
        except KeyboardInterrupt:
            print(f"\n\n⚠️ 用户中断发送！")
        
        finally:
            # 确保所有消息都发送完成
            print(f"\n正在刷新缓冲区，确保消息送达...")
            self.producer.flush()
            
            # 最终统计
            end_time = time.time()
            total_time = end_time - start_time
            actual_rate = self.sent_count / total_time if total_time > 0 else 0
            
            print(f"\n{'='*80}")
            print(f"📊 最终统计")
            print(f"{'='*80}")
            print(f"发送成功: {self.sent_count:,} 条")
            print(f"发送失败: {self.error_count} 条")
            print(f"成功率: {(self.sent_count / (self.sent_count + self.error_count) * 100):.2f}%")
            print(f"总耗时: {total_time / 60:.2f} 分钟")
            print(f"平均速率: {actual_rate:.2f} 条/秒")
            print(f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*80}\n")
    
    def close(self):
        """关闭Kafka生产者"""
        print("正在关闭Kafka生产者...")
        self.producer.close()
        print("✅ Kafka生产者已关闭\n")


def main():
    """主函数"""
    # ========================================
    # 配置参数
    # ========================================
    
    # Kafka集群地址（你的三台虚拟机）
    KAFKA_SERVERS = [
        'node1:9092',
        'node2:9092',
        'node3:9092'
    ]
    
    # Kafka主题名称
    TOPIC_NAME = 'etc-traffic-data'
    
    # 数据文件路径
    DATA_FILES = [
        'data_all/december_data_final.csv',
        'data_all/january_data_final.csv'
    ]
    
    # 发送速率（条/秒）
    SEND_RATE = 50
    
    # ========================================
    # 创建生产者并发送数据
    # ========================================
    
    producer = ETCKafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        topic_name=TOPIC_NAME
    )
    
    try:
        # 加载数据
        data = producer.load_data(DATA_FILES)
        
        # 发送数据
        producer.produce_data(data, rate_per_second=SEND_RATE)
        
    except Exception as e:
        print(f"\n❌ 程序异常: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        # 关闭生产者
        producer.close()


if __name__ == "__main__":
    main()

