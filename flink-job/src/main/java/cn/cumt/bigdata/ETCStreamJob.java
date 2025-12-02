package cn.cumt.bigdata;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * ETC大数据平台 - Flink流处理任务 (Flink 1.10 版)
 * 
 * 功能：
 * 1. 从Kafka消费ETC过车数据
 * 2. 数据预处理和验证
 * 3. 生成HBase RowKey
 * 4. 写入HBase存储
 */
public class ETCStreamJob {
    
    private static final Logger LOG = LoggerFactory.getLogger(ETCStreamJob.class);
    
    public static void main(String[] args) throws Exception {
        
        // ========================================
        // 1. 创建Flink流执行环境
        // ========================================
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 设置并行度（根据Kafka分区数设置）
        env.setParallelism(3);
        
        // 启用checkpoint（容错机制，每5分钟一次）
        env.enableCheckpointing(300000); // 5分钟
        
        LOG.info("========================================");
        LOG.info("ETC Flink流处理任务启动 (Flink 1.10)");
        LOG.info("========================================");
        
        // ========================================
        // 2. 配置Kafka Consumer (旧版API)
        // ========================================
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "node1:9092,node2:9092,node3:9092");
        properties.setProperty("group.id", "flink-etc-consumer-group");
        properties.setProperty("auto.offset.reset", "earliest"); // 从最早消费
        
        // 创建Kafka消费者
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
            "etc-traffic-data",         // Topic名称
            new SimpleStringSchema(),   // 序列化Schema
            properties                  // 配置属性
        );
        
        // ========================================
        // 3. 创建数据流
        // ========================================
        DataStream<String> kafkaStream = env.addSource(kafkaConsumer);
        
        LOG.info("Kafka Consumer配置完成");
        
        // ========================================
        // 4. 数据处理：JSON解析 -> ETCRecord对象
        // ========================================
        DataStream<ETCRecord> etcRecordStream = kafkaStream
                .map(new MapFunction<String, ETCRecord>() {
                    @Override
                    public ETCRecord map(String jsonStr) throws Exception {
                        try {
                            // 解析JSON字符串
                            JSONObject json = JSON.parseObject(jsonStr);
                            
                            // 创建ETCRecord对象
                            ETCRecord record = new ETCRecord();
                            record.setGCXH(json.getString("GCXH"));
                            record.setXZQHMC(json.getString("XZQHMC"));
                            record.setKKMC(json.getString("KKMC"));
                            record.setFXLX(json.getString("FXLX"));
                            record.setGCSJ(json.getString("GCSJ"));
                            record.setHPZL(json.getString("HPZL"));
                            record.setHPHM(json.getString("HPHM"));
                            record.setCLPPXH(json.getString("CLPPXH"));
                            
                            return record;
                        } catch (Exception e) {
                            LOG.error("JSON解析失败: " + jsonStr, e);
                            return null;
                        }
                    }
                })
                // 过滤掉解析失败或无效的数据
                .filter(new FilterFunction<ETCRecord>() {
                    @Override
                    public boolean filter(ETCRecord record) throws Exception {
                        return record != null && record.isValid();
                    }
                });
        
        LOG.info("数据流处理管道创建完成");
        
        // ========================================
        // 5. 写入HBase
        // ========================================
        etcRecordStream.addSink(new HBaseSinkFunction());
        
        LOG.info("HBase Sink已添加");
        
        // ========================================
        // 6. 启动Flink任务
        // ========================================
        LOG.info("========================================");
        LOG.info("开始执行Flink任务...");
        LOG.info("========================================");
        
        env.execute("ETC Big Data Stream Processing Job");
    }
    
    /**
     * HBase写入函数
     */
    public static class HBaseSinkFunction extends RichSinkFunction<ETCRecord> {
        
        private static final Logger LOG = LoggerFactory.getLogger(HBaseSinkFunction.class);
        
        private Connection hbaseConnection;
        private Table table;
        
        // HBase表名
        private static final String TABLE_NAME = "etc_traffic_data";
        // 列族名
        private static final String COLUMN_FAMILY = "info";
        
        // 统计计数器
        private long writeCount = 0;
        private long errorCount = 0;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            
            try {
                // 创建HBase配置
                org.apache.hadoop.conf.Configuration hbaseConfig = HBaseConfiguration.create();
                
                // HBase Zookeeper配置
                hbaseConfig.set("hbase.zookeeper.quorum", "node1,node2,node3");
                hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181");
                
                // 创建HBase连接
                hbaseConnection = ConnectionFactory.createConnection(hbaseConfig);
                
                // 获取表对象
                table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
                
                LOG.info("✅ HBase连接成功, 表: " + TABLE_NAME);
                
            } catch (Exception e) {
                LOG.error("❌ HBase连接失败", e);
                throw e;
            }
        }
        
        @Override
        public void invoke(ETCRecord record, Context context) throws Exception {
             // Flink 1.10 invoke方法签名可能不同，这里使用Context参数
             // 如果Flink 1.10报错，可能需要改为 public void invoke(ETCRecord record)
             doInvoke(record);
        }
        
        // 兼容不同Flink版本的invoke逻辑
        private void doInvoke(ETCRecord record) throws Exception {
             try {
                // 生成RowKey
                String rowKey = record.generateRowKey();
                
                // 创建Put对象
                Put put = new Put(Bytes.toBytes(rowKey));
                
                // 添加列数据
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("GCXH"), Bytes.toBytes(record.getGCXH()));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("XZQHMC"), Bytes.toBytes(record.getXZQHMC()));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("KKMC"), Bytes.toBytes(record.getKKMC()));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("FXLX"), Bytes.toBytes(record.getFXLX()));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("GCSJ"), Bytes.toBytes(record.getGCSJ()));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("HPZL"), Bytes.toBytes(record.getHPZL()));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("HPHM"), Bytes.toBytes(record.getHPHM()));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("CLPPXH"), Bytes.toBytes(record.getCLPPXH()));
                
                // 写入HBase
                table.put(put);
                
                writeCount++;
                
                if (writeCount % 1000 == 0) {
                    LOG.info("[统计] 已写入: " + writeCount + ", 失败: " + errorCount);
                }
                
            } catch (Exception e) {
                errorCount++;
                LOG.error("写入失败: " + record.getGCXH(), e);
            }
        }
        
        @Override
        public void close() throws Exception {
            super.close();
            
            if (table != null) table.close();
            if (hbaseConnection != null) hbaseConnection.close();
            
            LOG.info("HBase连接关闭. 总写入: " + writeCount + ", 失败: " + errorCount);
        }
    }
}
