import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author: ZHR
 * @Date: 2025/10/2 16:01
 * @Description:
 **/
public class FlinkHBaseOrderWriter {

    // HBase配置参数
    private static final String HBASE_TABLE = "ods:order_info";
    private static final String COLUMN_FAMILY = "cf";
    private static final String ZK_QUORUM = "cdh01:2181,cdh02:2181,cdh03:2181";  // 替换为实际ZK地址

    public static void main(String[] args) throws Exception {
        // 1. 初始化Flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);  // 与HBase预分区数一致

        // 2. 模拟订单数据流（实际可替换为Kafka/MySQL CDC）
        DataStream<Map<String, String>> orderStream = env.fromElements(
                createOrder("ORD123456", "2025-10-02 12:00:00", "100.00", "SUCCESS"),
                createOrder("ORD123457", "2025-10-02 12:05:00", "200.50", "PENDING")
        );

        // 3. 转换并写入HBase
        orderStream.addSink(new OrderHBaseSink());

        env.execute("Flink Write Order to HBase");
    }

    // 自定义HBase Sink
    public static class OrderHBaseSink extends RichSinkFunction<Map<String, String>> {
        private Connection hbaseConn;
        private Table orderTable;
        private BufferedMutator mutator;  // 批量写入优化

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化HBase连接
            org.apache.hadoop.conf.Configuration hbaseConf = HBaseConfiguration.create();
            hbaseConf.set("hbase.zookeeper.quorum", ZK_QUORUM);
            hbaseConn = ConnectionFactory.createConnection(hbaseConf);

            // 配置批量写入（性能优化）
            BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(HBASE_TABLE));
            params.writeBufferSize(1024 * 1024 * 5);  // 5MB缓冲区
            mutator = hbaseConn.getBufferedMutator(params);
        }

        @Override
        public void invoke(Map<String, String> order) throws Exception {
            // 解析订单数据
            String orderId = order.get("order_id");
            String createTime = order.get("create_time");
            String amount = order.get("amount");
            String status = order.get("status");

            // 生成RowKey
            long timestamp = TimeUtils.parseTime(createTime);  // 转换为13位时间戳
            int hash = Math.abs(orderId.hashCode() % 4);  // 4个分区
            String rowKey = String.format("hash_%02d|%d|%s", hash, timestamp, orderId);

            // 创建Put对象
            Put put = new Put(Bytes.toBytes(rowKey));
            // 写入列数据（列族:列名 -> 值）
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("order_id"), Bytes.toBytes(orderId));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("create_time"), Bytes.toBytes(createTime));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("amount"), Bytes.toBytes(amount));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("status"), Bytes.toBytes(status));

            // 批量写入（累积到缓冲区后自动提交）
            mutator.mutate(put);
        }

        @Override
        public void close() throws Exception {
            // 关闭资源
            if (mutator != null) mutator.close();
            if (hbaseConn != null) hbaseConn.close();
        }
    }

    // 生成模拟订单数据
    private static Map<String, String> createOrder(String orderId, String createTime, String amount, String status) {
        Map<String, String> order = new HashMap<>();
        order.put("order_id", orderId);
        order.put("create_time", createTime);
        order.put("amount", amount);
        order.put("status", status);
        return order;
    }
}

// 时间工具类（转换字符串为时间戳）
class TimeUtils {
    public static long parseTime(String timeStr) {
        // 实际实现需用SimpleDateFormat转换，此处简化
        return System.currentTimeMillis();
    }

}
