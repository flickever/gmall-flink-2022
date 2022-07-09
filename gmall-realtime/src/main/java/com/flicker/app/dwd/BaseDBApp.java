package com.flicker.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.flicker.app.bean.TableProcess;
import com.flicker.app.function.CustomerDeserialization;
import com.flicker.app.function.DimSinkFunction;
import com.flicker.app.function.TableProcessFunction;
import com.flicker.app.utils.MyKafkaUtil;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String sourceTopic = "ods_base_db";
        String groupId = "base_db_app";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        // 根据业务需求，不要delete数据
        SingleOutputStreamOperator<JSONObject> jsonObjStream = kafkaDS.map(JSON::parseObject)
                .filter(line -> !"delete".equals(line.getString("type")));

        // Flink CDC动态得到要监控的表信息，形成广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("gmall-realtime")
                .tableList("gmall-realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new CustomerDeserialization())
                .build();

        DataStreamSource<String> tableProcessDS = env.addSource(sourceFunction);
        // 配置流主键  表名 + 操作类型， 因为表的不同操作类型数据可能会插入不同的表
        // 配置流存储在MapState中
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>
                ("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = tableProcessDS.broadcast(mapStateDescriptor);

        // 连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObjStream.connect(broadcastStream);

        // 分流 处理广播流和主流
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase-tag") {
        };
        SingleOutputStreamOperator<JSONObject> kafka = connectedStream.process(new TableProcessFunction(hbaseTag, mapStateDescriptor));
        DataStream<JSONObject> hbase = kafka.getSideOutput(hbaseTag);

        kafka.print("Kafka>>>>>>>>");
        hbase.print("HBase>>>>>>>>");

        hbase.addSink(new DimSinkFunction());
        kafka.addSink(MyKafkaUtil.getKafkaProducer(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                return new ProducerRecord<byte[], byte[]>(
                        element.getString("sinkTable"),
                        element.getBytes("after")
                );
            }
        }));

        env.execute("BaseDBApp");
    }
}
