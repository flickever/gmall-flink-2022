package com.flicker.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.flicker.app.function.CustomerDeserialization;
import com.flicker.app.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DebeziumSourceFunction<String> sourceFunction  = MySQLSource.<String>builder()
                .databaseList("gmall-flink")
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("000000")
                .deserializer(new CustomerDeserialization())
                .startupOptions(StartupOptions.latest())
                .build();

        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        streamSource.print();
        String sinkTopic = "ods_base_db";
        streamSource.addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));

        env.execute("Flink CDC");
    }
}
