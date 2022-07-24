package com.flicker.app.dws;

import com.flicker.app.bean.KeywordStats;
import com.flicker.app.function.SplitFunction;
import com.flicker.app.utils.ClickHouseUtil;
import com.flicker.app.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 使用DDL方式读取Kafka数据创建表
        String groupId = "keyword_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        tableEnv.executeSql("create table page_view( " +
                "    `common` Map<STRING,STRING>, " +
                "    `page` Map<STRING,STRING>, " +
                "    `ts` BIGINT, " +
                "    `rt` as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +
                "    WATERMARK FOR rt AS rt - INTERVAL '1' SECOND " +
                ") with (" + MyKafkaUtil.getKafkaDDL(pageViewSourceTopic, groupId) + ")");

        //  过滤数据  上一跳页面为"search" and 搜索词 is not null
        Table fullWordTable = tableEnv.sqlQuery("select \n" +
                "    page['item'] as full_word\n" +
                "    ,rt\n" +
                "from \n" +
                "    page_view\n" +
                "where \n" +
                "    page['last_page_id']='search'\n" +
                "    and page['item'] is not null");

        // 注册udf
        tableEnv.createTemporaryFunction("split_words", SplitFunction.class);
        Table wordTable  = tableEnv.sqlQuery("select \n" +
                "    word\n" +
                "    ,rt\n" +
                "from \n" +
                fullWordTable + ", lateral table (split_words(full_word))");

        // 开窗聚合
        Table resultTable  = tableEnv.sqlQuery("select \n" +
                "    'search' source,\n" +
                "    ,data_format(tumble_start(rt, interval '10' second), 'yyyy-MM-dd HH:mm:ss') as stt\n" +
                "    ,data_format(tumble_end(rt, interval '10' second), 'yyyy-MM-dd HH:mm:ss')  as edt\n" +
                "    ,word as keyword\n" +
                "    ,count(*) as ct\n" +
                "    ,UNIX_TIMESTAMP() * 1000 as ts\n" +
                "from \n" +
                wordTable + "\n" +
                "group by \n" +
                "    word\n" +
                "    ,tumble(rt, interval '10' second)");

        DataStream<KeywordStats> keywordStatsDataStream = tableEnv.toAppendStream(resultTable, KeywordStats.class);

        keywordStatsDataStream.print();
        keywordStatsDataStream.addSink(ClickHouseUtil.getSink("insert into keyword_stats(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)"));

        env.execute("KeywordStatsApp");
    }
}
