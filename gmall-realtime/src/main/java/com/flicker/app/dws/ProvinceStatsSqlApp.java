package com.flicker.app.dws;

import com.flicker.app.bean.ProvinceStats;
import com.flicker.app.utils.ClickHouseUtil;
import com.flicker.app.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

// 商品搜索关键词
public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //使用DDL创建表 提取时间戳生成WaterMark
        String groupId = "province_stats";
        String orderWideTopic = "dwm_order_wide";
        tableEnv.executeSql("CREATE TABLE order_wide ( " +
                "  `province_id` BIGINT, " +
                "  `province_name` STRING, " +
                "  `province_area_code` STRING, " +
                "  `province_iso_code` STRING, " +
                "  `province_3166_2_code` STRING, " +
                "  `order_id` BIGINT, " +
                "  `split_total_amount` DECIMAL, " +
                "  `create_time` STRING, " +
                "  `rt` as TO_TIMESTAMP(create_time), " +
                "  WATERMARK FOR rt AS rt - INTERVAL '1' SECOND ) with(" +
                MyKafkaUtil.getKafkaDDL(orderWideTopic, groupId) + ")");

        // 查询数据  分组、开窗、聚合
        Table table = tableEnv.sqlQuery("select\n" +
                "    data_format(tumble_start(rt, interval '10' second), 'yyyy-MM-dd HH:mm:ss') as stt\n" +
                "    ,data_format(tumble_end(rt, interval '10' second), 'yyyy-MM-dd HH:mm:ss') as edt\n" +
                "    ,province_id\n" +
                "    ,province_name\n" +
                "    ,province_area_code\n" +
                "    ,province_iso_code\n" +
                "    ,province_3166_2_code\n" +
                "    ,count(distinct order_id) as order_count\n" +
                "    ,sum(split_total_amount)  as order_amount\n" +
                "    ,UNIX_TIMESTAMP()*1000    as ts\n" +
                "from \n" +
                "    order_wide\n" +
                "group by\n" +
                "    province_id\n" +
                "    ,province_name\n" +
                "    ,province_area_code\n" +
                "    ,province_iso_code\n" +
                "    ,province_3166_2_code\n" +
                "    ,tumble(rt, interval '10' second)");

        // 表转为流
        DataStream<ProvinceStats> provinceStatsDataStream = tableEnv.toAppendStream(table, ProvinceStats.class);

        // 写入ck
        provinceStatsDataStream.print();
        provinceStatsDataStream.addSink(ClickHouseUtil.getSink("insert into province_stats_210325 values(?,?,?,?,?,?,?,?,?,?)"));

        env.execute("ProvinceStatsSqlApp");

    }
}
