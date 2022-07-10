package com.flicker.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.flicker.app.bean.VisitorStats;
import com.flicker.app.utils.ClickHouseUtil;
import com.flicker.app.utils.DateTimeUtil;
import com.flicker.app.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String groupId = "visitor_stats_app";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";
        String pageViewSourceTopic = "dwd_page_log";

        DataStreamSource<String> uvDS = env.addSource(MyKafkaUtil.getKafkaConsumer(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> ujDS = env.addSource(MyKafkaUtil.getKafkaConsumer(userJumpDetailSourceTopic, groupId));
        DataStreamSource<String> pvDS = env.addSource(MyKafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId));

        // 每个流处理为相同类型的流
        // 3.1 UV Stream
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUvDS = uvDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");

            return new VisitorStats(
                    "",
                    "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L, 0L, 0L, 0L, 0L,
                    jsonObject.getLong("ts")
            );
        });

        // 3.2 UJ Stream
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUjDS = ujDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");

            return new VisitorStats(
                    "",
                    "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 0L, 0L, 1L, 0L,
                    jsonObject.getLong("ts")
            );
        });

        // 3.3 PV Stream
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithPvDS = pvDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");

            JSONObject page = jsonObject.getJSONObject("page");
            String last_page_id = page.getString("last_page_id");
            long sv = 0L;
            // 页面信息判断是否为刚进入
            if (last_page_id == null || last_page_id.length() <= 0) {
                sv = 1L;
            }

            return new VisitorStats(
                    "",
                    "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 0L, sv, 1L, page.getLong("during_time"),
                    jsonObject.getLong("ts")
            );
        });

        // 多流union
        DataStream<VisitorStats> unionDS = visitorStatsWithUvDS.union(visitorStatsWithUjDS, visitorStatsWithPvDS);

        // 提取WaterMark
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWMDS = unionDS
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                            @Override
                            public long extractTimestamp(VisitorStats visitorStats, long l) {
                                return visitorStats.getTs();
                            }
                        })
                );

        // 维度信息分组 按 地区 + 渠道 + 新老用户 + 版本
        // 粒度尽量细，但不能太细
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStream = visitorStatsWithWMDS
                .keyBy(line -> new Tuple4<>(line.getAr(), line.getCh(), line.getIs_new(), line.getVc()));

        // 开窗聚合 10s滚动
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowedStream =
                keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(11))); // 注意，这里一定要大于11，因为uj的乱序 + CEP监控就已经有了11s

        SingleOutputStreamOperator<VisitorStats> result = windowedStream.reduce(
                new ReduceFunction<VisitorStats>() {
                    @Override
                    public VisitorStats reduce(VisitorStats visitorStats, VisitorStats t1) throws Exception {

                        visitorStats.setUv_ct(visitorStats.getUv_ct() + t1.getUv_ct());
                        visitorStats.setPv_ct(visitorStats.getPv_ct() + t1.getPv_ct());
                        visitorStats.setSv_ct(visitorStats.getSv_ct() + t1.getSv_ct());
                        visitorStats.setUj_ct(visitorStats.getUj_ct() + t1.getUj_ct());
                        visitorStats.setDur_sum(visitorStats.getDur_sum() + t1.getDur_sum());

                        return visitorStats;
                    }
                },
                new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow timeWindow, Iterable<VisitorStats> iterable, Collector<VisitorStats> collector) throws Exception {
                        long start = timeWindow.getStart();
                        long end = timeWindow.getEnd();

                        VisitorStats visitorStats = iterable.iterator().next();

                        String stt = DateTimeUtil.toYMDhms(new Date(start));
                        String edt = DateTimeUtil.toYMDhms(new Date(end));
                        visitorStats.setStt(stt);
                        visitorStats.setEdt(edt);

                        collector.collect(visitorStats);
                    }
                });

        result.print("VisitorStatsApp>>>>>>");

        result.addSink(ClickHouseUtil.getSink("insert into visitor_status values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"));

        env.execute("VisitorStatsApp");
    }
}
