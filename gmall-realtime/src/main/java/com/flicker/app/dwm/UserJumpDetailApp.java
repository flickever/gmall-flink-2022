package com.flicker.app.dwm;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.flicker.app.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 使用Flink CEP计算跳出率
 */
public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 生产环境与kafka分区数量一致

        // 读取kafka主题数据
        String groupId = "UserJumpDetailApp";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_user_jump_detail";
        DataStreamSource<String> KafkaDs = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        // 生成Watermark
        SingleOutputStreamOperator<JSONObject> jsonObjDS  = KafkaDs.map(JSON::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObject, long l) {
                                        return jsonObject.getLong("ts");
                                    }
                                })
                );

        // 定义模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                return lastPageId == null || lastPageId.length() <= 0;
            }
        }).next("next").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                return lastPageId == null || lastPageId.length() <= 0;
            }
        }).within(Time.seconds(10));

//  与上面的代码是一样效果
//        Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
//            @Override
//            public boolean filter(JSONObject jsonObject) throws Exception {
//                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
//                return lastPageId == null || lastPageId.length() <= 0;
//            }
//        })
//                .times(2)
//                .consecutive() // 指定严格近邻
//                .within(Time.seconds(10));

        // 模式序列运用到流上
        // CEP是严格近邻的，当水印大于第二条匹配到的数据的时间戳，才会开始输出数据
        // 举例，匹配到的next数据A 时间为10:05 ,此时水印是10:03，匹配到的数据不会输出，只有当水印涨到10:05之后,才会确定next确实是数据A，这时候才会输出数据
        PatternStream<JSONObject> patternStream = CEP.pattern(
                jsonObjDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid")),
                pattern
        );

        // 提取匹配到的超时事件
        OutputTag<JSONObject> timeOutTag = new OutputTag<>("timeOut");
        SingleOutputStreamOperator<JSONObject> selectDs = patternStream.select(timeOutTag, new PatternTimeoutFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject timeout(Map<String, List<JSONObject>> map, long ts) throws Exception {
                return map.get("start").get(0);
            }
        }, new PatternSelectFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                return map.get("start").get(0);
            }
        });

        DataStream<JSONObject> timeOutDS = selectDs.getSideOutput(timeOutTag);

        DataStream<JSONObject> unionDs = selectDs.union(timeOutDS);

        unionDs.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));

        env.execute("UserJumpDetailApp");
    }
}
