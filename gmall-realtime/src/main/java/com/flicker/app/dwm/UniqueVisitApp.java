package com.flicker.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.flicker.app.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

// 得到uv数据，所以对数据去重，只保留一个用户当天第一次访问数据
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取kafka主题数据
        String groupId = "unique_visit_app";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";

        DataStreamSource<String> KafkaDs = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));
        SingleOutputStreamOperator<JSONObject> jsonObjDs = KafkaDs.map(JSON::parseObject);

        KeyedStream<JSONObject, String> keyedStream = jsonObjDs.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));
        // 过滤出当天第一次访问的用户，注意，从昨天一直在线到今天的用户只能算给昨天，不能算给今天
        SingleOutputStreamOperator<JSONObject> uvDs = keyedStream.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> dateState;
            private SimpleDateFormat simpleDateFormat;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("data-state", String.class);
                // 设置状态TTL，方便清理24小时还没新数据的状态，节省空间
                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.hours(24))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                valueStateDescriptor.enableTimeToLive(stateTtlConfig);
                dateState = getRuntimeContext().getState(valueStateDescriptor);
                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {

                // 取出上一跳页面信息，如果不为空就是非Unique用户
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");

                if (lastPageId == null || lastPageId.length() <= 0) {
                    // 更新日期
                    String currDate = simpleDateFormat.format(jsonObject.getLong("ts"));
                    String lastDate = dateState.value();
                    if (!currDate.equals(lastDate)) {
                        dateState.update(currDate);
                        return true;
                    }
                }
                return false;
            }
        });

        uvDs.print();

        uvDs.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));

        env.execute("UniqueVisitApp");
    }
}
