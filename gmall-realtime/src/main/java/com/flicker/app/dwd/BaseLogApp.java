package com.flicker.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.flicker.app.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 消费ods_base_log
        String sourceTopic = "ods_base_log";
        String groupId = "base_log_app";
        DataStreamSource<String> KafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        // 接到的数据转JSON对象,脏数据放到侧输出流
        OutputTag<String> dirtyTag = new OutputTag<String>("Dirty"){};
        SingleOutputStreamOperator<JSONObject> jsonDs = KafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (JSONException e) {
                    // 解析不了数据写入侧输出流
                    context.output(dirtyTag, s);
                }
            }
        });

        // 新老用户校验，使用状态编程实现
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS  = jsonDs.keyBy(line -> line.getJSONObject("common").getString("mid"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {

                    private ValueState<String> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", TypeInformation.of(String.class)));
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        String newFlag = jsonObject.getJSONObject("common").getString("isNew");
                        if ("1".equals(newFlag)){
                            String state = valueState.value();

                            if(state != null){
                                jsonObject.getJSONObject("common").put("isNew", "0");
                            }else{
                                valueState.update("1");
                            }
                        }
                        return jsonObject;
                    }
                });

        // 分流，页面数据放主流，启动与曝光数据放侧输出流
        OutputTag<String> startTag = new OutputTag<String>("start"){};
        OutputTag<String> displayTag = new OutputTag<String>("display"){};

        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {
                // 得到start数据
                String startValue = jsonObject.getString("start");
                if (startValue != null && startValue.length() > 0) {
                    context.output(startTag, jsonObject.toJSONString());
                } else {
                    // 只要不是启动日志，就是页面日志，放到主流中，曝光日志也放进去
                    collector.collect(jsonObject.toJSONString());
                    // 曝光日志处理
                    JSONArray displays = jsonObject.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        // 获取页面id，加入曝光日志,并发送到侧输出流
                        String pageId = jsonObject.getJSONObject("page").getString("page_id");
                        for (int i = 0, size = displays.size(); i < size; i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("pageId", pageId);
                            context.output(displayTag, display.toJSONString());
                        }
                    }
                }
            }
        });

        // 提取侧输出流
        DataStream<String> startStream = pageDS.getSideOutput(startTag);
        DataStream<String> displayStream = pageDS.getSideOutput(displayTag);
        DataStream<String> dirtyStream = jsonDs.getSideOutput(dirtyTag);

        // 打印
        startStream.print("start>>>>>>>>");
        displayStream.print("display>>>>>>>>");
        dirtyStream.print("dirty>>>>>>>>");
        pageDS.print("page>>>>>>>>");

        // 写入Kafka
        pageDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_page_log"));
        startStream.addSink(MyKafkaUtil.getKafkaProducer("dwd_start_log"));
        displayStream.addSink(MyKafkaUtil.getKafkaProducer("dwd_display_log"));

        env.execute("BaseLogApp");
    }
}
