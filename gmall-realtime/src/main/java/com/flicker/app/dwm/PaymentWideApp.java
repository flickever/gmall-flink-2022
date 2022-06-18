package com.flicker.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.flicker.app.bean.OrderWide;
import com.flicker.app.bean.PaymentInfo;
import com.flicker.app.bean.PaymentWide;
import com.flicker.app.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;

// 订单宽表和订单支付表合并，订单宽表没匹配到的话，就放在state中等待匹配，设置TTL 30分钟
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";

        // 订单宽表
        SingleOutputStreamOperator<OrderWide> orderWideDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId))
                .map(line -> JSON.parseObject(line, OrderWide.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<OrderWide>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                            @Override
                            public long extractTimestamp(OrderWide orderWide, long l) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return sdf.parse(orderWide.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    return l; // 数据进入系统时间
                                }
                            }
                        }));
        // 支付表
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = env.addSource(MyKafkaUtil.getKafkaConsumer(paymentInfoSourceTopic, groupId))
                .map(line -> JSON.parseObject(line, PaymentInfo.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<PaymentInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                            @Override
                            public long extractTimestamp(PaymentInfo paymentInfo, long l) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return sdf.parse(paymentInfo.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    return l; // 数据进入系统时间
                                }
                            }
                        }));

        // 双流Join
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoDS.keyBy(PaymentInfo::getOrder_id)
                .intervalJoin(orderWideDS.keyBy(OrderWide::getOrder_id))
                .between(Time.minutes(-15), Time.seconds(5))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context context, Collector<PaymentWide> collector) throws Exception {
                        collector.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });

        // 写入kafka
        paymentWideDS
                .map(JSONObject::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(paymentWideSinkTopic));

        env.execute("PaymentWideApp");

    }
}
