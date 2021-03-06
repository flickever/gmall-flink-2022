package com.flicker.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.flicker.app.bean.OrderDetail;
import com.flicker.app.bean.OrderInfo;
import com.flicker.app.bean.OrderWide;
import com.flicker.app.function.DimAsyncFunction;
import com.flicker.app.utils.DimUtil;
import com.flicker.app.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;
// 数据流 web/app -> nginx -> Springboot -> MySQL -> FLink -> Kafka(ods) -> FLink -> Kafka/Phoenix -> Flink(redis) -> Kafka(dwm)
public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取Kafka 主题的数据 并转换为JavaBean对象&提取时间戳生成WaterMark
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";

        // 订单信息表
        SingleOutputStreamOperator<OrderInfo> orderInfoDs = env.addSource(MyKafkaUtil.getKafkaConsumer(orderInfoSourceTopic, groupId))
                .map(line -> {
                    OrderInfo orderInfo = JSON.parseObject(line, OrderInfo.class);

                    String createTime = orderInfo.getCreate_time();
                    String[] createTimeArr = createTime.split(" ");
                    orderInfo.setCreate_date(createTimeArr[0]);
                    orderInfo.setCreate_hour(createTimeArr[0].split(":")[0]);

                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    orderInfo.setCreate_ts(simpleDateFormat.parse(createTime).getTime());

                    return orderInfo;
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                                    @Override
                                    public long extractTimestamp(OrderInfo orderInfo, long l) {
                                        return orderInfo.getCreate_ts();
                                    }
                                })
                );

        // 订单详情表
        SingleOutputStreamOperator<OrderDetail> orderDetailDs = env.addSource(MyKafkaUtil.getKafkaConsumer(orderDetailSourceTopic, groupId))
                .map(line -> {
                    OrderDetail orderDetail = JSON.parseObject(line, OrderDetail.class);

                    String createTime = orderDetail.getCreate_time();
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    orderDetail.setCreate_ts(simpleDateFormat.parse(createTime).getTime());

                    return orderDetail;
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderDetail>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                                    @Override
                                    public long extractTimestamp(OrderDetail orderDetail, long l) {
                                        return orderDetail.getCreate_ts();
                                    }
                                })
                );

        // 双流Join
        SingleOutputStreamOperator<OrderWide> orderWideNoDimDs = orderInfoDs.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailDs.keyBy(OrderDetail::getId))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context context, Collector<OrderWide> collector) throws Exception {
                        collector.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });

        orderWideNoDimDs.print("OrderWideNoDimDs>>>>>");

        // 4. 关联维度信息
        // 4.1 用户信息
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(orderWideNoDimDs,
                new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setUser_gender(jsonObject.getString("GENDER"));

                        String birthday = jsonObject.getString("BIRTHDAY");
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        long currentTs = System.currentTimeMillis();
                        long ts = sdf.parse(birthday).getTime();
                        long age = (currentTs - ts) / (1000 * 60 * 60 * 24 * 365L);
                        orderWide.setUser_age((int) age);
                    }
                },
                60,
                TimeUnit.SECONDS);

        // 4.2 关联地区维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(orderWideWithUserDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        orderWide.setProvince_name(dimInfo.getString("NAME"));
                        orderWide.setProvince_area_code(dimInfo.getString("AREA_CODE"));
                        orderWide.setProvince_iso_code(dimInfo.getString("ISO_CODE"));
                        orderWide.setProvince_3166_2_code(dimInfo.getString("ISO_3166_2"));
                    }
        }, 60, TimeUnit.SECONDS);


        //4.3 关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(
                orderWideWithProvinceDS, new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setSku_name(jsonObject.getString("SKU_NAME"));
                        orderWide.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                        orderWide.setSpu_id(jsonObject.getLong("SPU_ID"));
                        orderWide.setTm_id(jsonObject.getLong("TM_ID"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSku_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //4.4 关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(
                orderWideWithSkuDS, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSpu_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //4.5 关联TM维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
                orderWideWithSpuDS, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //4.6 关联Category维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideWithTmDS, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setCategory3_name(jsonObject.getString("NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }
                }, 60, TimeUnit.SECONDS);

        orderWideWithCategory3DS.print("orderWideWithCategory3DS>>>>>>>>>");

        orderWideWithCategory3DS
                .map(JSONObject::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(orderWideSinkTopic));

        env.execute("OrderWideApp");

    }
}
