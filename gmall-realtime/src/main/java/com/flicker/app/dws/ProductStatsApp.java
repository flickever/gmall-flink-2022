package com.flicker.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.flicker.app.bean.OrderWide;
import com.flicker.app.bean.PaymentWide;
import com.flicker.app.bean.ProductStats;
import com.flicker.app.common.GmallConstant;
import com.flicker.app.function.DimAsyncFunction;
import com.flicker.app.utils.ClickHouseUtil;
import com.flicker.app.utils.DateTimeUtil;
import com.flicker.app.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.text.ParseException;
import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;


// 商品主题,先把多个流union, 之后聚合, 聚合后关联出维度
public class ProductStatsApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall-flink/ck"));
        //env.enableCheckpointing(5000L);
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);


        // 读取数据
        String groupId = "product_stats_app";

        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";
        DataStreamSource<String> pvDS = env.addSource(MyKafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId));
        DataStreamSource<String> favorDS = env.addSource(MyKafkaUtil.getKafkaConsumer(favorInfoSourceTopic, groupId));
        DataStreamSource<String> cartDS = env.addSource(MyKafkaUtil.getKafkaConsumer(cartInfoSourceTopic, groupId));
        DataStreamSource<String> orderDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId));
        DataStreamSource<String> payDS = env.addSource(MyKafkaUtil.getKafkaConsumer(paymentWideSourceTopic, groupId));
        DataStreamSource<String> refundDS = env.addSource(MyKafkaUtil.getKafkaConsumer(refundInfoSourceTopic, groupId));
        DataStreamSource<String> commentDS = env.addSource(MyKafkaUtil.getKafkaConsumer(commentInfoSourceTopic, groupId));

        // 页面日志 -> 商品点击数据 & 曝光数据 -> sku 点击及曝光统计
        SingleOutputStreamOperator<ProductStats> productStatsWithClickAndDisplayDS  = pvDS.flatMap(new FlatMapFunction<String, ProductStats>() {
            @Override
            public void flatMap(String value, Collector<ProductStats> out) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);

                // page信息
                JSONObject page = jsonObject.getJSONObject("page");
                String pageId = page.getString("page_id");
                Long ts = page.getLong("ts");

                // 输出点击数据
                if ("good_detail".equals(pageId) && "sku_id".equals(page.getString("item_type"))) {
                    out.collect(ProductStats.builder()
                            .sku_id(page.getLong("item"))
                            .click_ct(1L)
                            .ts(page.getLong("ts"))
                            .build());
                }

                JSONArray displays = jsonObject.getJSONArray("display");
                if (displays != null && displays.size() > 0) {
                    for (int i = 0; i < displays.size(); i++) {
                        JSONObject display = displays.getJSONObject(i);

                        if ("sku_id".equals(display.getString("item_type"))) {
                            out.collect(ProductStats.builder()
                                    .sku_id(display.getLong("item"))
                                    .display_ct(1L)
                                    .ts(ts)
                                    .build());
                        }
                    }
                }
            }
        });

        // sku收藏统计
        SingleOutputStreamOperator<ProductStats> productStatsWithFavorDS = favorDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .favor_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        // sku 购物车统计
        SingleOutputStreamOperator<ProductStats> productStatsWithCartDS = cartDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .cart_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        // sku订单统计
        SingleOutputStreamOperator<ProductStats> productStatsWithOrderDS  = orderDS.map(line -> {
            OrderWide orderWide = JSON.parseObject(line, OrderWide.class);
            HashSet<Long> orderIdSet = new HashSet<>();
            orderIdSet.add(orderWide.getOrder_id());

            return ProductStats.builder()
                    .sku_id(orderWide.getSku_id())
                    .order_sku_num(orderWide.getSku_num())
                    .order_amount(orderWide.getSplit_total_amount())
                    .orderIdSet(orderIdSet)
                    .ts(DateTimeUtil.toTs(orderWide.getCreate_time()))
                    .build();
        });

        // sku 支付统计
        SingleOutputStreamOperator<ProductStats> productStatsWithPaymentDS = payDS.map(line -> {
            PaymentWide paymentWide = JSON.parseObject(line, PaymentWide.class);

            HashSet<Long> orderIdSet = new HashSet<>();
            orderIdSet.add(paymentWide.getOrder_id());

            return ProductStats.builder()
                    .sku_id(paymentWide.getSku_id())
                    .order_amount(paymentWide.getSplit_total_amount())
                    .paidOrderIdSet(orderIdSet)
                    .ts(DateTimeUtil.toTs(paymentWide.getOrder_create_time()))
                    .build();
        });

        // sku 退单统计: 退单金额、 退单数
        SingleOutputStreamOperator<ProductStats> productStatsWithRefundDS = refundDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);

            HashSet<Long> orderIdSet = new HashSet<>();
            orderIdSet.add(jsonObject.getLong("order_id"));

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                    .refundOrderIdSet(orderIdSet)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        // 评价统计
        SingleOutputStreamOperator<ProductStats> productStatsWithCommentDS  = commentDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            String appraise = jsonObject.getString("appraise");
            long goodCt = 0L;
            if (GmallConstant.APPRAISE_GOOD.equals(appraise)) {
                goodCt = 1L;
            }

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .comment_ct(1L)
                    .good_comment_ct(goodCt)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        // 7流 union
        DataStream<ProductStats> unionDs = productStatsWithClickAndDisplayDS.union(
                productStatsWithFavorDS,
                productStatsWithCartDS,
                productStatsWithOrderDS,
                productStatsWithPaymentDS,
                productStatsWithRefundDS,
                productStatsWithCommentDS
        );

        // 提取WaterMark
        SingleOutputStreamOperator<ProductStats> productStatsWithWMDS  = unionDs.assignTimestampsAndWatermarks(WatermarkStrategy.
                <ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(
                        new SerializableTimestampAssigner<ProductStats>() {
                                           @Override
                                           public long extractTimestamp(ProductStats element, long recordTimestamp) {
                                               return element.getTs();
                                           }
                                       }
                )
        );

        // 按照sku_id分组,10秒的滚动窗口,结合增量聚合(累加值)和全量聚合(提取窗口信息)
        SingleOutputStreamOperator<ProductStats> reduceDs = productStatsWithWMDS.keyBy(ProductStats::getSpu_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<ProductStats>() {
                            @Override
                            public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                                stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                                stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                                stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                                stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                                stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                                stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                                //stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
                                stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());
                                stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                                stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                                //stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);
                                stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                                stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                                //stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);

                                stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                                stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());
                                return stats1;
                            }
                        }
                        , new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                            @Override
                            public void apply(Long aLong, TimeWindow timeWindow, Iterable<ProductStats> iterable, Collector<ProductStats> collector) throws Exception {

                                long startTs = timeWindow.getStart();
                                long endTs = timeWindow.getEnd();
                                ProductStats next = iterable.iterator().next();
                                next.setStt(DateTimeUtil.toYMDhms(new Date(startTs)));
                                next.setEdt(DateTimeUtil.toYMDhms(new Date(endTs)));

                                next.setOrder_ct((long) next.getOrderIdSet().size());
                                next.setPaid_order_ct((long) next.getPaidOrderIdSet().size());
                                next.setRefund_order_ct((long) next.getRefundOrderIdSet().size());

                                collector.collect(next);
                            }
                        });

        // 关联维度 -> SKU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(reduceDs, new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
            @Override
            public String getKey(ProductStats productStats) {
                return productStats.getSpu_id().toString();
            }

            @Override
            public void join(ProductStats productStats, JSONObject jsonObject) throws ParseException {
                productStats.setSku_name(jsonObject.getString("SKU_NAME"));
                productStats.setSku_price(jsonObject.getBigDecimal("PRICE"));
                productStats.setSpu_id(jsonObject.getLong("SPU_ID"));
                productStats.setTm_id(jsonObject.getLong("TM_ID"));
                productStats.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
            }
        }, 60 , TimeUnit.SECONDS);

        //关联维度 -> SPU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS =
                AsyncDataStream.unorderedWait(productStatsWithSkuDS,
                        new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws ParseException {
                                productStats.setSpu_name(jsonObject.getString("SPU_NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getSpu_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        //关联维度 -> Category维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3DS =
                AsyncDataStream.unorderedWait(productStatsWithSpuDS,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws ParseException {
                                productStats.setCategory3_name(jsonObject.getString("NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getCategory3_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        //关联维度 -> TM维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDS =
                AsyncDataStream.unorderedWait(productStatsWithCategory3DS,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws ParseException {
                                productStats.setTm_name(jsonObject.getString("TM_NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getTm_id());
                            }
                        }, 60, TimeUnit.SECONDS);


        //TODO 8.将数据写入ClickHouse
        productStatsWithTmDS.print();
        productStatsWithTmDS.addSink(ClickHouseUtil.getSink("insert into table product_stats_210325 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO 9.启动任务
        env.execute("ProductStatsApp");
    }
}
