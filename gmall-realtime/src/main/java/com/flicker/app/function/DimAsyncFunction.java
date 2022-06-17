package com.flicker.app.function;

import com.alibaba.fastjson.JSONObject;
import com.flicker.app.common.GmallConfig;
import com.flicker.app.utils.DimUtil;
import com.flicker.app.utils.ThreadPoolUtil;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.text.ParseException;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> {
    private Connection connection;
    private ThreadPoolExecutor threadPoolExecutor;
    private String tableName;

    public DimAsyncFunction(String tableName){
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.CLICKHOUSE_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        threadPoolExecutor = ThreadPoolUtil.getThreadPool();
    }

    public abstract String getKey(T t);

    public abstract void join(T t, JSONObject jsonObject) throws ParseException;

    @Override
    public void asyncInvoke(T t, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.submit(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                // 查询维度信息
                String id = getKey(t);
                JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, id);

                // 补充维度
                if(dimInfo != null){
                    join(t, dimInfo);
                }
            }
        });
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {

    }
}
