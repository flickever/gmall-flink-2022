package com.flicker.app.utils;

import com.flicker.app.bean.TransientSink;
import com.flicker.app.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickHouseUtil {

    public static <T> SinkFunction<T> getSink(String sql){

        return JdbcSink.sink(sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {

                        Field[] fields = t.getClass().getDeclaredFields();
                        int offset = 0;

                        try {
                            for (int i = 0; i < fields.length; i++) {
                                Field field = fields[i];
                                field.setAccessible(true);
                                Object o = field.get(t);

                                // 部分字段不需要，打赏注解后，这里进行检测，发现有这个注解就跳过写入到ck
                                TransientSink annotation = field.getAnnotation(TransientSink.class);
                                if(annotation != null){
                                    offset++;
                                    continue;
                                }
                                preparedStatement.setObject(i+1-offset, o);
                            }
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                        preparedStatement.execute();
                    }
                },
                new JdbcExecutionOptions
                        .Builder()
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions
                        .JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build()
        );
    }
}
