package com.flicker.app.function;

import com.alibaba.fastjson.JSONObject;
import com.flicker.app.common.GmallConfig;
import com.flicker.app.utils.DimUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        connection.setAutoCommit(true);
    }

    @Override
    public void invoke(JSONObject value, Context context){
        PreparedStatement preparedStatement = null;

        String sinkTable = value.getString("sinkTable");
        JSONObject after = value.getJSONObject("after");

        // 如果是更新操作，删除掉redis中的缓存数据
        if("update".equals(value.getString("type"))){
            DimUtil.delRedisDimInfo(sinkTable.toUpperCase(), after.getString("id"));
        }

        String upsertSql = genUpsertSql(sinkTable, after);

        try {
            preparedStatement = connection.prepareStatement(upsertSql);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement != null){
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private String genUpsertSql(String sinkTable, JSONObject after) {

        Set<String> fields = after.keySet();
        Collection<Object> values = after.values();

        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" +
                StringUtils.join(fields, ",") + ")" + "values ('" +
                StringUtils.join(values, "','") + "')";
    }
}
