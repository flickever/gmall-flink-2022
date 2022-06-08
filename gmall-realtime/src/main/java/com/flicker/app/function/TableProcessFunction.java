package com.flicker.app.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.flicker.app.bean.TableProcess;
import com.flicker.app.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private OutputTag<JSONObject> jsonObjectOutputTag;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private Connection connection;

    public TableProcessFunction(OutputTag<JSONObject> jsonObjectOutputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.jsonObjectOutputTag = jsonObjectOutputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void processBroadcastElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
        // 只要after数据，解析为 TableProcess
        JSONObject jsonObject = JSON.parseObject(s);
        TableProcess tableProcess = jsonObject.getObject("after", TableProcess.class);

        // 建表
        if(TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())){
            checkTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend()
            );
        }

        // 把刚建好的HBase表信息写入状态,广播出去
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getSinkType();
        broadcastState.put(key, tableProcess);
    }


    @Override
    public void processElement(JSONObject jsonObject, ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        String key = jsonObject.getString("tableName") + "-" + jsonObject.getString("type");
        TableProcess tableProcess = broadcastState.get(jsonObject.getString(key));
        if(tableProcess != null){
            JSONObject after = jsonObject.getJSONObject("after");
            filterColumn(after, tableProcess.getSinkColumns());

            if(TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())){
                collector.collect(jsonObject);
            }else if(TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())){
                readOnlyContext.output(this.jsonObjectOutputTag, jsonObject);
            }
        }else {
            System.out.println("该组合Key：" + key + "不存在！");
        }
    }

    // 建表
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        PreparedStatement preparedStatement = null;

        if(sinkPk == null) {
            sinkPk = "id";
        }

        if(sinkExtend == null) {
            sinkExtend = "";
        }

        StringBuffer createTableSql = new StringBuffer("create table if not exists")
                .append(GmallConfig.HBASE_SCHEMA)
                .append(".")
                .append(sinkTable)
                .append("(");

        String[] fields = sinkColumns.split(",");
        for (int i = 0 , size = fields.length; i < size; i++) {
            String field = fields[i];

            if(sinkPk.equals(field)){
                createTableSql.append(field).append("varchar primary key");
            }else{
                createTableSql.append(field).append("varchar");
            }

            // 最后一个字段不加逗号
            if(i < size - 1){
                createTableSql.append(",");
            }
        }
        createTableSql.append(")").append(sinkExtend);

        try {
            preparedStatement = connection.prepareStatement(createTableSql.toString());
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Phoenix表" + sinkTable + "建表失败！");
        } finally {
            if(preparedStatement != null){
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void filterColumn(JSONObject after, String sinkColumns) {
        String[] fields = sinkColumns.split(",");
        List<String> strings = Arrays.asList(fields);

        // 循环删除不需要的列
        after.entrySet().removeIf(next -> !strings.contains(next.getKey()));
    }
}
