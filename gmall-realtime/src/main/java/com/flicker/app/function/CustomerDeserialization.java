package com.flicker.app.function;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class CustomerDeserialization implements DebeziumDeserializationSchema<String> {
    // 提取db信息中的db、tableName、before数据、after数据、操作类型
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        JSONObject result = new JSONObject();

        // 库名表名
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        String database = fields[1];
        String tableName = fields[2];

        Struct value = (Struct) sourceRecord.value();
        // before 数据
        Struct beforeStruct = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if(beforeStruct != null){
            Schema schema = beforeStruct.schema();
            List<Field> beforeFields = schema.fields();
            for (Field beforeField : beforeFields) {
                Object beforeValue = beforeStruct.get(beforeField);
                beforeJson.put(beforeField.name(), beforeValue);
            }
        }

        // after 数据
        Struct afterStruct = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if(afterStruct != null){
            Schema schema = afterStruct.schema();
            List<Field> afterFields = schema.fields();
            for (Field afterField : afterFields) {
                Object afterValue = afterStruct.get(afterField);
                afterJson.put(afterField.name(), afterValue);
            }
        }

        // 操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)) {
            type = "insert";
        }

        // 返回数据
        result.put("database", database);
        result.put("tableName", tableName);
        result.put("before", beforeJson);
        result.put("after", afterJson);
        result.put("type", type);

        collector.collect(result.toJSONString());

    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
