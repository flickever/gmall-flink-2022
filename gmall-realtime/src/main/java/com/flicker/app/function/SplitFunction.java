package com.flicker.app.function;

import com.flicker.app.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

@FunctionHint(output = @DataTypeHint("Row<word STRING>"))
public class SplitFunction extends TableFunction<Row> {

    public void eval(String str){

        try {
            List<String> strings = KeywordUtil.splitKeyword(str);
            for (String string : strings) {
                collect(Row.of(str));
            }
        } catch (Exception e) {
            e.printStackTrace();
            collect(Row.of(str));
        }
    }
}
