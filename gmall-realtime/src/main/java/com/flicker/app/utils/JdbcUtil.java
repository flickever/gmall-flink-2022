package com.flicker.app.utils;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {
    // 传入SQL，执行结果转成List<Bean>
    public static <T> List<T> queryList(Connection connection, String sql, Class<T> clz, boolean underScoreToCamel) throws Exception {
        List<T> resultList = new ArrayList<>();

        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();

        // 查询结果meta信息
        ResultSetMetaData metaData = preparedStatement.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (resultSet.next()){

            T t = clz.newInstance();
            // 遍历列名，为每个列赋值
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnName(i);

                // 判断是否要把camel命名风格转成驼峰命名
                if(underScoreToCamel){
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                }

                Object value = resultSet.getObject(i);
                BeanUtils.setProperty(t, columnName, value);

                resultList.add(t);
            }
        }
        preparedStatement.close();
        resultSet.close();

        return resultList;
    }
}
