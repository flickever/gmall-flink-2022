package com.flicker.app.utils;

import com.alibaba.fastjson.JSONObject;
import com.flicker.app.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.List;

public class DimUtil {

    public static JSONObject getDimInfo(Connection connection, String tableName, String id) throws Exception {

        // 查询前先查redis
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + id;
        String dimInfoJsonStr  = jedis.get(redisKey);

        // 如果查到数据，直接返回，并延长TTL时间
        if(dimInfoJsonStr != null){
            jedis.expire(redisKey, 24 * 60 * 60);
            jedis.close();
            return JSONObject.parseObject(dimInfoJsonStr);
        }

        // 如果没查到数据，从HBase查到后，写入redis
        String sql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + "where id = '" + id + "'";

        List<JSONObject> jsonObjects = JdbcUtil.queryList(connection, sql, JSONObject.class, true);

        JSONObject result = jsonObjects.get(0);
        jedis.set(redisKey, result.toJSONString());
        jedis.expire(redisKey, 24 * 60 * 60);
        jedis.close();

        return result;
    }

    public static void delRedisDimInfo(String tableName, String id){
        String redisKey = "DIM:" + tableName + ":" + id;
        Jedis jedis = RedisUtil.getJedis();
        jedis.del(redisKey);
        jedis.close();
    }
}
