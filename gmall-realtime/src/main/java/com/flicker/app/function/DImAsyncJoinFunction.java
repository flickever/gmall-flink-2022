package com.flicker.app.function;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

public interface DImAsyncJoinFunction<T> {

    String getKey(T t);

    void join(T t, JSONObject jsonObject) throws ParseException;
}
