package com.flicker.gmallpublishertest.controller;

import com.alibaba.fastjson.JSON;
import com.flicker.gmallpublishertest.service.SugarService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@RestController
public class SugarController {

    @Autowired
    private SugarService sugarService;

    @RequestMapping("/api/sugar/gmv")
    public String getGmv(@RequestParam(value = "date", defaultValue = "0") int date){

        if(date == 0){
            date = getToday();
        }

        HashMap<Object, Object> result = new HashMap<>();
        result.put("date", 0);
        result.put("msg", "");
        result.put("data", sugarService.getGmv(date));

        return JSON.toJSONString(result);
    }

    @RequestMapping("/api/sugar/tm")
    public String getGmvByTm(@RequestParam(value = "date", defaultValue = "0")  int date,
                             @RequestParam(value = "limit", defaultValue = "5")  int limit){

        if(date == 0){
            date = getToday();
        }

        Map gmvByTm = sugarService.getGmvByTm(date, limit);
        Set keySet = gmvByTm.keySet();
        Collection values = gmvByTm.values();

        return "{ " +
                "  \"status\": 0, " +
                "  \"msg\": \"\", " +
                "  \"data\": { " +
                "    \"categories\": [\"" +
                StringUtils.join(keySet,"\",\"") +
                "\"], " +
                "    \"series\": [ " +
                "      { " +
                "        \"name\": \"商品品牌\", " +
                "        \"data\": [" +
                StringUtils.join(values,",") +
                "] " +
                "      } " +
                "    ] " +
                "  } " +
                "}";

    }

    private int getToday(){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String format = sdf.format(System.currentTimeMillis());
        return Integer.parseInt(format);
    }
}
