package com.flicker.gmallpublishertest.service.impl;

import com.flicker.gmallpublishertest.mapper.ProductStatsMapper;
import com.flicker.gmallpublishertest.service.SugarService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class SugarServiceImpl implements SugarService {

    @Autowired
    private ProductStatsMapper productStatsMapper;

    @Override
    public BigDecimal getGmv(int date) {

        return productStatsMapper.selectGmv(date);

    }

    @Override
    public Map getGmvByTm(int date, int limit) {

        List<Map> maps = productStatsMapper.selectGmvByTm(date, limit);

        //数据库查出的Map格式转换为需要的map格式, 遍历mapList,将数据取出放入result   
        // Map[("tm_name"->"苹果"),(order_amount->279501)] 转为 Map[("苹果"->"279501")]
        HashMap<String, BigDecimal> result = new HashMap<>();

        for (Map map : maps) {
            result.put((String) map.get("tm_name"), (BigDecimal) map.get("order_amount"));
        }
        return result;
    }
}
