package com.flicker.gmalllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController // @Controller, @ResponseBody
@Slf4j
public class LoggerController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("test")
    public String test(){
        System.out.println("success");
        return "success";
    }

    @RequestMapping("test2")
    public String test2(@RequestParam("name") String name, @RequestParam(name = "age", defaultValue = "18") int age){
        System.out.println("name: " + name + ", age: " + age);
        return "success";
    }

    /**
     * mock虚拟数据落盘Kafka
     * @param jsonStr
     * @return
     */
    @RequestMapping("applog")
    public String getLog(@RequestParam("param") String jsonStr){

        // 数据落盘
        log.info(jsonStr);
        kafkaTemplate.send("ods_base_log", jsonStr);

        // 数据写入Kafka

        return "success";
    }
}