package com.atguigu.chapter05;

//需求：从本地集合以及单个元素中读取数据

import com.atguigu.chapter05.bean.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class FlinkSouceDemo1 {
    public static void main(String[] args) throws Exception {
        //1.执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Random random = new Random();
        List<WaterSensor> waterSensors = Arrays.asList(
                new WaterSensor("sensor"+random.nextInt(), System.currentTimeMillis(),random.nextInt(100)),
                new WaterSensor("sensor"+random.nextInt(), System.currentTimeMillis(),random.nextInt(100)),
                new WaterSensor("sensor"+random.nextInt(), System.currentTimeMillis(),random.nextInt(100))
                );
        //1.1Source读取数据
        env.fromCollection(waterSensors)
                //2.处理数据
                .print();

        //3.执行任务
        env.execute();
    }
}
