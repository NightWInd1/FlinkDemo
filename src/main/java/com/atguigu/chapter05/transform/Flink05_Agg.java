package com.atguigu.chapter05.transform;

import com.atguigu.chapter05.bean.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class Flink05_Agg {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1",1607527992000L,50));
        waterSensors.add(new WaterSensor("sensor_1",1607527994000L,20));
        waterSensors.add(new WaterSensor("sensor_1",1607527996000L,50));
        waterSensors.add(new WaterSensor("sensor_2",1607527998000L,10));
        waterSensors.add(new WaterSensor("sensor_2",1607527999000L,30));

        DataStreamSource<WaterSensor> ds = env.fromCollection(waterSensors);

        ds.keyBy( WaterSensor::getId)
                .minBy("vc")
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
