package com.atguigu.chapter05;

import com.atguigu.chapter05.bean.RandomWaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkSourceDemo4 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.addSource(new RandomWaterSensor()).print();

        env.execute();

    }
}
