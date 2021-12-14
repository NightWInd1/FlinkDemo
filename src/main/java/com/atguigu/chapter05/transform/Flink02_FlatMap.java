package com.atguigu.chapter05.transform;

import com.atguigu.chapter05.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

public class Flink02_FlatMap {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port",10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> dss = env.fromElements(1, 4, 5, 6);

        dss.flatMap(new FlatMapFunction<Integer, Integer>() {

            @Override
            public void flatMap(Integer integer, Collector<Integer> collector) throws Exception {
                collector.collect(integer);

                collector.collect(integer*integer);

                collector.collect(integer * integer * integer);
            }
        }).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
