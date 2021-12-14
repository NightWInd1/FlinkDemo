package com.atguigu.chapter05.transform;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink01_MapFunction {
    public static void main(String[] args){
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> dsMap = env.fromElements(1, 4, 5, 6, 7);

        dsMap.map(new RichMapFunction<Integer, Integer>() {

            @Override
            public Integer map(Integer integer) throws Exception {
                return integer * integer;
            }
        })
                .print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
