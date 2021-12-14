package com.atguigu.chapter05.transform;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class Flink07_Connect {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port",10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Integer> ds1 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<String> ds2 = env.fromElements("a", "b", "c", "d", "e");

        ConnectedStreams<Integer, String> cs1 = ds1.connect(ds2);

        cs1.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return value + "";
            }

            @Override
            public String map2(String value) throws Exception {
                return value;
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
