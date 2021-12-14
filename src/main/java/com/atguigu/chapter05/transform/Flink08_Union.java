package com.atguigu.chapter05.transform;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink08_Union {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port",10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Integer> ds1 = env.fromElements(1, 2, 3, 4);
        DataStreamSource<Integer> ds2 = env.fromElements(1, 2, 3, 4,5,6);
        DataStreamSource<Integer> ds3 = env.fromElements(1, 2, 3);
        DataStreamSource<String> ds4 = env.fromElements("a","b","c");

        DataStream<Integer> union = ds1.union(ds2).union(ds3);

        union.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
