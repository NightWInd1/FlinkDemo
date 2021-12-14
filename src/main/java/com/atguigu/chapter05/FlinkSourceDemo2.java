package com.atguigu.chapter05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
//需求：从HDFS上读取文件

public class FlinkSourceDemo2 {
    public static void main(String[] args) throws Exception {

        //1.创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1.1设置并行度为1
        env.setParallelism(1);

        //1.2从文件中读取有界数据
        DataStreamSource<String> dss = env.readTextFile("hdfs://hadoop102:8020/words.txt");

        //2.处理数据
        dss
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String s, Collector<String> collector) throws Exception {

                        for (String word : s.split(" ")) {

                            collector.collect(word);

                        }
                    }
                })
                .map(new MapFunction<String, Tuple2<String,Long>>() {
                            @Override
                            public Tuple2<String, Long> map(String s) throws Exception {
                                return Tuple2.of(s,1L);

                            }

                        })
                        .keyBy(0)
                        .sum(1)
                        .print();

        //3.执行任务
        env.execute();

    }
}
