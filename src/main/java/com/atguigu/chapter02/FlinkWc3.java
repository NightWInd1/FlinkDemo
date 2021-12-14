package com.atguigu.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.security.Key;

public class FlinkWc3 {
    public static void main(String[] args) throws Exception {

        //1.创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.1.读取流
        DataStreamSource<String> dss = env.socketTextStream("hadoop102", 9999);

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

                //2.1将单词转成元组
                .map(new MapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {
                        return Tuple2.of(s,1L);
                    }
                })

                //2.2Keyselector选Key，进不同的处理通道
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> tuple2) throws Exception {
                        return tuple2.f0;
                    }
                })
                .sum(1)
                .print();

        env.execute("FlinkWc3");
    }
}
