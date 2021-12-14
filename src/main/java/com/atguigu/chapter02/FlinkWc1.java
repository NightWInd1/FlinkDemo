package com.atguigu.chapter02;

//需求：用批处理来处理有界流


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class FlinkWc1 {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment exec = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> dataSource = exec.readTextFile("input/words.txt");

        dataSource
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
                .groupBy(0)
                .sum(1)
                .print();

    }
}
