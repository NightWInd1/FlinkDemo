package com.atguigu.backup;

//需求：用批处理来处理有界流

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class FlinkWc1 {
    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        //1.1.创建批处理处理环境

        ExecutionEnvironment exec = ExecutionEnvironment.getExecutionEnvironment();

        //1.2.读取有界流数据

        DataSource<String> dataSource = exec.readTextFile("input/words.txt");

        //2.处理数据
        //2.1.将读取进来的数据切分成一个个的单词
        dataSource
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String s, Collector<String> collector) throws Exception {

                        for (String word : s.split(" ")) {

                            collector.collect(word);

                        }

                    }
                })
                //将单词转换为元组，便于wordcount
                .map(new MapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {
                        return Tuple2.of(s,1L);
                    }
                })
                //按照元组的Key进行分组，聚合
                .groupBy(0)
                .sum(1)
                //3.打印
                .print();

    }
}
