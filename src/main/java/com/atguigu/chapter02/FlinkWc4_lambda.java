package com.atguigu.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 拉姆达表达式只能在函数式接口中才能使用
 * 函数式接口：接口中只有一个需要实现的方法，可以通过@Functionalinterface判定
 *Lambda表达式：(参数列表) --> {处理逻辑 重写方法之后里面的逻辑}
 * 如果函数式接口中参数有泛型，那么需要.returns(Types.返回类型)
 *
 */
public class FlinkWc4_lambda {
    public static void main(String[] args) throws Exception {

        //1.创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.1.读取流
        DataStreamSource<String> dss = env.socketTextStream("localhost", 9999);

        //2.处理数据
        dss
                .flatMap((FlatMapFunction<String, String>) (s, collector) -> {

                    for (String word : s.split(" ")) {
                        collector.collect(word);
                    }
                })
                .returns(Types.STRING)
                //2.1将单词转成元组
                .map((MapFunction<String, Tuple2<String, Long>>) s -> Tuple2.of(s,1L))
                .returns(Types.TUPLE(Types.STRING,Types.LONG))
                //2.2Keyselector选Key，进不同的处理通道
                .keyBy(v -> v.f0)
                .sum(1)
                .print();

        env.execute("FlinkWc3");
    }
}
