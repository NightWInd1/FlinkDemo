package com.atguigu.pra;

import com.atguigu.utils.MyList;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @Author:MingWang
 */
public class Flink04_CountTumblingWindow {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        Configuration conf = new Configuration();
        conf.setInteger("rest.port",20000);

        DataStreamSource<String> ds = env.socketTextStream("hadoop218", 9999);

        ds
                .flatMap(new FlatMapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                        for (String word : s.split(" ")) {
                            collector.collect(Tuple2.of(word,1L));
                        }
                    }
                })
                        .keyBy(t->t.f0)
                                .countWindow(3)
                                        .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, GlobalWindow>() {
                                            @Override
                                            public void process(
                                                    String key,
                                                    Context context,
                                                    Iterable<Tuple2<String, Long>> elements,
                                                    Collector<String> out) throws Exception {

                                                List<Tuple2<String, Long>> list = MyList.toList(elements);

                                                out.collect(list.toString());
                                            }
                                        })
                                                .print();

        env.execute();

    }
}
