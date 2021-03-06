package com.atguigu.chapter07;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Date;

public class Flink02_SlidingWindow {
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
                                .window(SlidingProcessingTimeWindows.of(Time.seconds(5),Time.seconds(3)))
                                        .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                                            @Override
                                            public void process(
                                                                String key,
                                                                Context context,
                                                                Iterable<Tuple2<String, Long>> elements,
                                                                Collector<String> out) throws Exception {

                                                Date start = new Date(context.window().getStart());
                                                Date end = new Date(context.window().getEnd());

                                                ArrayList<String> list = new ArrayList<>();
                                                for (Tuple2<String, Long> element : elements) {
                                                    list.add(element.f0);
                                                }

                                                out.collect("??????:"+start+" "+end+" "+list);

                                            }
                                        })
                                                .print();
        env.execute();
    }
}
