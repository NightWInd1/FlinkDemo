package com.atguigu.chapter07;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

public class Flink01_TumblingWindow {
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
                                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                                        .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                                            @Override
                                            public void process(
                                                                String key,
                                                                Context context,
                                                                Iterable<Tuple2<String, Long>> elements,
                                                                Collector<String> out) throws Exception {

                                                Date start = new Date(context.window().getStart());
                                                Date end = new Date(context.window().getEnd());

                                                out.collect("窗口"+start + " "+end+" "+elements);
                                            }
                                        })
                                                .print();
        env.execute();
    }
}
