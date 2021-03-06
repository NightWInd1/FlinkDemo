package com.atguigu.backup;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Date;

public class FlinkSlidingWindows {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        conf.setInteger("rest.port",20000);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> dss = env.socketTextStream("hadoop218", 9999);

        dss.flatMap(new FlatMapFunction<String, Tuple2<String,Long>>() {

            @Override
            public void flatMap(String s, Collector<Tuple2<String, Long>> out) throws Exception {

                for (String words : s.split(" ")) {
                    out.collect(Tuple2.of(words,1L));
                }
            }
        })
                .keyBy(t->t.f0)
                        .window(SlidingProcessingTimeWindows.of(Time.seconds(3),Time.seconds(2)))
                                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                                    @Override
                                    public void process(
                                                        String key,
                                                        Context context,
                                                        Iterable<Tuple2<String, Long>> elements,
                                                        Collector<String> out) throws Exception {

                                        Date start = new Date(context.window().getStart());
                                        Date end = new Date(context.window().getEnd());

                                        ArrayList<String> words = new ArrayList<>();

                                        for (Tuple2<String, Long> element : elements) {
                                            words.add(element.f0);
                                        }
                                        out.collect("??????"+start+" "+end+" "+words);
                                    }
                                })
                                        .print();


        env.execute();
    }
}
