package com.atguigu.pra;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Date;

/*
    需求：简单了解TumblingWindow的WordCount案例
 */
public class Flink01_TumblingWindow {
    public static void main(String[] args) throws Exception {

        //1.创建Flink流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.设置并行度
        env.setParallelism(1);

        //3.设置Webui端口
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",20000);

        //4.读取流数据
        DataStreamSource<String> ds = env.socketTextStream("hadoop218", 9999);

        //5.Map转换流中的数据为数组
        ds
                .map(new MapFunction<String, Tuple2<String,Long>>() {

                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {
                        String s1 = null;
                        for (String word : s.split(" ")) {
                            s1=word;
                        }
                        return Tuple2.of(s1,1L);
                    }

                })
                //6.KeyBy分流
                .keyBy(t->t.f0)
                //7.调用滚动窗口函数
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

                        ArrayList<String> list = new ArrayList<>();

                        for (Tuple2<String, Long> element : elements) {
                            list.add(element.f0);
                        }

                        out.collect("窗口"+start+" "+end + " "+list);

                    }
                })

                //8.打印结果
                .print();

        //9.执行命令
        env.execute();

    }
}
