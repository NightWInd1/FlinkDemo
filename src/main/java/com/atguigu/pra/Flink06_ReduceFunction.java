package com.atguigu.pra;

import com.atguigu.chapter05.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

public class Flink06_ReduceFunction {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        Configuration conf = new Configuration();
        conf.setInteger("rest.port",20000);

        DataStreamSource<String> ds = env.socketTextStream("hadoop218", 9999);

        ds
                .map(line->{

                    String[] split = line.split(",");
                    return new WaterSensor(split[0],Long.valueOf(split[1]),Integer.valueOf(split[2]));

                })
                .keyBy(WaterSensor::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<WaterSensor>() {
                            @Override
                            public WaterSensor reduce(WaterSensor ws1, WaterSensor ws2) throws Exception {

                                System.out.println("xxxxx");
                                ws1.setVc(ws1.getVc() + ws2.getVc());
                                return ws1;
                            }

                        }
                        , new WindowFunction<WaterSensor, String, String, TimeWindow>() {
                            @Override
                            public void apply(String key, TimeWindow window, Iterable<WaterSensor> input, Collector<String> out) throws Exception {

                                System.out.println("yyyy");

                                WaterSensor sensor = input.iterator().next();

                                out.collect(sensor+ " " +window);
                            }
                        }
                )
                .print();
        env.execute();
    }
}
