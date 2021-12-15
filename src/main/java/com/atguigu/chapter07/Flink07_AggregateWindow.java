package com.atguigu.chapter07;

import com.atguigu.chapter05.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Flink07_AggregateWindow {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        Configuration conf = new Configuration();
        conf.setInteger("rest.port",20000);

        DataStreamSource<String> ds = env.socketTextStream("hadoop218", 9999);

        ds
                .map(line->{

                    String[] data = line.split(",");
                    return new WaterSensor(data[0],Long.valueOf(data[1]),Integer.valueOf(data[2]));

                })
                .keyBy(WaterSensor::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<WaterSensor, MyAvg, Double>() {
                               //初始化累加器
                               @Override
                               public MyAvg createAccumulator() {
                                   return new MyAvg();
                               }

                               //
                               @Override
                               public MyAvg add(WaterSensor ws, MyAvg acc) {
                                   acc.sum += ws.getVc();
                                   acc.count += 1;
                                   return acc;
                               }

                               //获取结果
                               @Override
                               public Double getResult(MyAvg myAvg) {
                                   return myAvg.avg();
                               }

                               @Override
                               public MyAvg merge(MyAvg myAvg, MyAvg acc1) {
                                   return null;
                               }
                           }
                        , (key, window, input, out) -> {

                            Double avg = input.iterator().next();

                            out.collect(avg+","+key+","+window);

                        }
                )
                .print();

        env.execute();

    }
    public static class MyAvg{
        public int sum = 0;
        public double count = 0;

        public double avg(){
            return sum * 1.0 / count;
        }
    }
}
