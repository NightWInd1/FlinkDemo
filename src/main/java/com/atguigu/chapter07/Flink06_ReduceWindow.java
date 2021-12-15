package com.atguigu.chapter07;

import com.atguigu.chapter05.bean.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Flink06_ReduceWindow {
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
                .reduce( (ws1, ws2) -> {

                    System.out.println("xxxxx");
                    ws1.setVc(ws1.getVc() + ws2.getVc());

                    return ws1;
                }
                        , (key, window, input, out) -> {

                            System.out.println("yyyyy");

                            WaterSensor sensor = input.iterator().next();

                            out.collect(sensor+ " "+window);
                        }
                )
                .print();

        env.execute();

    }
}
