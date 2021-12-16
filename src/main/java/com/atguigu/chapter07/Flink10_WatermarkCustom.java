package com.atguigu.chapter07;

import com.atguigu.chapter05.bean.WaterSensor;
import com.atguigu.utils.MyList;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;

public class Flink10_WatermarkCustom {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port",20000);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.setParallelism(1);

        DataStreamSource<String> ds = env.socketTextStream("hadoop218", 9999);

        ds
                .map(line->{

                    String[] data = line.split(",");
                    return new WaterSensor(data[0],Long.valueOf(data[1]),Integer.valueOf(data[2]));

                })
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy
                                        .forGenerator((WatermarkStrategy<WaterSensor>) context -> new MyCustomMark())
                                        .withTimestampAssigner( (element, recordTimestamp) -> element.getTs())
                        )
                                .keyBy(WaterSensor::getId)
                                        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                                                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                                                    @Override
                                                    public void process(
                                                            String key,
                                                            Context ctx,
                                                            Iterable<WaterSensor> elements,
                                                            Collector<String> out) throws Exception {

                                                        List<WaterSensor> list = MyList.toList(elements);
                                                        out.collect(key+" "+list);
                                                    }
                                                })
                                                        .print();
        env.execute();


    }
    public static class MyCustomMark implements WatermarkGenerator{

        long maxTs = Long.MIN_VALUE + 3000;
        @Override
        public void onEvent(Object event, long eventTimestamp, WatermarkOutput output) {

            maxTs = Math.max(maxTs,eventTimestamp);
            output.emitWatermark(new Watermark(maxTs-3000));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {

        }
    }
}
