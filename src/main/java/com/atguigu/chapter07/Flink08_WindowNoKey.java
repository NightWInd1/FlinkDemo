package com.atguigu.chapter07;

import com.atguigu.chapter05.bean.WaterSensor;
import com.atguigu.utils.MyList;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;

import static com.atguigu.chapter07.Flink07_AggregateWindow.*;

public class Flink08_WindowNoKey {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        Configuration conf = new Configuration();
        conf.setInteger("rest.port",20000);

        DataStreamSource<String> ds = env.socketTextStream("hadoop218", 9999);

        ds
                .map(line->{
                    String[] data = line.split(",");
                    WaterSensor sensor = new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2]));
                    return sensor;
                })
                        .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                                .process(new ProcessAllWindowFunction<WaterSensor, String, TimeWindow>() {
                                    @Override
                                    public void process(Context ctx, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {

                                        List<WaterSensor> list = MyList.toList(elements);

                                        out.collect("窗口:"+list);
                                    }
                                })
                                        .print();

        env.execute();
    }
}
