package com.atguigu.chapter05.sink;

import com.atguigu.chapter05.bean.RandomWaterSensor;
import com.atguigu.chapter05.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.metrics2.sink.FileSink;

public class FlinkSink01_FileSink {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port",10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<WaterSensor> ds = env.addSource(new RandomWaterSensor());

        SingleOutputStreamOperator<String> map = ds.map(WaterSensor::toString);


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
