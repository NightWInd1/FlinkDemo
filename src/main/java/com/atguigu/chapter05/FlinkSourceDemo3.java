package com.atguigu.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

//需求：从Kafka中流读取数据
public class FlinkSourceDemo3 {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.1设置并行度
        env.setParallelism(1);
        //1.2创建KAFKA的配置文件
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.setProperty("group.id","FlinkSourceDemo3");
        properties.setProperty("auto.offset.reset","latest");
        //2.处理数据
        env.addSource(new FlinkKafkaConsumer<String>(
                "test",
                new SimpleStringSchema(),
                properties
        )).print();
        //3.执行任务
        env.execute();


    }
}
