package com.atguigu.chapter06.request2;

import com.atguigu.chapter06.bean.MarketingUserBehavior;
import com.atguigu.chapter06.datasource.MockRandomData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//需求：APP通过每个渠道每种行为的数量；
//即通过小米下载了多少，通过华为商城下载了多少，通过ios store下载了多少…

public class MarketChannelCount {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10005);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<MarketingUserBehavior> ds = env.addSource(new MockRandomData());

        ds.map(new MapFunction<MarketingUserBehavior, Tuple2<String, Long>>() {
                    @Override

                    public Tuple2<String, Long> map(MarketingUserBehavior marketingUserBehavior) throws Exception {
                        return Tuple2.of(marketingUserBehavior.getChannel()+":"+marketingUserBehavior.getBehavior(), 1L);
                    }
                })
                .keyBy(v -> v.f0)
                .sum(1)
                .print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

