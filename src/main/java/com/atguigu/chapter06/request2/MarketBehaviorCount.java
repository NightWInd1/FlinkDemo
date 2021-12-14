package com.atguigu.chapter06.request2;

import com.atguigu.chapter06.bean.MarketingUserBehavior;
import com.atguigu.chapter06.datasource.MockRandomData;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MarketBehaviorCount {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port",10002);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<MarketingUserBehavior> dss = env.addSource(new MockRandomData());

        dss.map(new MapFunction<MarketingUserBehavior, Tuple2<String,Long>>() {

            @Override
            public Tuple2<String, Long> map(MarketingUserBehavior marketingUserBehavior) throws Exception {
                return Tuple2.of(marketingUserBehavior.getBehavior(),1L);
            }
        })
                .filter(new FilterFunction<Tuple2<String, Long>>() {
                    @Override
                    public boolean filter(Tuple2<String, Long> tuple2) throws Exception {
                        if (tuple2.f0.equals("download")){
                            return true;
                        }else {
                            return false;
                        }
                    }
                })
                .keyBy(v->v.f0)
                .sum(1)
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
