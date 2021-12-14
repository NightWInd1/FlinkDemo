package com.atguigu.chapter06.request1;

import com.atguigu.chapter06.bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/*public class PVCount {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port",10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> ds = env.readTextFile("input/UserBehavior.csv");

        ds.map(new MapFunction<String, UserBehavior>() {

            @Override
            public UserBehavior map(String s) throws Exception {
                String[] split = s.split(",");

                Long userId = Long.valueOf(split[0]);
                Long itemId = Long.valueOf(split[1]);
                Integer categoryId = Integer.valueOf(split[2]);
                String behavior = split[3];
                Long timestamp = Long.valueOf(split[4]);

                return new UserBehavior(userId,itemId,categoryId,behavior,timestamp);
            }
        })
                .filter(new FilterFunction<UserBehavior>() {
                    @Override
                    public boolean filter(UserBehavior userBehavior) throws Exception {
                        if (userBehavior.getBehavior().equals("pv")){
                            return true;
                        }else {
                            return false;
                        }
                    }
                })
                .map(new MapFunction<UserBehavior, Tuple2<String,Long>>() {

                    @Override
                    public Tuple2<String, Long> map(UserBehavior userBehavior) throws Exception {
                        return Tuple2.of(userBehavior.getBehavior(),1L);
                    }
                })



        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}*/
