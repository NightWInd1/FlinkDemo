package com.atguigu.chapter06.datasource;

import com.atguigu.chapter06.bean.MarketingUserBehavior;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class MockRandomData implements SourceFunction<MarketingUserBehavior> {
    private boolean running = true;
    Random  random = new Random();
    List<String> behaviors = Arrays.asList("download","install","update","uninstall");
    List<String> channels = Arrays.asList("huawei", "xiaomi", "apple", "baidu", "tencent", "oppo", "vivo");

    @Override
    public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
        while(running){
            ctx.collect(new MarketingUserBehavior(

                    (long)random.nextInt(5000),
                    behaviors.get(random.nextInt(behaviors.size())),
                    channels.get(random.nextInt(channels.size())),
                    System.currentTimeMillis()));

                    Thread.sleep(1000);

        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
