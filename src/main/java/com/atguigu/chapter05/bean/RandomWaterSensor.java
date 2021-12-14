package com.atguigu.chapter05.bean;



import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class RandomWaterSensor implements SourceFunction<WaterSensor> {
        private boolean running = true;

    @Override
    public void run(SourceContext<WaterSensor> ctx) throws Exception {
        Random random = new Random();
        while(running){
          ctx.collect(new WaterSensor("sensor_id"+random.nextInt(100),System.currentTimeMillis(),random.nextInt(100)));
          Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
    running = false;
    }
}

