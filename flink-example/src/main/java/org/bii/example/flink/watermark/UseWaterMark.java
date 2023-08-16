package org.bii.example.flink.watermark;

import java.time.Duration;
import java.util.Stack;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.bii.example.flink.common.datasource.TaxiRideGenerator;
import org.bii.example.flink.common.datatype.TaxiRide;
import org.bii.example.flink.conf.FlinkStreamConf;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/08/04
 */
public class UseWaterMark {
    
    
    public static void main(String[] args) throws Exception {
    
        
        SinkFunction<Long> sink = new PrintSinkFunction<>();
        SourceFunction<TaxiRide> source = new TaxiRideGenerator();
        
        StreamExecutionEnvironment env = FlinkStreamConf.getWebUiEnv();
        DataStreamSource<TaxiRide> dataSource = env.addSource(source);
    
        WatermarkStrategy<TaxiRide> watermarkStrategy = WatermarkStrategy
                .<TaxiRide>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((ride, streamRecordTimestamp) -> ride.getEventTimeMillis());
    
        
    
        dataSource.assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(ride -> ride.rideId)
                .process(new AlertFunction())
                .print();
    
       
        env.execute("Long Taxi Rides");
    }
    
    @VisibleForTesting
    public static class AlertFunction extends KeyedProcessFunction<Long, TaxiRide, Long> {
        
        @Override
        public void open(Configuration config) throws Exception {
        
        }
        
        @Override
        public void processElement(TaxiRide ride, Context context, Collector<Long> out)
                throws Exception {}
        
        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<Long> out)
                throws Exception {}
    }
}

