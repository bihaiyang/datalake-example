package org.bii.example.flink.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.bii.example.flink.common.datasource.TaxiRideGenerator;
import org.bii.example.flink.common.datatype.TaxiRide;
import org.bii.example.flink.conf.FlinkStreamConf;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/08/09
 */
public class RideCntExample {
    
    public static void main(String[] args) throws Exception {
        
        StreamExecutionEnvironment webUiEnv = FlinkStreamConf.getWebUiEnv();
        
        DataStreamSource<TaxiRide> rides = webUiEnv.addSource(new TaxiRideGenerator());
        
        rides.map((MapFunction<TaxiRide, Tuple2<Long, Long>>) ride -> Tuple2.of(ride.driverId, 1L))
                .returns(Types.TUPLE(Types.LONG, Types.LONG))
                .keyBy((KeySelector<Tuple2<Long, Long>, Long>) map -> map.f0)
                .sum(1)
                .print();
        
        webUiEnv.execute("ride cnt exam");
        
        
    }
}
