package org.bii.example.flink.function;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

/**
 * @author bihaiyang
 * @desc 表聚合函数，智能使用table api进行调用 flatAggregate()
 * @since 2023/08/15
 */
public class AggUdtfFunction extends TableAggregateFunction<Tuple2<Integer, Integer>, TopKAccumulator> {
    
    public void accumulate(TopKAccumulator acc, Integer value){
        
        if(value > acc.first){
            acc.second = acc.first;
            acc.first = value;
        }else if(value > acc.second){
            acc.second = value;
        }
    }
    
    public void emitValue(TopKAccumulator acc, Collector<Tuple2<Integer, Integer>> out){
    
        if (acc.first != Integer.MIN_VALUE) {
            out.collect(Tuple2.of(acc.first, 1));
        }
        if (acc.second != Integer.MIN_VALUE) {
            out.collect(Tuple2.of(acc.second, 2));
        }
    }
    
    @Override
    public TopKAccumulator createAccumulator() {
        return new TopKAccumulator();
    }
}


class TopKAccumulator{
    
    public Integer first;
    public Integer second;
    
}
