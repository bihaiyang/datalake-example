package org.bii.example.flink.function;

import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/08/15
 */
public class AggUdafFunction extends AggregateFunction<Row, AggDomain> {
    
    public void accumulate(AggDomain agg, Integer val){
        if(val < agg.getMax()){
            agg.setMax(val);
        }
    }
    
    @Override
    public AggDomain createAccumulator() {
        return new AggDomain();
    }
    
    @Override
    public Row getValue(AggDomain accumulator) {
        return Row.of(accumulator.getMax());
    }
    
}

class AggDomain{
    
    private Integer max;
    
    public Integer getMax() {
        return max;
    }
    
    public void setMax(Integer max) {
        this.max = max;
    }
}