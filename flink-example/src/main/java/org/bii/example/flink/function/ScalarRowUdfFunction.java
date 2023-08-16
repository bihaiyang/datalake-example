package org.bii.example.flink.function;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/08/15
 */
public class ScalarRowUdfFunction extends ScalarFunction {
    
    public Row eval(Integer num){
        return Row.of("word", num + "~");
    }
    
}
