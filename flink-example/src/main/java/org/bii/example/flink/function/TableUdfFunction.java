package org.bii.example.flink.function;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/08/15
 */
public class TableUdfFunction extends TableFunction<Row> {
    
    public void eval(@DataTypeHint(inputGroup = InputGroup.ANY) Integer num){
        collect(Row.of("a", num + "~"));
    }
    
}
