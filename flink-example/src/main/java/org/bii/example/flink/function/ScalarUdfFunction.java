package org.bii.example.flink.function;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author bihaiyang
 * @desc scalar 标量函数 将输入的值转换为一个新的函数
 * @since 2023/08/15
 */
public class ScalarUdfFunction extends ScalarFunction {

     public @DataTypeHint("STRING") String eval(Integer a){
         return "test" + a;
     }
}
