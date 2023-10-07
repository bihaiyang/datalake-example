package com.bii.oneid.compute;

import com.bii.oneid.util.MapUtil;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/10/07
 */
public class OneIdGraphSplitCompute {

    public Dataset<Row> run(Dataset<Row> dataframe, Map<String, Integer> keyDict, String delimiter, String colon) {
    
        Map<Integer, String> reverseMap = MapUtil.reverse(keyDict);
    
        JavaRDD<List<String>> idPairsRdd = dataframe.javaRDD().map(new Function<Row, List<String>>() {
            @Override
            public List<String> call(Row row) throws Exception {
                return Arrays.asList(row.getString(0).split(colon));
            }
        });
    
        //  Cluster1,ARRAY(((CL1,LK1,LT1),(CR1,RK1,RT1),SC1),((CL2,LK2,LT2),(CR2,RK2,RT2),SC2),((CL3,LK3,LT3),(CR3,RK3,RT3),SC3)...)
        
    
    }
}
