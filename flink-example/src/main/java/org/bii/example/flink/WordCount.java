package org.bii.example.flink;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author bihaiyang
 * @desc
 * @since 2022/12/15
 */
public class WordCount {
    
    protected static final Logger LOG = LoggerFactory.getLogger(WordCount.class);
    
    
    public static void main(String[] args) throws Exception {
    
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    
        DataSource<String> lineDs = env.readTextFile("/Users/bihaiyang/IdeaProjects/github-workspace/datalake-example/flink-example/src/main/input/words.txt");
    
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOnes = lineDs
                .flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] words = line.split("    ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.LONG));
    
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneUg = wordAndOnes.groupBy(0);
        AggregateOperator<Tuple2<String, Long>> sum =
                wordAndOneUg.sum(1);
    
        sum.print();

    
    
    }
}
