package com.bii.oneid.compute;

import com.bii.oneid.util.VertexUtil;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import scala.Tuple3;
import scala.Tuple7;
import scala.Tuple2;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/10/06
 */
public class OneIdConnectedComponent {
    
    
    public Dataset<Row> run(Dataset<Row> df, Map<String, Integer> keyPriority){
        Map<Integer, String> reverseMap = new HashMap<>(keyPriority.size());
        keyPriority.forEach((key, priority) -> {
            reverseMap.put(priority, key);
        });
        Encoder<KeyPriorityAndValueDf> keyPriorityAndValueDfEncoder = Encoders.bean(KeyPriorityAndValueDf.class);
        Dataset<KeyPriorityAndValueDf> keyPriorityAndValueDfDataset = df
                .map((MapFunction<Row, KeyPriorityAndValueDf>) row -> {
                    KeyPriorityAndValueDf keyPriorityAndValueDf = new KeyPriorityAndValueDf();
                    keyPriorityAndValueDf
                            .setLeftPriority(keyPriority.getOrDefault(row.getString(0), 63));
                    keyPriorityAndValueDf.setLeftKeyId(row.get(1).toString());
                    keyPriorityAndValueDf
                            .setRightPriority(keyPriority.getOrDefault(row.get(2).toString(), 63));
                    keyPriorityAndValueDf.setRightKeyId(row.get(3).toString());
                    keyPriorityAndValueDf.setCosineScore(Double.valueOf(row.get(4).toString()));
                    return keyPriorityAndValueDf;
                }, keyPriorityAndValueDfEncoder);
    
        Dataset<Row> inputDAtaFrame = keyPriorityAndValueDfDataset
                .toDF("left_key_type", "left_key_id", "right_key_type", "right_key_id",
                        "cosine_score");
        Dataset<Row> clusterDataset = graphCompute(inputDAtaFrame);
    
        UDF1<Integer, String> keyTypeTransUdf = id -> reverseMap.getOrDefault(id, "unknown");
    
        SparkSession spark = clusterDataset.sparkSession();
        spark.udf().register("key_type_trans", keyTypeTransUdf, DataTypes.StringType);
        clusterDataset.toDF("cluster_id", "key_id", "key_type");
        clusterDataset.createOrReplaceTempView("cluster_data");
        return spark.sql("select cluster_id, key_id, key_type_trans(key_type) from cluster_data");
    }
    
    
    public Dataset<Row> graphCompute(Dataset<Row> inputDataframe){
        SparkSession spark = inputDataframe.sparkSession();
        JavaRDD<Tuple3<Tuple3<String, String, Integer>, Tuple3<String, String, Integer>, Double>> value = inputDataframe.javaRDD().map(row -> {
            return new Tuple3<>(
                    new Tuple3<>(VertexUtil.makeBigIntegerVertices(row.getString(1), row.getInt(0))
                            .toString(),
                            row.getString(1),
                            row.getInt(0)),
                    new Tuple3<>(VertexUtil.makeBigIntegerVertices(row.getString(3), row.getInt(2))
                            .toString(),
                            row.getString(3),
                            row.getInt(2)),
                    row.getDouble(4));
        });
        JavaRDD<Tuple7<String, String, Integer, String, String, Integer, Double>> edgeRdd = value
                .map(v -> {
                    return new Tuple7<>(v._1()._1(), v._1()._2(), v._1()._3(), v._2()._1(),
                            v._2()._2(), v._2()._3(), v._3());
                });
        Dataset<Row> edgeDf = spark.createDataFrame(edgeRdd, Tuple7.class).toDF(
                "left_cluster_id", "left_key", "left_key_type",
                "right_cluster_id", "right_key", "right_key_type", "cosine_score");
    
        JavaRDD<Tuple3<String, String, Integer>> vertexRDD = value.map(tuple3 -> {
            List<Tuple3<String, String, Integer>> list = new ArrayList<>();
            list.add(tuple3._1());
            list.add(tuple3._2());
            return list;
        }).flatMap(
                (FlatMapFunction<List<Tuple3<String, String, Integer>>, Tuple3<String, String, Integer>>)
                        tuple3s -> tuple3s.iterator()).distinct();
    
        Dataset<Row> vertexDf = spark.createDataFrame(vertexRDD, Tuple3.class).toDF("cluster_id", "key_id", "key_type");
        vertexDf.createOrReplaceTempView("vertex");
        Dataset<Row> vertexRn = spark
                .sql("select cluster_id, key_id, key_type, row_number() over(order by cluster_id) rn from vertex ");
        
        vertexRn.createOrReplaceTempView("vertexRn");
        edgeDf.createOrReplaceTempView("edge");
        
        spark.sql("select "
                + "from edge eg"
                + "join vertexRn lvt"
                + "on eg.left_cluster_id = lvt.cluster"
                + "join vertexRn rvt"
                + "on eg.right_cluster_id = rvt.cluster").map(
                        new MapFunction<Row, Edge>() {
    
                            @Override
                            public Object call(Row row) throws Exception {
                                return null;
                            }
                        });
    
        RDD<Tuple2<String, Tuple3<String, String, Integer>>> vertexRdd = vertexRn.javaRDD().map(
                (Function<Row, Tuple2<String, Tuple3<String, String, Integer>>>)
                        row -> new Tuple2<>(row.getString(3),
                                new Tuple3<>(row.getString(0), row.getString(1), row.getInt(2))))
                .rdd();
        
        
    
    
    }
}

class KeyPriorityAndValueDf implements Serializable {
    
    Integer leftPriority;
    
    String leftKeyId;
    
    Integer rightPriority;
    
    String rightKeyId;
    
    Double cosineScore;
    
    public Integer getLeftPriority() {
        return leftPriority;
    }
    
    public void setLeftPriority(Integer leftPriority) {
        this.leftPriority = leftPriority;
    }
    
    public String getLeftKeyId() {
        return leftKeyId;
    }
    
    public void setLeftKeyId(String leftKeyId) {
        this.leftKeyId = leftKeyId;
    }
    
    public Integer getRightPriority() {
        return rightPriority;
    }
    
    public void setRightPriority(Integer rightPriority) {
        this.rightPriority = rightPriority;
    }
    
    public String getRightKeyId() {
        return rightKeyId;
    }
    
    public void setRightKeyId(String rightKeyId) {
        this.rightKeyId = rightKeyId;
    }
    
    public Double getCosineScore() {
        return cosineScore;
    }
    
    public void setCosineScore(Double cosineScore) {
        this.cosineScore = cosineScore;
    }
}
