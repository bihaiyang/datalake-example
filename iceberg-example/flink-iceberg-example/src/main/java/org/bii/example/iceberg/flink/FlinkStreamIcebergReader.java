package org.bii.example.iceberg.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/05/22
 */
public class FlinkStreamIcebergReader {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        Configuration configuration = new Configuration();
        configuration.set("fs.alluxio.impl", "alluxio.hadoop.FileSystem");
        TableLoader tableLoader = TableLoader.fromHadoopTable(
                "alluxio://alluxio-master-test-0.default.svc.cluster.local:19998/iceberg/datalake/ods_test",
                configuration);
        DataStream<RowData> stream = FlinkSource.forRowData()
                .env(env)
                .tableLoader(tableLoader)
                .streaming(true)
                .build();
    
        stream.print();
        env.execute("test");
    }
}
