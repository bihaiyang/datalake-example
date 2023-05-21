package com.bii.example.hudi.flink.driver;

import com.bii.example.hudi.flink.ColumnInfo;
import com.bii.example.hudi.flink.FlinkSqlBuilder;
import com.bii.example.hudi.flink.config.FlinkEnvConfig;
import com.bii.example.hudi.flink.service.HudiOperatorService;
import com.bii.example.hudi.flink.service.sql.SQLHudiOperatorService;
import com.bii.example.hudi.flink.service.sql.SQLOperator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.table.HoodieTableFactory;

/**
 * @fileName: AlluxioHudiQueryDriver.java
 * @description: 通过flink on hudi查询指定alluxio路径数据
 * @author: huangshimin
 * @date: 2021/12/16 8:57 下午
 */
public class AlluxioHudiQueryDriver {
    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hudiPath = parameterTool.get(
                "hudiPath",
                "alluxio://alluxio-master-test-0.default.svc.cluster.local:19998/datalake/ods_test");
        String sourceTableName = parameterTool.get("hudiTableName", "ods_test");
        
        StreamTableEnvironment streamTableEnv = FlinkEnvConfig.getStreamTableEnv();
        Map<String, Object> props = Maps.newHashMap();
        props.put(FactoryUtil.CONNECTOR.key(), HoodieTableFactory.FACTORY_ID);
        props.put(FlinkOptions.PATH.key(), hudiPath);
        props.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.COPY_ON_WRITE.name());
        props.put(FlinkOptions.PRECOMBINE_FIELD.key(), "dt");
        props.put(FlinkOptions.RECORD_KEY_FIELD.key(), "id");
        props.put(FlinkOptions.PARTITION_PATH_FIELD.key(), "dt");
        props.put(FlinkOptions.TABLE_NAME.key(), sourceTableName);
        props.put(FlinkOptions.READ_AS_STREAMING.key(), "true");
        props.put(FlinkOptions.READ_STREAMING_CHECK_INTERVAL.key(), 3);
        // 指定commit开始消费
        props.put(FlinkOptions.READ_START_COMMIT.key(), FlinkOptions.START_COMMIT_EARLIEST);
        String sourceDDL = new FlinkSqlBuilder( props, sourceTableName,
                Lists.newArrayList(
                        ColumnInfo.builder()
                                .columnName("id")
                                .columnType("int").build(),
                        ColumnInfo.builder()
                                .columnName("data")
                                .columnType("string").build(),
                        ColumnInfo.builder()
                                .columnName("category")
                                .columnType("string")
                                .build())).generatorDDL();
        
        HudiOperatorService<StreamTableEnvironment, SQLOperator, Consumer<TableResult>>
                streamHudiOperatorService = new SQLHudiOperatorService<>();
        streamHudiOperatorService.operation(
                streamTableEnv,
                SQLOperator.builder()
                        .querySQLList(Lists.newArrayList("select * from " + sourceTableName))
                        .ddlSQLList(Lists.newArrayList(sourceDDL))
                        .build(),
                tableResult -> tableResult.print());
    }
}
