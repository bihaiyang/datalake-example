package com.bii.oneid.configuration;

import static com.bii.oneid.configuration.ConfigOptions.key;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/10/04
 */
public class OneIdOptions {
    
    public final static ConfigOption<String> ONEID_KEY_LIST =
            key("oneid.key.list")
            .stringType()
            .noDefaultValue()
            .withDescription("id 列表");
    
    public final static ConfigOption<String> ONEID_DATABASE =
            key("oneid.database")
            .stringType()
            .noDefaultValue()
            .withDescription("数据库");
    
    public final static ConfigOption<String> ONEID_PARTITION =
            key("oneid.partition")
            .stringType()
            .defaultValue("")
            .withDescription("输入表所在分区");
    
    public final static ConfigOption<String> ONEID_LOCATION =
            key("oneid.location")
            .stringType()
            .defaultValue("alluxio://alluxio-master-0.default.svc.cluster.local:19998")
            .withDescription("oneid warehouse location");
    
    public final static ConfigOption<Integer> MAX_ITERATION_TIME =
            key("oneid.graph.component.iteration.max")
                    .intType()
                    .defaultValue(50)
                    .withDescription("最大迭代次数");
    
    public final static ConfigOption<String> GRAPH_COMPONENT_INPUT_TABLE =
            key("oneid.graph.component.src.table")
            .stringType()
            .defaultValue("oneid_relation_input_sd")
            .withDescription("输入表名称");
    
    public final static ConfigOption<String> GRAPH_COMPONENT_OUTPUT_TABLE =
            key("oneid.graph.component.output.table")
            .stringType()
            .defaultValue("oneid_cc_output")
            .withDescription("输出表所在分区");
    
    public final static ConfigOption<String> GRAPH_EXTEND_INPUT_TABLE =
            key("oneid.graph.extend.input.table")
            .stringType()
            .defaultValue("oneid_cc_output_split")
            .withDescription("图拆分后的输入表名称");
    
    public final static ConfigOption<String> GRAPH_EXTEND_OUTPUT_TABLE =
            key("oneid.graph.extend.output.table")
            .stringType()
            .defaultValue("oneid_cc_output")
            .withDescription("输出表名");
    
    public final static ConfigOption<String> GRAPH_EXTEND_ALL_KEY_INPUT_TABLE =
            key("oneid.graph.extend.all.key.table")
            .stringType()
            .defaultValue("oneid_allkey_sd")
            .withDescription("所有key的输入表名称");
    
    public final static ConfigOption<String> GRAPH_SPLIT_INPUT_TABLE =
            key("oneid.graph.split.input.table")
            .stringType()
            .defaultValue("oneid_graph_input_all")
            .withDescription("oneid 图拆分 输入表");
    
    public final static ConfigOption<String> GRAPH_SPLIT_OUTPUT_TABLE =
            key("oneid.graph.output.table")
            .stringType()
            .defaultValue("oneid_cc_output_split")
            .withDescription("oneid 图拆分输出表");
    
    
    
    
}
