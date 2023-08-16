package com.bii.data.tool.datadictionary.data;

import java.util.List;
import java.util.Map;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/08/15
 */
public abstract class AbstractDataCollector {
    
    /**
     * 获取所有的数据表信息
     *
     * @return 返回数据表信息
     */
    public abstract List<Table> listTables();
    
    
    /**
     * 获取所有的数据表信息
     *
     * @param tableNames 包含的表名称
     * @return 返回数据表信息
     */
    public abstract Map<String, List<Column>> listTableColumns(List<String> tableNames);
    
    
}
