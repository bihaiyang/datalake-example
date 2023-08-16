package com.bii.data.tool.datadictionary.data;

import lombok.Data;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/08/15
 */
@Data
public abstract class AbstractDataSourceParam {
    
    private String type;
    
    /** 数据字典存储路径,支持相对路径和绝对路径,相对路径相对于data-dictionary.jar所在目录 */
    private String filePath;
}
