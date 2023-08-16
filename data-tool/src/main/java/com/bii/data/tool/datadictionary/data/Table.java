package com.bii.data.tool.datadictionary.data;

import lombok.Builder;
import lombok.Data;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/08/15
 */
@Data
@Builder
public class Table {

    private String name;
    
    private String comment;
    
    private TableProperties tableProperties;
    
    private String ddl;
    
}

@Data
@Builder
class TableProperties{
    
    private String size;
}