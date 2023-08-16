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
public class Column {
    
    /** 序号 */
    private String tableName;
    /** 字段名 */
    private String columnName;
    /** 字段类型 */
    private String columnType;
    /** 字段类型 */
    private String columnLength;
    /** 主键 */
    private String columnKey;
    /** 唯一 */
    private String columnUnique;
    /** 空值 */
    private String isNullable;
    /** 缺省 */
    private String columnDefault;
    /** 注释 */
    private String columnComment;
}
