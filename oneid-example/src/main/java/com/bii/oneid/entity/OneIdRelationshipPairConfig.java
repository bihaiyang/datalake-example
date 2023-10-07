package com.bii.oneid.entity;

import java.util.ArrayList;
import java.util.List;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/09/25
 */
public class OneIdRelationshipPairConfig {

    /**
     * 表id
     */
    private Long tableId;

    /**
     * 表名称
     */
    private String tableName;

    /**
     * 表类型
     */
    private String tableType;

    /**
     * 数据库
     */
    private String database;

    /**
     * 权重
     */
    private String weight;

    /**
     * id类型 与 字段关系映射
     */
    private List<IdTypeColumnRelMap> idTypeAndColRelMaps = new ArrayList<>();

    public Long getTableId() {
        return tableId;
    }

    public void setTableId(Long tableId) {
        this.tableId = tableId;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableType() {
        return tableType;
    }

    public void setTableType(String tableType) {
        this.tableType = tableType;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getWeight() {
        return weight;
    }

    public void setWeight(String weight) {
        this.weight = weight;
    }

    public List<IdTypeColumnRelMap> getIdTypeAndColRelMaps() {
        return idTypeAndColRelMaps;
    }

    public void setIdTypeAndColRelMaps(
            List<IdTypeColumnRelMap> idTypeAndColRelMaps) {
        this.idTypeAndColRelMaps = idTypeAndColRelMaps;
    }
}
