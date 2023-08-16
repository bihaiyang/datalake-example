package com.bii.data.tool.datadictionary.data;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/08/15
 */
public class MysqlDataCollector extends AbstractDataCollector {
    
    /**
     * 获取指定数据库所有表信息
     */
    private static final String TABLES_SQL = "SELECT TABLE_NAME,TABLE_COMMENT "
            + "FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME ASC";
    
    /**
     * 版本
     */
    private static final String TABLES_COLUMN_SQL = "" +
            "SELECT COL.TABLE_NAME                            AS TABLE_NAME,\n" +
            "       COL.COLUMN_NAME                           AS COLUMN_NAME,\n" +
            "       COL.COLUMN_TYPE                           AS COLUMN_TYPE,\n" +
            "       IF(COL.COLUMN_KEY = 'PRI', 'Y', 'N')      AS COLUMN_KEY,\n" +
            "       IF(CON.CONSTRAINT_NAME IS NULL, 'N', 'Y') AS COLUMN_UNIQUE,\n" +
            "       IF(COL.IS_NULLABLE = 'YES', 'Y', 'N')     AS IS_NULLABLE,\n" +
            "       COL.COLUMN_DEFAULT                        AS COLUMN_DEFAULT,\n" +
            "       COL.COLUMN_COMMENT                        AS COLUMN_COMMENT\n" +
            "FROM information_schema.COLUMNS COL\n" +
            "LEFT JOIN INFORMATION_SCHEMA.STATISTICS STA ON STA.TABLE_SCHEMA = COL.TABLE_SCHEMA AND STA.TABLE_NAME = COL.TABLE_NAME AND STA.COLUMN_NAME = COL.COLUMN_NAME\n"
            +
            "LEFT JOIN information_schema.TABLE_CONSTRAINTS CON ON CON.CONSTRAINT_SCHEMA = STA.TABLE_SCHEMA AND CON.TABLE_NAME = STA.TABLE_NAME AND CON.CONSTRAINT_NAME = STA.INDEX_NAME AND CON.CONSTRAINT_TYPE = 'UNIQUE'\n"
            +
            "WHERE COL.TABLE_SCHEMA = ?\n" +
            "ORDER BY COL.TABLE_NAME ASC,\n" +
            "         CASE COL.COLUMN_KEY WHEN 'PRI' THEN 0 ELSE 1 END ASC,\n" +
            "         COL.COLUMN_NAME ASC";
    
    private String schema;
    
    private Connection connection;
    
    public MysqlDataCollector(Connection connection, String schema) {
        this.connection = connection;
        this.schema = schema;
    }
    
    @Override
    public List<Table> listTables() {
        try (PreparedStatement preparedStatement = connection.prepareStatement(TABLES_SQL)) {
            preparedStatement.setString(1, schema);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                List<Table> tables = new ArrayList<>();
                while (resultSet.next()) {
                    String tableName = resultSet.getString("TABLE_NAME");
                    tables.add(Table.builder()
                            .name(tableName)
                            .comment(resultSet.getString("TABLE_COMMENT"))
                            .build());
                }
                return tables;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public Map<String, List<Column>> listTableColumns(List<String> tableNames) {
        Map<String, List<Column>> tableColumns = new HashMap<>();
        try (PreparedStatement preparedStatement = connection.prepareStatement(TABLES_COLUMN_SQL)) {
            preparedStatement.setString(1, schema);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    String tableName = resultSet.getString("TABLE_NAME");
                    tableColumns.computeIfAbsent(tableName, k -> new ArrayList<>()).add(
                            Column.builder()
                                    .tableName(tableName)
                                    .columnName(resultSet.getString("COLUMN_NAME"))
                                    .columnType(resultSet.getString("COLUMN_TYPE"))
                                    .columnKey(resultSet.getString("COLUMN_KEY"))
                                    .columnUnique("N")
                                    .isNullable(resultSet.getString("IS_NULLABLE"))
                                    .columnDefault(resultSet.getString("COLUMN_DEFAULT"))
                                    .columnComment(resultSet.getString("COLUMN_COMMENT"))
                                    .build());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return tableColumns;
    }
    
    
}
