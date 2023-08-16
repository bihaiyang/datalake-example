package com.bii.data.tool.datadictionary.data;


import java.sql.SQLException;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/08/15
 */
public class DataCollectorFactory {
    
    
    public static AbstractDataCollector newInstance(AbstractDataSourceParam param)
            throws SQLException, ClassNotFoundException {
        switch (param.getType()){
            case "mysql":
            default:
                MysqlParam mysqlParam = (MysqlParam) param;
                return new MysqlDataCollector(DataSourceFactory.connection(mysqlParam), mysqlParam.getSchema());
        
        }
    }
}
