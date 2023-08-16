package com.bii.data.tool.datadictionary.data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/08/15
 */
public class DataSourceFactory {


    public static Connection connection(AbstractDataSourceParam dataSourceParam)
            throws SQLException, ClassNotFoundException {
        dataSourceParam.getType();
        switch (dataSourceParam.getType()){
            case "mysql":
            default:
               MysqlParam mysqlParam = (MysqlParam) dataSourceParam;
               return getMysqlConnection(mysqlParam);
               
        }
 
    }
    
    
    private static Connection getMysqlConnection(MysqlParam param)
            throws SQLException, ClassNotFoundException {
        Class.forName(param.getDriverClassName());
        return DriverManager
                .getConnection(param.getUrl(), param.getUser(), param.getPassword());
    }
}
