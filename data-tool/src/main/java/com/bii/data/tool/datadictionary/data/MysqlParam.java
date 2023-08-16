package com.bii.data.tool.datadictionary.data;

import lombok.Data;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/08/15
 */
@Data
public class MysqlParam extends AbstractDataSourceParam{
    
    /**
     * 数据库连接
     */
    private String url;
    /**
     * 用户
     */
    private String user;
    
    /**
     * 驱动类
     */
    private String driverClassName;
    /**
     * 密码
     */
    private String password;
    /**
     * db
     */
    private String schema;
}
