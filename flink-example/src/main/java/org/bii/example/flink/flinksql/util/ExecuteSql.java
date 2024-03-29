package org.bii.example.flink.flinksql.util;

import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ExecuteSql {
    
    private static Logger log = LoggerFactory.getLogger(ExecuteSql.class);
    
    
    public static TableResult exeSQLFromFile(String path, TableEnvironment tEnv){
        List<String> sqls = FileUtils.readLineFromFile(path);
        List<SqlCommandParser> sqlCommandParsers = SqlFileParser.fileToSql(sqls);
        return exeSql(sqlCommandParsers, tEnv);
    }
    
    
    public static TableResult exeSQLFromString(String sql, TableEnvironment tEnv){
        List<SqlCommandParser> sqlCommandCallList = SqlFileParser.fileToSql(sql);
        return exeSql(sqlCommandCallList, tEnv);
    }
    

    public static TableResult exeSql(List<SqlCommandParser> sqlCommandCallList,
            TableEnvironment tEnv) {
        TableResult tableResult = null;
        for (SqlCommandParser sqlCommandCall : sqlCommandCallList) {
            switch (sqlCommandCall.sqlCommand) {
                //配置
                case SET:
                    setSingleConfiguration(tEnv, sqlCommandCall.operands[0],
                            sqlCommandCall.operands[1]);
                    break;
                //insert 语句
                case INSERT_INTO:
                case INSERT_OVERWRITE:
                    LogPrintUtil.logPrint(sqlCommandCall);
                    tableResult = tEnv.executeSql(sqlCommandCall.operands[0]);
                    break;
                //显示语句
                case SELECT:
                case SHOW_CATALOGS:
                case SHOW_DATABASES:
                case SHOW_MODULES:
                case SHOW_TABLES:
                    LogPrintUtil.queryRestPrint(tEnv, sqlCommandCall);
                    break;
                default:
                    LogPrintUtil.logPrint(sqlCommandCall);
                    tableResult = tEnv.executeSql(sqlCommandCall.operands[0]);
                    break;
            }
        }
        return tableResult;
    }


    /**
     * 单个设置Configuration
     */
    public static void setSingleConfiguration(TableEnvironment tEnv, String key, String value) {
        if (StringUtils.isEmpty(key) || StringUtils.isEmpty(value)) {
            return;
        }
        Configuration configuration = tEnv.getConfig().getConfiguration();
        log.info("#############setConfiguration#############\n  key={} value={}", key, value);
        configuration.setString(key, value);
    }
}
