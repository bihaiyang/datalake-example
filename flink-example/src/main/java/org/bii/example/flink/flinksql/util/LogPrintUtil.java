package org.bii.example.flink.flinksql.util;

import org.apache.flink.table.api.TableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class LogPrintUtil {
    
    private static Logger log = LoggerFactory.getLogger(LogPrintUtil.class);

    /**
     * 打印sqlCommandParser 日志信息
     */
    public static void logPrint(SqlCommandParser sqlCommandParser) {
        if (sqlCommandParser == null) {
            throw new NullPointerException("sqlCommandParser is null");
        }
        System.out.println(
                "\n #############" + sqlCommandParser.sqlCommand.name() + "############# \n"
                        + sqlCommandParser.operands[0]);
        log.info("\n #############{}############# \n {}", sqlCommandParser.sqlCommand.name(),
                sqlCommandParser.operands[0]);
    }

    /**
     * show 语句  select语句结果打印
     */
    public static void queryRestPrint(TableEnvironment tEnv, SqlCommandParser sqlCommandParser) {
        if (sqlCommandParser == null) {
            throw new NullPointerException("sqlCommandParser is null");
        }
        LogPrintUtil.logPrint(sqlCommandParser);
    
        tEnv.executeSql(sqlCommandParser.operands[0]).print();

    }

    public static void info(String obj) {
        System.out.println("\n #############" + obj + "############# \n");
        log.info("\n #############{}############# \n ", obj);
    }
    
}
