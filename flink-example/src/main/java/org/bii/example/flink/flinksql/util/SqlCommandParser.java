package org.bii.example.flink.flinksql.util;



/**
 * @author zhuchengwen
 * @date 2021.04.22
 */
public class SqlCommandParser {

    public SqlCommand sqlCommand;

    public String[] operands;

    public SqlCommandParser(SqlCommand sqlCommand, String[] operands) {
        this.sqlCommand = sqlCommand;
        this.operands = operands;
    }

    public SqlCommandParser(String[] operands) {
        this.operands = operands;
    }
    
    public SqlCommand getSqlCommand() {
        return sqlCommand;
    }
    
    public String[] getOperands() {
        return operands;
    }
}
