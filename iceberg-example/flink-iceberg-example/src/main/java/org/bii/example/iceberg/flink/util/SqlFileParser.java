package org.bii.example.iceberg.flink.util;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhuchengwen
 * @date 2021.04.22
 */
public class SqlFileParser {
    
    Logger log = LoggerFactory.getLogger(LogPrintUtil.class);
    
    public static List<SqlCommandParser> sqlListParse(List<String> sqlList) {
        List<SqlCommandParser> sqlCommandCallList = new ArrayList<>();
        for(String sql : sqlList){
            Optional<SqlCommandParser> optionalCall = parse(sql);
            if (optionalCall.isPresent()) {
                sqlCommandCallList.add(optionalCall.get());
            } else {
                throw new RuntimeException("不支持该语法使用" + sql.toString() + "'");
            }
        }
        return sqlCommandCallList;
    }
    
    public static List<SqlCommandParser> fileToSql(String lines) {
        String[] split = lines.split(SystemConstant.LINE_FEED);
        List<String> lineList = Arrays.asList(split);
        return fileToSql(lineList);
    }
    
    public static List<SqlCommandParser> fileToSql(List<String> lineList) {
        
        if (CollectionUtils.isEmpty(lineList)) {
            throw new RuntimeException("lineList is null");
        }
        
        List<SqlCommandParser> sqlCommandCallList = new ArrayList<>();
        
        StringBuilder stmt = new StringBuilder();
        
        for (String line : lineList) {
            //开头是 -- 的表示注释
            if (line.trim().isEmpty() || line.startsWith(SystemConstant.COMMENT_SYMBOL) ||
                    trimStart(line).startsWith(SystemConstant.COMMENT_SYMBOL)) {
                continue;
            }
            stmt.append(SystemConstant.LINE_FEED).append(line);
            if (line.trim().endsWith(SystemConstant.SEMICOLON)) {
                Optional<SqlCommandParser> optionalCall = parse(stmt.toString());
                if (optionalCall.isPresent()) {
                    sqlCommandCallList.add(optionalCall.get());
                } else {
                    throw new RuntimeException("不支持该语法使用" + stmt.toString() + "'");
                }
                stmt.setLength(0);
            }
        }
        
        return sqlCommandCallList;
        
    }
    
    private static Optional<SqlCommandParser> parse(String stmt) {
        stmt = stmt.trim();
        if (stmt.endsWith(SystemConstant.SEMICOLON)) {
            stmt = stmt.substring(0, stmt.length() - 1).trim();
        }
        for (SqlCommand cmd : SqlCommand.values()) {
            final Matcher matcher = cmd.getPattern().matcher(stmt);
            if (matcher.matches()) {
                final String[] groups = new String[matcher.groupCount()];
                for (int i = 0; i < groups.length; i++) {
                    groups[i] = matcher.group(i + 1);
                }
                return cmd.getOperandConverter().apply(groups)
                        .map((operands) -> new SqlCommandParser(cmd, operands));
            }
        }
        return Optional.empty();
    }
    
    
    private static String trimStart(String str) {
        if (StringUtils.isEmpty(str)) {
            return str;
        }
        final char[] value = str.toCharArray();
        
        int start = 0, last = 0 + str.length() - 1;
        int end = last;
        while ((start <= end) && (value[start] <= ' ')) {
            start++;
        }
        if (start == 0 && end == last) {
            return str;
        }
        if (start >= end) {
            return "";
        }
        return str.substring(start, end);
    }
    
    
}
