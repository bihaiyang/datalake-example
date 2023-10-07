package com.bii.oneid.util;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/09/25
 */
public class StringUtil {
    
    private final static Pattern pattern = Pattern.compile("((?<=\\{)([a-zA-Z\\d]+)(?=\\}))");
    
    public static String replace(String str, Map<String, Object> map){
        
        Matcher matcher = pattern.matcher(str);
        while (matcher.find()) {
            String key = matcher.group();
            str = str.replaceAll("\\$\\{" + key + "\\}", map.get(key) + "");
        }
        return str;
    }
}
