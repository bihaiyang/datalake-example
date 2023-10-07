package com.bii.oneid.util;

import java.util.HashMap;
import java.util.Map;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/10/04
 */
public class KeyPriorityUtil {

    public static Map<String, Integer> getKeyPriority(String keyList){
        Map<String, Integer> map = new HashMap<>(8);
        String[] keys = keyList.split(",");
        int priority = 1;
        for (String key : keys) {
            map.put(key, priority++);
        }
        return map;
    }
}
