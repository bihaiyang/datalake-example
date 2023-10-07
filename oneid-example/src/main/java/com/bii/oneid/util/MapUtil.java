package com.bii.oneid.util;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/10/07
 */
public class MapUtil {
    
    public static <K, V> Map<V, K> reverse(Map<K, V> map) {
        return map.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    }
}
