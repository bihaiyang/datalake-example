package com.bii.oneid.configuration;


import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/09/27
 */
public class Configuration {
    
    private static final Logger log = LoggerFactory.getLogger(Configuration.class);
    
    protected final HashMap<String, Object> conf;
    
    public Configuration(){
        this.conf = new HashMap<>();
    }
    
    public Configuration(Configuration other){
        this.conf = new HashMap<>(other.conf);
    }
    
    public static Configuration fromMap(Map<String, String> map){
        final Configuration configuration = new Configuration();
        map.forEach(configuration::setString);
        return configuration;
    }
    
    
    public <T> Configuration set(ConfigOption<T> option, T value){
        setValueInternal(option.key(), value);
        return this;
    }
    
    <T> void setValueInternal(String key, T value){
        if(key == null){
            throw new NullPointerException("Key must not be null.");
        }
        if(value == null){
            throw new NullPointerException("Value must not be null.");
        }
        synchronized (this.conf){
            this.conf.put(key, value);
        }
    }
    
    
    public void setClass(String key, Class<?> klazz) {
        setValueInternal(key, klazz.getName());
    }
    
    
    public String getString(ConfigOption<String> configOption) {
        return getOptional(configOption).orElseGet(configOption::defaultValue);
    }
    
    public String getString(ConfigOption<String> configOption, String overrideDefault) {
        return getOptional(configOption).orElse(overrideDefault);
    }
    
   
    public void setString(String key, String value) {
        setValueInternal(key, value);
    }
  
    public void setString(ConfigOption<String> key, String value) {
        setValueInternal(key.key(), value);
    }
    
    public int getInteger(ConfigOption<Integer> configOption) {
        return getOptional(configOption).orElseGet(configOption::defaultValue);
    }
    
    public int getInteger(ConfigOption<Integer> configOption, int overrideDefault) {
        return getOptional(configOption).orElse(overrideDefault);
    }
    
    public void setInteger(String key, int value) {
        setValueInternal(key, value);
    }
    
    public void setInteger(ConfigOption<Integer> key, int value) {
        setValueInternal(key.key(), value);
    }
    
    public long getLong(String key, long defaultValue) {
        return getRawValue(key).map(Configuration::convertToLong).orElse(defaultValue);
    }
   
    public long getLong(ConfigOption<Long> configOption) {
        return getOptional(configOption).orElseGet(configOption::defaultValue);
    }
   
    public long getLong(ConfigOption<Long> configOption, long overrideDefault) {
        return getOptional(configOption).orElse(overrideDefault);
    }
    
    public void setLong(String key, long value) {
        setValueInternal(key, value);
    }
    
    public void setLong(ConfigOption<Long> key, long value) {
        setValueInternal(key.key(), value);
    }
    
    public boolean getBoolean(String key, boolean defaultValue) {
        return getRawValue(key).map(Configuration::convertToBoolean).orElse(defaultValue);
    }
   
    public boolean getBoolean(ConfigOption<Boolean> configOption) {
        return getOptional(configOption).orElseGet(configOption::defaultValue);
    }
   
    public boolean getBoolean(ConfigOption<Boolean> configOption, boolean overrideDefault) {
        return getOptional(configOption).orElse(overrideDefault);
    }
   
    public void setBoolean(String key, boolean value) {
        setValueInternal(key, value);
    }
    
    public void setBoolean(ConfigOption<Boolean> key, boolean value) {
        setValueInternal(key.key(), value);
    }
    
    public float getFloat(String key, float defaultValue) {
        return getRawValue(key).map(Configuration::convertToFloat).orElse(defaultValue);
    }
    
    public float getFloat(ConfigOption<Float> configOption) {
        return getOptional(configOption).orElseGet(configOption::defaultValue);
    }
    
    public float getFloat(ConfigOption<Float> configOption, float overrideDefault) {
        return getOptional(configOption).orElse(overrideDefault);
    }
    
    public void setFloat(String key, float value) {
        setValueInternal(key, value);
    }
    
    public void setFloat(ConfigOption<Float> key, float value) {
        setValueInternal(key.key(), value);
    }
    
    public double getDouble(String key, double defaultValue) {
        return getRawValue(key).map(Configuration::convertToDouble).orElse(defaultValue);
    }
    
    public double getDouble(ConfigOption<Double> configOption) {
        return getOptional(configOption).orElseGet(configOption::defaultValue);
    }
    
    public double getDouble(ConfigOption<Double> configOption, double overrideDefault) {
        return getOptional(configOption).orElse(overrideDefault);
    }
    
    public void setDouble(String key, double value) {
        setValueInternal(key, value);
    }
    
    public void setDouble(ConfigOption<Double> key, double value) {
        setValueInternal(key.key(), value);
    }
    
    
   
    private Optional<Object> getRawValue(String key) {
        if (key == null) {
            throw new NullPointerException("Key must not be null.");
        }
        
        synchronized (this.conf) {
            return Optional.ofNullable(this.conf.get(key));
        }
    }
    
    
    
    public <T> Optional<T> getOptional(ConfigOption<T> option) {
        Optional<Object> rawValue = getRawValueFromOption(option);
        Class<?> clazz = option.getClazz();
        
        try {
            return rawValue.map(v -> convertValue(v, clazz));
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Could not parse value '%s' for key '%s'.",
                            rawValue.map(Object::toString).orElse(""), option.key()),
                    e);
        }
    }
    
    
    public static <T> T convertValue(Object rawValue, Class<?> clazz) {
        if (Integer.class.equals(clazz)) {
            return (T) convertToInt(rawValue);
        } else if (Long.class.equals(clazz)) {
            return (T) convertToLong(rawValue);
        } else if (Boolean.class.equals(clazz)) {
            return (T) convertToBoolean(rawValue);
        } else if (Float.class.equals(clazz)) {
            return (T) convertToFloat(rawValue);
        } else if (Double.class.equals(clazz)) {
            return (T) convertToDouble(rawValue);
        } else if (String.class.equals(clazz)) {
            return (T) convertToString(rawValue);
        } else if (clazz.isEnum()) {
            return (T) convertToEnum(rawValue, (Class<? extends Enum<?>>) clazz);
        }
        throw new IllegalArgumentException("Unsupported type: " + clazz);
    }
    
    
    
    
    
    static Integer convertToInt(Object o) {
        if (o.getClass() == Integer.class) {
            return (Integer) o;
        } else if (o.getClass() == Long.class) {
            long value = (Long) o;
            if (value <= Integer.MAX_VALUE && value >= Integer.MIN_VALUE) {
                return (int) value;
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "Configuration value %s overflows/underflows the integer type.",
                                value));
            }
        }
        
        return Integer.parseInt(o.toString());
    }
    
    static Long convertToLong(Object o) {
        if (o.getClass() == Long.class) {
            return (Long) o;
        } else if (o.getClass() == Integer.class) {
            return ((Integer) o).longValue();
        }
        
        return Long.parseLong(o.toString());
    }
    
    
    
    
    static Boolean convertToBoolean(Object o) {
        if (o.getClass() == Boolean.class) {
            return (Boolean) o;
        }
        
        switch (o.toString().toUpperCase()) {
            case "TRUE":
                return true;
            case "FALSE":
                return false;
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Unrecognized option for boolean: %s. Expected either true or false(case insensitive)",
                                o));
        }
    }
    
    static Float convertToFloat(Object o) {
        if (o.getClass() == Float.class) {
            return (Float) o;
        } else if (o.getClass() == Double.class) {
            double value = ((Double) o);
            if (value == 0.0
                    || (value >= Float.MIN_VALUE && value <= Float.MAX_VALUE)
                    || (value >= -Float.MAX_VALUE && value <= -Float.MIN_VALUE)) {
                return (float) value;
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "Configuration value %s overflows/underflows the float type.",
                                value));
            }
        }
        
        return Float.parseFloat(o.toString());
    }
    
    static Double convertToDouble(Object o) {
        if (o.getClass() == Double.class) {
            return (Double) o;
        } else if (o.getClass() == Float.class) {
            return ((Float) o).doubleValue();
        }
        
        return Double.parseDouble(o.toString());
    }
    
    

    
    public static <E extends Enum<?>> E convertToEnum(Object o, Class<E> clazz) {
        if (o.getClass().equals(clazz)) {
            return (E) o;
        }
        
        return Arrays.stream(clazz.getEnumConstants())
                .filter(
                        e ->
                                e.toString()
                                        .toUpperCase(Locale.ROOT)
                                        .equals(o.toString().toUpperCase(Locale.ROOT)))
                .findAny()
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        String.format(
                                                "Could not parse value for enum %s. Expected one of: [%s]",
                                                clazz, Arrays.toString(clazz.getEnumConstants()))));
    }
  
    static String convertToString(Object o) {
        if (o.getClass() == String.class) {
            return (String) o;
        }
        return o.toString();
    }
    
    
    
    private Optional<Object> getRawValueFromOption(ConfigOption<?> configOption) {
        return getRawValue(configOption.key());
    }
    
}
