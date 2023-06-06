package org.bii.example.flink.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/06/05
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.ANNOTATION_TYPE})
public @interface TableName {
    
    /**
     * 实体对应的表名
     */
    String value() default "";
    
    
    /**
     * 是否保持使用全局的 tablePrefix 的值
     * <p> 只生效于 既设置了全局的 tablePrefix 也设置了上面 {@link #value()} 的值 </p>
     * <li> 如果是 false , 全局的 tablePrefix 不生效 </li>
     *
     */
    boolean keepGlobalPrefix() default false;
 
   
}
