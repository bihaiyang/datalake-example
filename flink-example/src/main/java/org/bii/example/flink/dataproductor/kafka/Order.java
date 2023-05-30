package org.bii.example.flink.dataproductor.kafka;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/05/29
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Order {
    
    /**
     * 订单编号
     */
    private String uuid;
    
    /**
     * 城市编码
     */
    private Long cityId;
    
    /**
     * 性别
     */
    private String sex;
    
    /**
     * 商品id
     */
    private Long productId;
    
    /**
     * 下单时间
     */
    private String orderTime;
    
}
