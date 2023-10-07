package com.bii.oneid.entity;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/09/25
 */

public class IdTypeColumnRelMap {
    
    /**
     * id类型
     */
    private String idTypeCode;
    
    /**
     * id类型名称
     */
    private String idTypeName;
    
    /**
     * id类型权重
     */
    private String idTypePriority;
    
    /**
     * id类型属性: 唯一ID/非唯一ID
     */
    private String idTypeAttribute;
    
    /**
     * 拾取规则 : 某个字段 或者 sql片段
     */
    private String collectRule;
    
    
    public String getIdTypeCode() {
        return idTypeCode;
    }
    
    public void setIdTypeCode(String idTypeCode) {
        this.idTypeCode = idTypeCode;
    }
    
    public String getIdTypeName() {
        return idTypeName;
    }
    
    public void setIdTypeName(String idTypeName) {
        this.idTypeName = idTypeName;
    }
    
    public String getIdTypePriority() {
        return idTypePriority;
    }
    
    public void setIdTypePriority(String idTypePriority) {
        this.idTypePriority = idTypePriority;
    }
    
    public String getIdTypeAttribute() {
        return idTypeAttribute;
    }
    
    public void setIdTypeAttribute(String idTypeAttribute) {
        this.idTypeAttribute = idTypeAttribute;
    }
    
    public String getCollectRule() {
        return collectRule;
    }
    
    public void setCollectRule(String collectRule) {
        this.collectRule = collectRule;
    }
}
