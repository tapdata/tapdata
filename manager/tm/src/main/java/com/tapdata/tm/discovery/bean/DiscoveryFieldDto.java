package com.tapdata.tm.discovery.bean;

import lombok.Data;

@Data
public class DiscoveryFieldDto {
    /** 名称 */
    private String name;
    /** 类型 */
    private String dataType;
    /** 主键 */
    private Boolean primaryKey;
    /** 外键 */
    private Boolean foreignKey;
    /** 索引 */
    private Boolean index;
    /** 非空 */
    private Boolean notNull;
    /** 默认值 */
    private Boolean defaultValue;
    /** 业务名称 */
    private Boolean businessName;
    /** 业务类型 */
    private Boolean businessType;
    /** 业务描述 */
    private Boolean businessDesc;
}
