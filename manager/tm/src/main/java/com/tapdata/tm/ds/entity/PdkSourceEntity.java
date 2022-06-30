package com.tapdata.tm.ds.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;


/**
 * @Author: Zed
 * @Date: 2021/8/20
 * @Description: 数据源类型
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("PdkSource")
public class PdkSourceEntity extends BaseEntity {
    private String name;
    private String icon;
    private String group;
    private String version;
    private Integer buildNumber;
    @Indexed
    private String scope;
    @Indexed
    private String type;
    @Indexed
    private String jarFile;
    private Long jarTime;
    private String jarRid;
    private Object applications;
    private Object node;
}
