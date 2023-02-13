package com.tapdata.tm.ds.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.constants.DataSourceQCType;
import io.tapdata.pdk.apis.entity.Capability;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;
import java.util.Map;

/**
 * @Author: Zed
 * @Date: 2021/8/20
 * @Description: 数据源类型
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class DataSourceTypeDto extends BaseDto {
    /** 删除标记*/
    private boolean is_deleted = false;
    /** JsonSchema，用于渲染创建数据源的页面，格式查看：https://json-schema.org/specification.html */
    //包含了 applications
    private Map<String, Object> properties;
    private Map<String, Object> messages;
    /** official-官方提供的，self-自定义数据源 */
    private String supplierType;
    /** pdk类型 */
    private String pdkType;
    private String pdkId;
    private String pdkHash;
    /** 名称 Amazon S3 Connection Verifier */
    private String name;
    /** 类型  amazon_s3_connection_verifier */
    private String type;
    /**  */
    private String source;
    /** 源，目标， 源&目标 */
    private String connectionType;
    /**  */
    private String buildProfiles;
    /** 类名 */
    private String className;
    /** 支持的目标数据源 */
    private List<String> supportTargetDatabaseType;
    /** lib目录 */
    private String libDir;
    /** lib名 */
    private String libName;
    /** 版本 */
    private String version;
    private boolean latest;

    private String icon;
    private String group;
    private Integer buildNumber;
    private String scope;
    private String jarFile;
    private Long jarTime;
    private String jarRid;
    /**
     * Quality Control Type（认证类型｜质量控制类型）
     */
    private DataSourceQCType qcType;

    private List<String> tags; // 标签  - 本地自建库(localDatabase) 云数据库(cloudDatabase) 消息队列(mq) NoSQL数据库(nosql) SaaS应用(saas)

    private Boolean isComing; // 即将上线

    private List<Capability> capabilities;
}
