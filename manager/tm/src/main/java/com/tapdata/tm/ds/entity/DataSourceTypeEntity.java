package com.tapdata.tm.ds.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;

/**
 * @Author: Zed
 * @Date: 2021/8/20
 * @Description: 数据源类型
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class DataSourceTypeEntity extends BaseEntity {
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
    /** 最后更新时间  yyyyMMddHHmmss */
    private String lastUpdated;
    /** lib目录 */
    private String libDir;
    /** lib名 */
    private String libName;
    /** 版本 */
    private String version;

    private List<String> tags; // 标签  - 本地自建库(localDatabase) 云数据库(cloudDatabase) 消息队列(mq) NoSQL数据库(nosql) SaaS应用(saas)

    private Boolean isComing; // 即将上线
}
