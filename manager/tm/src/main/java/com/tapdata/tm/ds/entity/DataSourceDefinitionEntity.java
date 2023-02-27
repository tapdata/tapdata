
package com.tapdata.tm.ds.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import io.tapdata.pdk.apis.entity.Capability;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * 数据源定义模型
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("DatabaseTypes")
public class DataSourceDefinitionEntity extends BaseEntity {


    /** 删除标记*/
    private boolean is_deleted = false;
    /**
     * JsonSchema，用于渲染创建数据源的页面，格式查看：https://json-schema.org/specification.html
     */
    private String properties;
    /**
     * official-官方提供的，self-自定义数据源
     */
    private String supplierType;
    private String pdkId;
    /** pdk类型 */
    private String pdkType;
    private String pdkHash;

    /**
     * 名称 Amazon S3 Connection Verifier
     */
    @Indexed
    private String name;
    /**
     * 类型  amazon_s3_connection_verifier
     */
    @Indexed(unique = true)
    private String type;
    /**
     *
     */
    private String source;
    /**
     * 源，目标， 源&目标
     */
    private String connectionType;
    /**
     *
     */
    private String buildProfiles;
    /**
     * 类名
     */
    private String className;
    /**
     * 支持的目标数据源
     */
    private List<String> supportTargetDatabaseType;
    /**
     * lib目录
     */
    private String libDir;
    /**
     * lib名
     */
    private String libName;
    /**
     * 版本
     */
    private String version;
    private boolean latest;

    private String icon;
    private String group;
    private Integer buildNumber;
    private String scope;
    private String jarFile;
    private Long jarTime;
    private String jarRid;
    private String expression;
    private String tapTypeDataTypeMap;

    private LinkedHashMap<String, Object> messages;

    private List<Capability> capabilities;
    private Boolean beta;
    private String authentication;
    private List<String> tags;
}
