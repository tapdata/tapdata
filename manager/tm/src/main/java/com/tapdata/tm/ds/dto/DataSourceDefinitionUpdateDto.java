
package com.tapdata.tm.ds.dto;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.tapdata.tm.commons.base.convert.ObjectIdDeserialize;
import com.tapdata.tm.commons.base.convert.ObjectIdSerialize;
import lombok.Data;
import org.bson.types.ObjectId;

import java.util.List;
import java.util.Map;

/**
 * 可以更新的数据源字段实体
 */
@Data
public class DataSourceDefinitionUpdateDto {

    @JsonSerialize( using = ObjectIdSerialize.class)
    @JsonDeserialize( using = ObjectIdDeserialize.class)
    private ObjectId id;
    /** JsonSchema，用于渲染创建数据源的页面，格式查看：https://json-schema.org/specification.html */
    private Map<String, Object> properties;

    /** 名称 Amazon S3 Connection Verifier */
    private String name;
    /** 类型  amazon_s3_connection_verifier */
    private String dataSourceType;
    /**  */
    private String source;
    /** 源，目标， 源&目标 */
    private String connectionType;
    /**  */
    private String buildProfiles;
    /** 类名 */
    private String className;
    /** 支持的目标数据源 */
    private List<String> supportTargetType;
    /** lib目录 */
    private String libDir;
    /** lib名 */
    private String libName;
    /** 版本 */
    private String version;
}
