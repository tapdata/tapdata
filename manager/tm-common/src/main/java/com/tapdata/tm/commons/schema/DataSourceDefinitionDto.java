
package com.tapdata.tm.commons.schema;

import com.fasterxml.jackson.core.type.TypeReference;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.commons.base.dto.BaseDto;
import io.tapdata.pdk.apis.entity.Capability;
import lombok.*;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 数据源定义模型
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class DataSourceDefinitionDto extends BaseDto {

    public static final String PDK_TYPE = "pdk";

    /** 删除标记*/
    private boolean is_deleted = false;
    /** JsonSchema，用于渲染创建数据源的页面，格式查看：https://json-schema.org/specification.html */
    //包含了 applications
    private LinkedHashMap<String, Object> properties;
    /** official-官方提供的，self-自定义数据源 */
    private String supplierType;
    /** pdk类型 */
    private String pdkType;

    /**
     * used to identify PDKs.
     * It's a combination of:
     *   1. scope
     *   2. customerId(if scope == customer)
     *   4. pdkId
     *   5. group
     *   6. version
     */
    private String pdkHash;

    private String pdkId;

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

    private String expression;
    private String tapTypeDataTypeMap;

    private LinkedHashMap<String, Object> messages;

    private List<Capability> capabilities;

    private Boolean beta;
    private String pdkAPIVersion;
    private Integer pdkAPIBuildNumber;


    public String calculatePdkHash(String customerId){
        List<String> input = new ArrayList<>();
        input.add(scope);
        if ("customer".equalsIgnoreCase(scope)) {
            input.add(customerId);
        }
        input.add(pdkId);
        input.add(group);
        input.add(version);
        String str = String.join("#", input);
        try {
            byte[] raw = MessageDigest.getInstance("SHA-256").digest(str.getBytes(StandardCharsets.UTF_8));
            BigInteger number = new BigInteger(1, raw);
            StringBuilder hexString = new StringBuilder(number.toString(16));
            while (hexString.length() < 32) {
                hexString.insert(0, '0');
            }
            return hexString.toString();
        } catch (NoSuchAlgorithmException e) {
            return str;
        }

    }

    public Map<Class<?>, String> getTapMap() {
        return JsonUtil.parseJsonUseJackson(tapTypeDataTypeMap, new TypeReference<Map<Class<?>, String>>() {});
    }
}
