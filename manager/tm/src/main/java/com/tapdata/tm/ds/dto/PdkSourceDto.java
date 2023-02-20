package com.tapdata.tm.ds.dto;

import com.tapdata.tm.commons.constants.DataSourceQCType;
import io.tapdata.pdk.apis.entity.Capability;
import lombok.*;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * @Author: Zed
 * @Date: 2022/2/23
 * @Description:
 */
@Data
@EqualsAndHashCode(callSuper=false)
public class PdkSourceDto {
    private String name;
    private String icon;
    private String group;
    private String version;
    private Integer buildNumber;
    private String scope;
    private String type;
    private String jarFile;
    private Long jarTime;
    private String jarRid;
    private LinkedHashMap<String, Object> configOptions;
    private Object node;
    private String id;
    private String expression;
    private String tapTypeDataTypeMap;
    /**
     * Quality Control Type（认证类型｜质量控制类型）
     */
    private DataSourceQCType qcType;
    //多语言信息
    private LinkedHashMap<String, Object> messages;
    //数据能力
    private List<Capability> capabilities;
    private String pdkAPIVersion;
    private Integer pdkAPIBuildNumber;
}
