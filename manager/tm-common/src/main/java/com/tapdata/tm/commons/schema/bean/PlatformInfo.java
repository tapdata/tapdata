package com.tapdata.tm.commons.schema.bean;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;
import org.springframework.data.mongodb.core.mapping.Field;

/**
 * @Author: Zed
 * @Date: 2021/9/9
 * @Description:
 */
@Data
@EqualsAndHashCode(callSuper=false)
public class PlatformInfo {
    private String region;
    private String zone;
    private String sourceType;
    @Field("DRS_instances")
    @JsonProperty("DRS_instances")
    private String drsInstances;
    @Field("IP_type")
    @JsonProperty("IP_type")
    private String ipType;
    private String vpc;
    private String ecs;
    private String checkedVpc;
    private String strategyExistence;
    @Field("DRS_regionName")
    @JsonProperty("DRS_regionName")
    private String drsRegionName;
    @Field("DRS_zoneName")
    @JsonProperty("DRS_zoneName")
    private String drsZoneName;;
    private String regionName;
    private String zoneName;
    private String agentType;
    private Boolean isThrough;

    private Boolean bidirectional;

}
