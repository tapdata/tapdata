package com.tapdata.tm.featurecheck.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

@EqualsAndHashCode(callSuper = true)
@Data
@Document("FeatureCheck")
public class FeatureCheckEntity extends BaseEntity {

    private String featureType;

    private String featureCode;

    private String minAgentVersion;

    private String description;
}
