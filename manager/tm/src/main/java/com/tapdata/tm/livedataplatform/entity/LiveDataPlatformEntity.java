package com.tapdata.tm.livedataplatform.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;


/**
 * LineageGraph
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("LiveDataPlatform")
public class LiveDataPlatformEntity extends BaseEntity {
    private String mode;

}