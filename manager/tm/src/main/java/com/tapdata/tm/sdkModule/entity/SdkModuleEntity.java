package com.tapdata.tm.sdkModule.entity;

import com.tapdata.tm.modules.entity.ModulesEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.base.entity.BaseEntity;
import org.springframework.data.mongodb.core.mapping.Document;


/**
 * sdkModule
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("SdkModule")
public class SdkModuleEntity extends ModulesEntity {
	private String sdkId;
	private String sdkVersionId;
}