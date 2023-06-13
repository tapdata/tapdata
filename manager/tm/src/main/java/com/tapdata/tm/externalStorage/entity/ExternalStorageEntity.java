package com.tapdata.tm.externalStorage.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;


/**
 * External Storage
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("ExternalStorage")
public class ExternalStorageEntity extends BaseEntity {
	private String name;
	private String type;
	private String uri;
	@Deprecated
	private String table;
	private Integer ttlDay;
	private Boolean canEdit = true;
	private Boolean canDelete;
    private boolean defaultStorage = false;
	private Boolean init;

	private boolean ssl;
	private String sslCA;
	private String sslKey;
	private String sslPass;
	private boolean sslValidate;
	private boolean checkServerIdentity;
}
