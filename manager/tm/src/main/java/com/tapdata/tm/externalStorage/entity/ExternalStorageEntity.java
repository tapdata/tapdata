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
	private boolean canEdit = false;
	private boolean canDelete = true;
    private boolean defaultStorage = false;
	private boolean init = false;
}