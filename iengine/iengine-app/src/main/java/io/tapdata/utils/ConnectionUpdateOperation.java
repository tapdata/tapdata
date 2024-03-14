package io.tapdata.utils;

import lombok.Getter;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/3/14 16:40 Create
 */
@Getter
public enum ConnectionUpdateOperation implements IUpdateOperation {
	ID("id"),
	TYPE("type"),
	NAME("name"),
	STATUS("status"),
	PDK_TYPE("pdkType"),
	PDK_HASH("pdkHash"),
	UNIQUE_NAME("uniqueName"),
	DATABASE_TYPE("database_type"),
	PLAIN_PASSWORD("plain_password"),
	EXT_PARAM("extParam"),
	SCHEMA("schema"),
	SCHEMA_VERSION("schemaVersion"),

	CONNECTION_ID("connectionId"),
	CONNECTION_OPTIONS("options"),
	DB_VERSION("db_version"),
	DB_FULL_VERSION("dbFullVersion"),
	RETRY("retry"),
	NEXT_RETRY("next_retry"),
	VALIDATE_DETAILS("validate_details"),

	LOAD_FIELDS_STATUS("loadFieldsStatus"),
	RESPONSE_BODY("response_body"),
	LOAD_SCHEMA_FIELD("loadSchemaField"),
	UPDATE_SCHEMA("updateSchema"),
	EVER_LOAD_SCHEMA("everLoadSchema"),
	EDIT_TEST("editTest"),
	EXTERNAL_STORAGE_ID("externalStorageId"),
	IS_EXTERNAL_STORAGE_ID("isExternalStorage"),
	;

	private final String key;

	ConnectionUpdateOperation(String key) {
		this.key = key;
	}

	@Getter
	public enum ResponseBody implements IUpdateOperation {
		RETRY("retry"),
		NEXT_RETRY("next_retry"),
		VALIDATE_DETAILS("validate_details"),
		;
		private final IUpdateOperation parent = RESPONSE_BODY;
		private final String key;

		ResponseBody(String key) {
			this.key = key;
		}
	}

	@Getter
	public enum Schema implements IUpdateOperation {
		TABLES("tables"),
		;
		private final IUpdateOperation parent = SCHEMA;
		private final String key;

		Schema(String key) {
			this.key = key;
		}
	}
}
