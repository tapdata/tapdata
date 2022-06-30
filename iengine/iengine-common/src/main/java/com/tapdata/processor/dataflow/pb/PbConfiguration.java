package com.tapdata.processor.dataflow.pb;

import com.github.os72.protobuf.dynamic.DynamicSchema;
import lombok.Builder;
import lombok.Data;

import java.util.Map;

@Data
@Builder
public class PbConfiguration {

	private DynamicSchema schema;

	/**
	 * 字段属性和msgDef名字的映射关系
	 * {
	 * "Unit.login": "Unit.Login",
	 * "Unit.login.encryptionRules": "string",
	 * "Unit.login.loginTime": "string",
	 * "Unit.login.platformName": "string",
	 * "Unit.login.loginSerialNumber": "string",
	 * "Unit.login.platformPassword": "string"
	 * }
	 */
	private Map<String, String> filedMsgDefNameMappingMap;

}
