package com.tapdata.entity;

import java.util.HashMap;
import java.util.Map;

public enum FileProtocolEnum {
	LOCALFILE("localFile", "com.tapdata.constant.LocalFileUtil"),
	FTP("ftp", "com.tapdata.constant.FTPUtil"),
	SMB("smb", "com.tapdata.constant.SmbUtil"),
	;

	private String type;

	private String className;

	FileProtocolEnum(String type, String className) {
		this.type = type;
		this.className = className;
	}

	public String getType() {
		return type;
	}

	public String getClassName() {
		return className;
	}

	private static final Map<String, FileProtocolEnum> typeMap = new HashMap<>();

	static {
		for (FileProtocolEnum fileProtocolEnum : FileProtocolEnum.values()) {
			typeMap.put(fileProtocolEnum.getType(), fileProtocolEnum);
		}
	}

	public static FileProtocolEnum fromType(String type) {
		return typeMap.get(type);
	}
}
