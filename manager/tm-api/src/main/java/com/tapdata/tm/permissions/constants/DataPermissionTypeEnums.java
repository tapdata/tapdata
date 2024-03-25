package com.tapdata.tm.permissions.constants;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/8/3 12:09 Create
 */
public enum DataPermissionTypeEnums {
	Role,
	;

	public static DataPermissionTypeEnums parse(String str) {
		if (null == str || str.isEmpty()) return null;

		for (DataPermissionTypeEnums type : values()) {
			if (type.name().equalsIgnoreCase(str)) {
				return type;
			}
		}
		return null;
	}
}
