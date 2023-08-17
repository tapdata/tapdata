package com.tapdata.tm.permissions.constants;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.DataPermissionHelper;

import java.util.function.Supplier;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/8/8 19:00 Create
 */
public enum DataPermissionMenu {
	Connections(DataPermissionDataTypeEnums.Connections, "v2_datasource_all_data"),
	MigrateTack(DataPermissionDataTypeEnums.Task, "v2_data_replication_all_data"),
	SyncTack(DataPermissionDataTypeEnums.Task, "v2_data_flow_all_data"),
	;

	private final DataPermissionDataTypeEnums dataType;
	private final String allDataPermissionName;

	DataPermissionMenu(DataPermissionDataTypeEnums dataType, String allDataPermissionName) {
		this.dataType = dataType;
		this.allDataPermissionName = allDataPermissionName;
	}

	public DataPermissionDataTypeEnums getDataType() {
		return dataType;
	}

	public String getAllDataPermissionName() {
		return allDataPermissionName;
	}

	public <T> T openInController(UserDetail userDetail, boolean setFilter, Supplier<T> supplier) {
		return DataPermissionHelper.openInController(userDetail, this, setFilter, supplier);
	}
}
