package com.tapdata.tm.permissions.constants;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.DataPermissionHelper;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/8/8 19:00 Create
 */
public enum DataPermissionMenuEnums {
	Connections(DataPermissionDataTypeEnums.Connections, "v2_datasource_all_data"),
	MigrateTack(DataPermissionDataTypeEnums.Task, "v2_data_replication_all_data"),
	SyncTack(DataPermissionDataTypeEnums.Task, "v2_data_flow_all_data"),
	LogCollectorTack(DataPermissionDataTypeEnums.Task, ""),
	ConnHeartbeatTack(DataPermissionDataTypeEnums.Task, ""),
	MemCacheTack(DataPermissionDataTypeEnums.Task, ""),
	;

	private final DataPermissionDataTypeEnums dataType;
	private final String allDataPermissionName;

	DataPermissionMenuEnums(DataPermissionDataTypeEnums dataType, String allDataPermissionName) {
		this.dataType = dataType;
		this.allDataPermissionName = allDataPermissionName;
	}

	public DataPermissionDataTypeEnums getDataType() {
		return dataType;
	}

	public String getAllDataPermissionName() {
		return allDataPermissionName;
	}

	public <T> T checkAndSetFilter(UserDetail userDetail, DataPermissionActionEnums actionEnums, Supplier<T> supplier) {
		return DataPermissionHelper.check(userDetail, this, actionEnums, this.dataType, null, supplier, supplier);
	}

	public static DataPermissionMenuEnums ofTaskSyncType(String taskSyncType) {
		if (TaskDto.SYNC_TYPE_MIGRATE.equals(taskSyncType)) {
			return DataPermissionMenuEnums.MigrateTack;
		} else if (TaskDto.SYNC_TYPE_SYNC.equals(taskSyncType)) {
			return DataPermissionMenuEnums.SyncTack;
		} else if (TaskDto.SYNC_TYPE_LOG_COLLECTOR.equals(taskSyncType)) {
			return DataPermissionMenuEnums.LogCollectorTack;
		} else if (TaskDto.SYNC_TYPE_CONN_HEARTBEAT.equals(taskSyncType)) {
			return DataPermissionMenuEnums.ConnHeartbeatTack;
		} else if (TaskDto.SYNC_TYPE_MEM_CACHE.equals(taskSyncType)) {
			return DataPermissionMenuEnums.MemCacheTack;
		}
		return null;
	}

	public static <T> T checkAndSetFilter(UserDetail userDetail, Filter filter, DataPermissionActionEnums actionEnums, Supplier<T> supplier) {
		return Optional.ofNullable(filter.getWhere())
			.map(o -> (String) o.get("syncType"))
			.map(DataPermissionMenuEnums::ofTaskSyncType)
			.map(menuEnums -> {
				return DataPermissionHelper.check(userDetail, menuEnums, actionEnums, menuEnums.dataType, null, supplier, supplier);
//				syncType.checkAndSetFilter(userDetail, actionEnums, supplier)
			})
			.orElseGet(supplier);
	}
}
