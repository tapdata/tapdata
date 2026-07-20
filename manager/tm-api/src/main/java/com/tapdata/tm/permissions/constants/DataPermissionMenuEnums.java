package com.tapdata.tm.permissions.constants;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.DataPermissionHelper;
import org.apache.commons.collections4.MapUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/8/8 19:00 Create
 */
public enum DataPermissionMenuEnums {
	Connections(DataPermissionDataTypeEnums.Connections,initAllDataPermissionName(DataPermissionDataTypeEnums.Connections,"v2_datasource_all_data")),
	ApiClient(DataPermissionDataTypeEnums.Application, initAllDataPermissionName(DataPermissionDataTypeEnums.Application,"v2_api-client_all_data")),
	ApiServers(DataPermissionDataTypeEnums.ApiServer, initAllDataPermissionName(DataPermissionDataTypeEnums.ApiServer,"v2_api-servers_all_data")),
	MigrateTack(DataPermissionDataTypeEnums.Task, initAllDataPermissionName(DataPermissionDataTypeEnums.Task,"v2_data_replication_all_data")),
	SyncTack(DataPermissionDataTypeEnums.Task, initAllDataPermissionName(DataPermissionDataTypeEnums.Task,"v2_data_flow_all_data")),
	LogCollectorTack(DataPermissionDataTypeEnums.Task, initAllDataPermissionName(DataPermissionDataTypeEnums.Task,"v2_log_collector_all_data")),
	ConnHeartbeatTack(DataPermissionDataTypeEnums.Task, null),
	MemCacheTack(DataPermissionDataTypeEnums.Task, initAllDataPermissionName(DataPermissionDataTypeEnums.Task,"v2_shared_cache_all_data")),
	TaskRebalance(DataPermissionDataTypeEnums.Task, initTaskRebalancePermissionName()),
	INSPECT_TACK(DataPermissionDataTypeEnums.INSPECT, initAllDataPermissionName(DataPermissionDataTypeEnums.INSPECT,"v2_data_check_all_data")),
	Modules(DataPermissionDataTypeEnums.Modules, initAllDataPermissionName(DataPermissionDataTypeEnums.Modules,"v2_data-server-list_all_data")),
	;

	private final DataPermissionDataTypeEnums dataType;
	private final Map<String,String> allDataPermissionName;

	DataPermissionMenuEnums(DataPermissionDataTypeEnums dataType, Map<String,String> allDataPermissionName) {
		this.dataType = dataType;
		this.allDataPermissionName = allDataPermissionName;
	}

	public DataPermissionDataTypeEnums getDataType() {
		return dataType;
	}

	public String getAllDataPermissionName(String actionName) {
		return MapUtils.isNotEmpty(allDataPermissionName) ? allDataPermissionName.get(actionName) : null;
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

	public static Map<String,String> initAllDataPermissionName(DataPermissionDataTypeEnums enums,String allData){
		Map<String,String> allDataPermissionName = new HashMap<>();
		enums.allActions().forEach(action -> {
			if(action.equals("View")){
				allDataPermissionName.put(action, allData);
			}else{
				allDataPermissionName.put(action, allData+"_"+action);
			}
		});
		return allDataPermissionName;
	}

	public static Map<String, String> initTaskRebalancePermissionName() {
		Map<String, String> allDataPermissionName = new HashMap<>();
		allDataPermissionName.put(DataPermissionActionEnums.View.name(), "v2_task_rebalance");
		allDataPermissionName.put(DataPermissionActionEnums.Edit.name(), "v2_task_rebalance_Edit");
		return allDataPermissionName;
	}
}
