package io.tapdata.common.sharecdc;

import com.tapdata.constant.Log4jUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.sharecdc.LogContent;
import com.tapdata.entity.sharecdc.ShareCdcConstant;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.LogCollecotrConnConfig;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.common.SettingService;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.pdk.apis.functions.connector.source.ConnectionConfigWithTables;
import org.apache.commons.collections4.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.function.Function;

/**
 * @author samuel
 * @Description
 * @create 2022-01-27 10:35
 **/
public class ShareCdcUtil {

	private final static Logger logger = LogManager.getLogger(ShareCdcUtil.class);
	private final static String SHARE_CDC_KEY_PREFIX = "SHARE_CDC_";
	private final static String NAMESPACE_DELIMITER = ".";

	public static String getConstructName(TaskDto taskDto) {
		return SHARE_CDC_KEY_PREFIX + taskDto.getName();
	}

	public static String getConstructName(TaskDto taskDto, String tableName) {
		return SHARE_CDC_KEY_PREFIX + taskDto.getName() + "_" + tableName;
	}

	public static boolean shareCdcEnable(SettingService settingService) {
		assert settingService != null;
		settingService.loadSettings(ShareCdcConstant.SETTING_SHARE_CDC_ENABLE);
		String shareCdcEnable = settingService.getString(ShareCdcConstant.SETTING_SHARE_CDC_ENABLE, "true");
		try {
			return Boolean.parseBoolean(shareCdcEnable);
		} catch (Exception e) {
			logger.warn("Get global share cdc enable setting failed, key: " + ShareCdcConstant.SETTING_SHARE_CDC_ENABLE
					+ ", will use default value: true"
					+ "; Error: " + e.getMessage() + "\n" + Log4jUtil.getStackString(e));
			return true;
		}
	}

	public static String getTapRecordEventTableName(TapRecordEvent tapRecordEvent) {
		if (null != tapRecordEvent.getNamespaces() && !tapRecordEvent.getNamespaces().isEmpty()) {
			return joinNamespaces(tapRecordEvent.getNamespaces());
		} else {
			return tapRecordEvent.getTableId();
		}
	}

	public static String getTapRecordEventTableNameV2(TapRecordEvent tapRecordEvent,String taskType) {
		if (TaskDto.SYNC_TYPE_LOG_COLLECTOR.equals(taskType) && null != tapRecordEvent.getNamespaces() && !tapRecordEvent.getNamespaces().isEmpty()) {
			return joinNamespaces(tapRecordEvent.getNamespaces());
		} else {
			return tapRecordEvent.getTableId();
		}
	}

	public static String joinNamespaces(Collection<String> namespaces) {
		return String.join(NAMESPACE_DELIMITER, namespaces);
	}

	public static String getTableId(LogContent logContent) {
		if (null != logContent.getTableNamespaces() && !logContent.getTableNamespaces().isEmpty()) {
			return joinNamespaces(logContent.getTableNamespaces());
		}
		return logContent.getFromTable();
	}

	public static List<String> getTableNames(LogCollectorNode logCollectorNode) {
		if (null != logCollectorNode.getLogCollectorConnConfigs() && !logCollectorNode.getLogCollectorConnConfigs().isEmpty()) {
			Set<String> tableNames = new HashSet<>();
			for (LogCollecotrConnConfig config : logCollectorNode.getLogCollectorConnConfigs().values()) {
				String tableNamePrefix = joinNamespaces(config.getNamespace());
				for (String tableName : config.getTableNames()) {
					tableNames.add(joinNamespaces(Arrays.asList(tableNamePrefix, tableName)));
				}
			}
			return new ArrayList<>(tableNames);
		}
		return logCollectorNode.getTableNames();
	}

	public static List<LogCollecotrConnConfig> getLogCollectorConfigs(LogCollectorNode logCollectorNode) {
		List<LogCollecotrConnConfig> connConfigs = new ArrayList<>();
		if (MapUtils.isNotEmpty(logCollectorNode.getLogCollectorConnConfigs())) {
			connConfigs.addAll(logCollectorNode.getLogCollectorConnConfigs().values());
		}else{
			List<String> tableNames = logCollectorNode.getTableNames();
			String connectionId = logCollectorNode.getConnectionIds().get(0);
			LogCollecotrConnConfig logCollecotrConnConfig = new LogCollecotrConnConfig(connectionId, logCollectorNode.getTableNames());
			connConfigs.add(logCollecotrConnConfig);
		}
		return connConfigs;
	}

	public static void fillConfigNamespace(LogCollectorNode logCollectorNode, Function<String, Connections> findConnection) {
		Map<String, LogCollecotrConnConfig> connConfigs = logCollectorNode.getLogCollectorConnConfigs();
		if (null != connConfigs && !connConfigs.isEmpty()) {
			for (LogCollecotrConnConfig logCollecotrConnConfig : connConfigs.values()) {
				Connections conn = findConnection.apply(logCollecotrConnConfig.getConnectionId());
				logCollecotrConnConfig.setNamespace(conn.getNamespace());
			}
		}
	}

	public static List<ConnectionConfigWithTables> connectionConfigWithTables(Node<?> node, Function<Collection<String>, List<Connections>> findConnections) {
		// If 'LogCollectorNode' is merge connection mode then 'connectionConfigWithTables' not null use 'StreamReadMultiConnectionFunction'
		if (node instanceof LogCollectorNode) {
			LogCollectorNode collectorNode = (LogCollectorNode) node;
			Map<String, LogCollecotrConnConfig> connConfigs = collectorNode.getLogCollectorConnConfigs();
			if (null != connConfigs && !connConfigs.isEmpty()) {
				List<Connections> connectionsList = findConnections.apply(connConfigs.keySet());
				if (null == connectionsList || connectionsList.isEmpty()) {
					throw new RuntimeException("Collector connections is empty.");
				}

				LogCollecotrConnConfig config;
				ConnectionConfigWithTables item;
				List<ConnectionConfigWithTables> list = new ArrayList<>();
				for (Connections conn : connectionsList) {
					config = connConfigs.get(conn.getId());
					if (null == config) {
						throw new RuntimeException("Collector config not found with connection id '" + conn.getId() + "'");
					}

					item = new ConnectionConfigWithTables();
					item.setTables(config.getTableNames());
					item.setConnectionConfig(new DataMap());
					item.getConnectionConfig().putAll(conn.getConfig());

					list.add(item);
				}
				return list;
			}
		}
		return null;
	}

	public static List<Connections> getConnectionIds(Node<?> node, Function<Collection<String>, List<Connections>> findConnections, Map<String, Connections> connectionsMap) {
		if (node instanceof LogCollectorNode) {
			LogCollectorNode collectorNode = (LogCollectorNode) node;
			Map<String, LogCollecotrConnConfig> connConfigs = collectorNode.getLogCollectorConnConfigs();
			if (null != connConfigs && !connConfigs.isEmpty()) {
				List<Connections> connectionsList = new ArrayList<>();
				Set<String> connIds = new HashSet<>();
				if (null == connectionsMap) {
					connIds = connConfigs.keySet();
				} else {
					for (String id : connConfigs.keySet()) {
						if (connectionsMap.containsKey(id)) {
							connectionsList.add(connectionsMap.get(id));
						} else {
							connIds.add(id);
						}
					}
				}
				if (!connIds.isEmpty()) {
					connectionsList.addAll(findConnections.apply(connIds));
				}
				if (connectionsList.isEmpty()) {
					throw new RuntimeException("Collector connections is empty.");
				}
				return connectionsList;
			}
		}
		return null;
	}

	public static List<Connections> getConnectionIds(Node<?> node, Function<Collection<String>, List<Connections>> findConnections) {
		return getConnectionIds(node, findConnections, null);
	}

	public static void iterateAndHandleSpecialType(Map<String, Object> map, Function<Object, Object> handleFunc) {
		if(null == map) return;
		if(null == handleFunc) return;
		iterateAndHandleMap(map, handleFunc, null, null, null);
	}

	public static void iterateAndHandleSpecialType(Map<String, Object> map, Function<Object, Object> handleFunc,
												   IllegalDatePredicate illegalDatePredicate, TapEvent tapEvent, EventType eventType) {
		if (null == map) return;
		if (null == handleFunc) return;
		iterateAndHandleMap(map, handleFunc, illegalDatePredicate, tapEvent, eventType);
	}

	private static void iterateAndHandleMap(Map<String, Object> map, Function<Object, Object> handleFunc, IllegalDatePredicate illegalDatePredicate, TapEvent tapEvent, EventType eventType) {
		if (null == map) return;
		for (Map.Entry<String, Object> entry : map.entrySet()) {
			Object value = entry.getValue();

			if (value instanceof Map) {
				iterateAndHandleMap((Map<String, Object>) value, handleFunc, illegalDatePredicate, tapEvent, eventType);
			} else if (value instanceof Collection) {
				iterateAndHandleCollection((Collection<?>) value, handleFunc, illegalDatePredicate, tapEvent, eventType);
			} else {
				Object result = handleFunc.apply(value);
				if (null != illegalDatePredicate && null != tapEvent && null != eventType && illegalDatePredicate.test(result)) {
					TapEventUtil.setContainsIllegalDate(tapEvent, true);
					switch (eventType) {
						case BEFORE:
							TapEventUtil.addBeforeIllegalDateField(tapEvent, entry.getKey());
							break;
						case AFTER:
							TapEventUtil.addAfterIllegalDateField(tapEvent, entry.getKey());
							break;
						default:
							break;
					}
				}
				map.put(entry.getKey(), result);
			}
		}
	}

	private static void iterateAndHandleCollection(Collection<?> _collection, Function<Object, Object> handleFunc, IllegalDatePredicate illegalDatePredicate, TapEvent tapEvent, EventType eventType) {
		if (null == _collection) return;
		for (Object obj : _collection) {
			if (obj instanceof Map) {
				iterateAndHandleMap((Map<String, Object>) obj, handleFunc, illegalDatePredicate, tapEvent, eventType);
			} else if (obj instanceof Collection) {
				iterateAndHandleCollection((Collection<?>) obj, handleFunc, illegalDatePredicate, tapEvent, eventType);
			} else {
				// This situation will not be processed temporarily
				// 1. Replacing elements in collection has performance issues
				// 2. The probability of this scenario occurring is very low
			}
		}
	}
}
