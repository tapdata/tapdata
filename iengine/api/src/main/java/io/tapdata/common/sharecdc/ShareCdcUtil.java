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
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.functions.connector.source.ConnectionConfigWithTables;
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
}
