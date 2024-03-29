package io.tapdata.Schedule;

import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.SystemUtil;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.worker.WorkerSingletonLock;
import io.tapdata.utils.AppType;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/3/13 10:43 Create
 */
public class WorkerHeatBeatReports {

	private ConfigurationCenter configCenter;
	private ClientMongoOperator clientMongoOperator;

	private ConnectorManager connectorManager;
	private Integer threshold;
	private String instanceNo;
	private String version;
	private String userId;
	private String hostname;
	private Double processCpuLoad;
	private long usedMemory;
	private String singletonLock;

	public void init(ConnectorManager connectorManager, ConfigurationCenter configCenter, ClientMongoOperator clientMongoOperator) throws Exception {
		this.connectorManager = connectorManager;
		this.configCenter = configCenter;
		this.clientMongoOperator = clientMongoOperator;
		this.threshold = connectorManager.getThreshold();
		this.instanceNo = connectorManager.getInstanceNo();
		this.version = connectorManager.getVersion();
		this.userId = (String) configCenter.getConfig(ConfigurationCenter.USER_ID);
		this.hostname = SystemUtil.getHostName();
		this.processCpuLoad = SystemUtil.getProcessCpuLoad();
		this.usedMemory = SystemUtil.getUsedMemory();
		this.singletonLock = WorkerSingletonLock.getCurrentTag();
	}

	public void report(boolean isExit) {
		Map<String, Object> value = new HashMap<>();
		value.put("total_thread", threshold);
		value.put("process_id", instanceNo);
		value.put("user_id", userId);
		value.put("singletonLock", singletonLock);
		value.put("version", version);
		value.put("hostname", hostname);
		value.put("cpuLoad", processCpuLoad);
		value.put("usedMemory", usedMemory);
		value.put("metricValues", connectorManager.getMetricValues());
		value.put("worker_type", ConnectorConstant.WORKER_TYPE_CONNECTOR);

		connectorManager.setPlatformInfo(value);
		configIfNotBlank("version", value::put);
		configIfNotBlank("gitCommitId", value::put);
		if (isExit) {
			// 更新ping_time为1，以便于其他端可以快速识别本实例已经停止，而不用等待超时
			value.put("ping_time", 1);
		}
		clientMongoOperator.insertOne(value, ConnectorConstant.WORKER_COLLECTION + "/health");
		if (isExit && AppType.currentType().isDaas()) {
			ConnectorManager.exit(instanceNo);
		}
	}

	protected void configIfNotBlank(String name, BiConsumer<String, String> setter) {
		String value = (String) configCenter.getConfig(name);
		if (StringUtils.isNotBlank(value)) {
			setter.accept(name, value);
		}
	}
}
