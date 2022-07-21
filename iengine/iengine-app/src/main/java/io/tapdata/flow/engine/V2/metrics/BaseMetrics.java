package io.tapdata.flow.engine.V2.metrics;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.metrics.Metrics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2021-12-23 18:26
 **/
public abstract class BaseMetrics implements Closeable {

	private static final Logger logger = LogManager.getLogger(BaseMetrics.class);

	public static final Long METRICS_INTERVAL = 5 * 1000L;
	protected static final String SUB_TASK_METRICS_PREFIX = "sub_task_";
	protected static final String SUB_TASK_NODE_METRICS_PREFIX = "sub_task_node_";
	protected static final int BATCH_SIZE = 10;
	protected static final String IMAP_NAME = "TaskNodeMetrics";

	protected ClientMongoOperator clientMongoOperator;
	protected List<Metrics> metricsList;
	protected HazelcastInstance hazelcastInstance;
	protected ConfigurationCenter configurationCenter;

	public BaseMetrics(ClientMongoOperator clientMongoOperator, ConfigurationCenter configurationCenter, HazelcastInstance hazelcastInstance) {
		checkInput(clientMongoOperator, configurationCenter);
		this.clientMongoOperator = clientMongoOperator;
		this.configurationCenter = configurationCenter;
		this.metricsList = new ArrayList<>();
		this.hazelcastInstance = hazelcastInstance;
	}

	protected void insertMetrics(Metrics metrics) {
		if (metrics == null) {
			throw new NullPointerException("Metrics cannot be null");
		}
		metricsList.add(metrics);
		if (metricsList.size() == BATCH_SIZE) {
			insertMetrics();
		}
	}

	protected void insertMetrics() {
		if (metricsList == null) {
			throw new NullPointerException("Metrics list cannot be null");
		}
		try {
			clientMongoOperator.insertMany(metricsList, ConnectorConstant.METRICS_COLLECTION + "/batch");
			metricsList.clear();
		} catch (Exception e) {
			throw new RuntimeException("Call insert metrics rest api failed; Request body: " + metricsList + "; " + e.getMessage(), e);
		}
	}

	private void checkInput(ClientMongoOperator clientMongoOperator, ConfigurationCenter configurationCenter) {
		if (clientMongoOperator == null) {
			throw new IllegalArgumentException("Client mongo operator cannot be null");
		}
		if (configurationCenter == null) {
			throw new IllegalArgumentException("Configure center cannot be null");
		}
	}

	protected void errorLog(Exception e) {
		logger.error(e.getMessage() + ";\nStack trace:\n" + Log4jUtil.getStackString(e));
	}

	protected IMap<String, Map<String, Map<String, Metrics>>> getSubTaskNodeMetricsMap() {
		return hazelcastInstance.getMap(IMAP_NAME);
	}

	@Override
	public void close() {
	}

	abstract public void doStats();

	public enum NodeType {
		SOURCE,
		TARGET,
		;
	}
}
