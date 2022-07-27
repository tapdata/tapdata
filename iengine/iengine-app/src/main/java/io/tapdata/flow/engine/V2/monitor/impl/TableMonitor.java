package io.tapdata.flow.engine.V2.monitor.impl;

import com.tapdata.constant.ExecutorUtil;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.tm.commons.task.dto.SubTaskDto;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.monitor.Monitor;
import io.tapdata.node.pdk.ConnectorNodeService;
import io.tapdata.pdk.apis.functions.connection.GetTableNamesFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.monitor.PDKMethod;
import io.tapdata.schema.TapTableMap;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * @author samuel
 * @Description
 * @create 2022-07-21 16:04
 **/
public class TableMonitor implements Monitor<TableMonitor.TableResult> {
	private final Logger logger = LogManager.getLogger(TableMonitor.class);
	public static final long AWAIT_SECOND = 10L;
	public static final long PERIOD_SECOND = 60L;
	public static final String TAG = TableMonitor.class.getSimpleName();
	public static final int BATCH_SIZE = 100;
	private TapTableMap<String, TapTable> tapTableMap;
	private String associateId;
	private ReentrantLock lock;
	private ScheduledExecutorService threadPool;
	private TableResult tableResult;
	private SubTaskDto subTaskDto;

	public TableMonitor(TapTableMap<String, TapTable> tapTableMap, String associateId, SubTaskDto subTaskDto) {
		if (null == tapTableMap) {
			throw new RuntimeException("Missing Tap Table Map");
		}
		if (null == associateId) {
			throw new RuntimeException("Missing associate id");
		}
		this.tapTableMap = tapTableMap;
		this.associateId = associateId;
		this.subTaskDto = subTaskDto;
		this.lock = new ReentrantLock();
		this.threadPool = new ScheduledThreadPoolExecutor(1);
		this.tableResult = TableResult.create();
		verify();
	}

	private void verify() {
		ConnectorNode connectorNode = ConnectorNodeService.getInstance().getConnectorNode(associateId);
		GetTableNamesFunction getTableNamesFunction = connectorNode.getConnectorFunctions().getGetTableNamesFunction();
		if (null == getTableNamesFunction) {
			throw new RuntimeException("Connector node: " + connectorNode + " unsupported getTableNamesFunction");
		}
	}

	@Override
	public void close() throws IOException {
		ExecutorUtil.shutdown(threadPool, AWAIT_SECOND, TimeUnit.SECONDS);
	}

	@Override
	public void start() {
		threadPool.scheduleAtFixedRate(() -> {
			ConnectorNode connectorNode = ConnectorNodeService.getInstance().getConnectorNode(associateId);
			Thread.currentThread().setName("Table-Monitor-" + connectorNode.getAssociateId());
			Log4jUtil.setThreadContext(subTaskDto);
			try {
				while (true) {
					try {
						if (lock.tryLock(5L, TimeUnit.SECONDS)) {
							break;
						}
					} catch (InterruptedException e) {
						break;
					}
				}
				List<String> tapTableNames = new ArrayList<>(tapTableMap.keySet());
				GetTableNamesFunction getTableNamesFunction = connectorNode.getConnectorFunctions().getGetTableNamesFunction();
				PDKInvocationMonitor.invoke(connectorNode, PDKMethod.GET_TABLE_NAMES,
						() -> getTableNamesFunction.tableNames(connectorNode.getConnectorContext(), BATCH_SIZE, dbTableNames -> {
							if (null == dbTableNames) {
								return;
							}
							for (String dbTableName : dbTableNames) {
								if (tapTableNames.contains(dbTableName)) {
									tapTableNames.remove(dbTableName);
									continue;
								}
								tableResult.add(dbTableName);
							}
						}), TAG);
				if (CollectionUtils.isNotEmpty(tapTableNames)) {
					tableResult.removeAll(tapTableNames);
				}
			} catch (Throwable throwable) {
				logger.warn("Found add/remove table failed, will retry next time, error: " + throwable.getMessage(), throwable);
			} finally {
				if (lock.isLocked()) {
					lock.unlock();
				}
			}
		}, 0L, PERIOD_SECOND, TimeUnit.SECONDS);
		logger.info("Dynamic table monitor started, interval: "+PERIOD_SECOND+" seconds");
	}

	@Override
	public void consume(Consumer<TableResult> consumer) {
		try {
			if (lock.tryLock(10L, TimeUnit.SECONDS)) {
				consumer.accept(tableResult);
				tableResult.clear();
			}
		} catch (InterruptedException ignored) {
		} finally {
			if (lock.isLocked()) {
				lock.unlock();
			}
		}
	}

	public static class TableResult {
		private List<String> addList;
		private List<String> removeList;

		private TableResult() {
		}

		public static TableResult create() {
			TableResult tableResult = new TableResult();
			tableResult.addList = new ArrayList<>();
			tableResult.removeList = new ArrayList<>();
			return tableResult;
		}

		public TableResult add(String tableName) {
			if (!addList.contains(tableName)) {
				addList.add(tableName);
			}
			return this;
		}

		public TableResult remove(String tableName) {
			removeList.add(tableName);
			return this;
		}

		public TableResult removeAll(List<String> tableNames) {
			for (String tableName : tableNames) {
				if (!removeList.contains(tableName)) {
					removeList.add(tableName);
				}
			}
			return this;
		}

		public TableResult clear() {
			addList.clear();
			removeList.clear();
			return this;
		}

		public List<String> getAddList() {
			return addList;
		}

		public List<String> getRemoveList() {
			return removeList;
		}
	}
}
