package io.tapdata.flow.engine.V2.monitor.impl;

import com.tapdata.constant.ExecutorUtil;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.monitor.Monitor;
import io.tapdata.pdk.apis.functions.connection.GetTableNamesFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.monitor.PDKMethod;
import io.tapdata.schema.TapTableMap;
import org.apache.commons.collections.CollectionUtils;

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
	public static final long AWAIT_SECOND = 10L;
	public static final long PERIOD_MINUTES = 1L;
	public static final String TAG = TableMonitor.class.getSimpleName();
	public static final int BATCH_SIZE = 100;
	private TapTableMap<String, TapTable> tapTableMap;
	private ConnectorNode connectorNode;
	private ReentrantLock lock;
	private ScheduledExecutorService threadPool;
	private TableResult tableResult;

	public TableMonitor(TapTableMap<String, TapTable> tapTableMap, ConnectorNode connectorNode) {
		if (null == tapTableMap) {
			throw new RuntimeException("Missing Tap Table Map");
		}
		if (null == connectorNode) {
			throw new RuntimeException("Missing Connector Node");
		}
		this.tapTableMap = tapTableMap;
		this.connectorNode = connectorNode;
		this.lock = new ReentrantLock();
		this.threadPool = new ScheduledThreadPoolExecutor(1);
		this.tableResult = TableResult.create();
		verify();
	}

	private void verify() {
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
			Thread.currentThread().setName("Table-Monitor-" + connectorNode.getAssociateId());
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
			} finally {
				if (lock.isLocked()) {
					lock.unlock();
				}
			}
		}, 0L, PERIOD_MINUTES, TimeUnit.MINUTES);
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
			addList.add(tableName);
			return this;
		}

		public TableResult remove(String tableName) {
			removeList.add(tableName);
			return this;
		}

		public TableResult removeAll(List<String> tableNames) {
			removeList.addAll(tableNames);
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
