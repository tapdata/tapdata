package io.tapdata.flow.engine.V2.monitor.impl;

import com.tapdata.constant.ExecutorUtil;
import com.tapdata.entity.Connections;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.Runnable.LoadSchemaRunner;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.node.pdk.ConnectorNodeService;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connection.GetTableNamesFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.schema.TapTableMap;
import org.apache.commons.collections.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @author samuel
 * @Description
 * @create 2022-07-21 16:04
 **/
public class TableMonitor extends TaskMonitor<TableMonitor.TableResult> {
	public static final long AWAIT_SECOND = 10L;
	public static final long PERIOD_SECOND = 60L;
	public static final String TAG = TableMonitor.class.getSimpleName();
	public static final int BATCH_SIZE = 100;
	private TapTableMap<String, TapTable> tapTableMap;
	private String associateId;
	private ReentrantLock lock;
	private ScheduledExecutorService threadPool;
	private TableResult tableResult;
	private Set<String> removeTables;
	private Connections connections;

	private Predicate<String> dynamicTableFilter;

	public TableMonitor(TapTableMap<String, TapTable> tapTableMap, String associateId, TaskDto taskDto, Connections connections, Predicate<String> dynamicTableFilter) {
		super(taskDto);
		if (null == tapTableMap) {
			throw new RuntimeException("Missing Tap Table Map");
		}
		if (null == associateId) {
			throw new RuntimeException("Missing associate id");
		}
		this.tapTableMap = tapTableMap;
		this.associateId = associateId;
		this.connections = connections;
		this.lock = new ReentrantLock();
		this.threadPool = new ScheduledThreadPoolExecutor(1);
		this.tableResult = TableResult.create();
		this.removeTables = new HashSet<>();
		this.dynamicTableFilter = dynamicTableFilter;
		verify();
	}

	private void verify() {
		//TODO should use PDKInvocationMonitor.invoke(connectorNode, PDKMethod.GET_TABLE_NAMES,
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
			try {
				while (true) {
					try {
						if (lock.tryLock(1L, TimeUnit.SECONDS)) {
							break;
						}
					} catch (InterruptedException e) {
						break;
					}
				}
				LoadSchemaRunner.TableFilter tableFilter = LoadSchemaRunner.TableFilter.create(connections.getTable_filter(), connections.getIfOpenTableExcludeFilter());
				List<String> tapTableNames = new ArrayList<>(tapTableMap.keySet());
				tapTableNames = tapTableNames.stream().filter(name -> !removeTables.contains(name)).collect(Collectors.toList());
				GetTableNamesFunction getTableNamesFunction = connectorNode.getConnectorFunctions().getGetTableNamesFunction();
				if (null == getTableNamesFunction) {
					logger.warn("Connector [" + connectorNode.getConnectorContext().getSpecification().getName() + "] not support get table names function," +
							"start dynamic table monitor failed");
					this.close();
					return;
				}
				List<String> finalTapTableNames = tapTableNames;
				PDKInvocationMonitor.invoke(connectorNode, PDKMethod.GET_TABLE_NAMES,
						() -> getTableNamesFunction.tableNames(connectorNode.getConnectorContext(), BATCH_SIZE, dbTableNames -> Optional.ofNullable(dbTableNames)
								.ifPresent(names -> names.stream()
										.filter(tableFilter)
										.forEach(dbTableName -> {
													if (finalTapTableNames.contains(dbTableName) || !dynamicTableFilter.test(dbTableName)) {
														finalTapTableNames.remove(dbTableName);
														return;
													}
													tableResult.add(dbTableName);
													removeTables.remove(dbTableName);
												}
										)
								)
						), TAG);
				if (CollectionUtils.isNotEmpty(tapTableNames)) {
					tableResult.removeAll(tapTableNames);
					removeTables.addAll(tapTableNames);
				}
			} catch (Throwable throwable) {
				logger.warn("Found add/remove table failed, will retry next time, error: " + throwable.getMessage(), throwable);
			} finally {
				try {
					lock.unlock();
				} catch (Exception ignored) {
				}
			}
		}, 0L, PERIOD_SECOND, TimeUnit.SECONDS);
		logger.info("Dynamic table monitor started, interval: " + PERIOD_SECOND + " seconds");
	}

	@Override
	public void consume(Consumer<TableResult> consumer) {
		try {
			if (lock.tryLock(1L, TimeUnit.SECONDS)) {
				consumer.accept(tableResult);
				tableResult.clear();
			}
		} catch (InterruptedException ignored) {
		} finally {
			try {
				lock.unlock();
			} catch (Exception ignored) {
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
