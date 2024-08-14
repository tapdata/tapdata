package io.tapdata.flow.engine.V2.monitor.impl;

import com.tapdata.constant.ExecutorUtil;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.entity.Connections;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.Runnable.LoadSchemaRunner;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.partition.TapPartition;
import io.tapdata.entity.schema.partition.TapSubPartitionTableInfo;
import io.tapdata.entity.utils.cache.Entry;
import io.tapdata.entity.utils.cache.Iterator;
import io.tapdata.flow.engine.V2.util.PartitionTableUtil;
import io.tapdata.node.pdk.ConnectorNodeService;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connection.GetTableNamesFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.schema.TapTableMap;
import org.apache.commons.collections.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
	protected TapTableMap<String, TapTable> tapTableMap;
	protected String associateId;
	protected ReentrantLock lock;
	protected ScheduledExecutorService threadPool;
	protected TableResult tableResult;
	protected Set<String> removeTables;
	protected Connections connections;

	protected Predicate<String> dynamicTableFilter;

	protected Boolean syncSourcePartitionTableEnable;

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

	public TableMonitor withSyncSourcePartitionTableEnable(Boolean syncSourcePartitionTableEnable) {
		this.syncSourcePartitionTableEnable = syncSourcePartitionTableEnable;
		return this;
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

	protected List<TapTable> partitionTableInfoSet(Set<String> masterTables, Set<String> existsSubTable, Map<String, Set<String>> parentTableAndSubIdMap) {
		Iterator<Entry<TapTable>> iterator = tapTableMap.iterator();
		List<TapTable> masterTapTables = new ArrayList<>();
		while (iterator.hasNext()) {
			Entry<TapTable> next = iterator.next();
			TapTable table = next.getValue();
			if (PartitionTableUtil.checkIsMasterPartitionTable(table)) {
				String id = next.getKey();
				masterTables.add(id);
				Set<String> subTableIds = new HashSet<>();
				Optional.ofNullable(table.getPartitionInfo())
						.map(TapPartition::getSubPartitionTableInfo)
						.ifPresent(schemas -> subTableIds.addAll(schemas.stream()
								.filter(Objects::nonNull)
								.map(TapSubPartitionTableInfo::getTableName)
								.collect(Collectors.toList()))
						);
				parentTableAndSubIdMap.put(id, subTableIds);
				existsSubTable.addAll(subTableIds);
				masterTapTables.add(table);
			}
		}
		return masterTapTables;
	}

	protected void loadSubTableByPartitionTable(ConnectorNode connectorNode,
												List<TapTable> masterTapTables,
												Map<String, Set<String>> parentTableAndSubIdMap,
												List<String> finalTapTableNames,
												Set<String> existsSubTable) {
		if (parentTableAndSubIdMap.isEmpty()) return;
		ConnectorFunctions connectorFunctions = connectorNode.getConnectorFunctions();
		Optional.ofNullable(connectorFunctions.getQueryPartitionTablesByParentName()).ifPresent(function ->
				PDKInvocationMonitor.invoke(connectorNode, PDKMethod.QUERY_PARTITION_TABLES_BY_PARENT_NAME, () -> {
					TapConnectorContext connectorContext = connectorNode.getConnectorContext();
					try {
						function.query(connectorContext, masterTapTables, partitionResult -> partitionResult.stream()
								.filter(Objects::nonNull)
								.filter(t -> parentTableAndSubIdMap.containsKey(t.getMasterTableName()))
								.filter(t -> Objects.nonNull(t.getSubPartitionTableNames()) && !t.getSubPartitionTableNames().isEmpty())
								.forEach(info -> {
									String masterTableName = info.getMasterTableName();
									finalTapTableNames.remove(masterTableName);
									Set<String> oldSubTableIds = parentTableAndSubIdMap.get(masterTableName);
									Set<String> masterTableId = masterTapTables.stream().map(TapTable::getId).collect(Collectors.toSet());
									// remove all sub table , and ignore drop sub table now
									removeTables.removeAll(info.getSubPartitionTableNames());
									List<String> newSubTable = info.getSubPartitionTableNames().stream()
											.filter(id -> !oldSubTableIds.contains(id))
											.filter(dbTableName -> filterTable(finalTapTableNames,
													masterTableId,
													existsSubTable,
													dbTableName))
											.collect(Collectors.toList());
									if(!newSubTable.isEmpty()) {
										tableResult.add(masterTableName);
									}
								})
						);
					} catch (Exception e) {
						logger.warn("Call QueryPartitionTablesByParentName function failed, will stop task after snapshot, errors: "
								+ e.getClass().getSimpleName() + "  " + e.getMessage() + "\n" + Log4jUtil.getStackString(e));
					}
				}, TAG)
		);
	}

	protected boolean filterTable(List<String> finalTapTableNames, Set<String> masterTables, Set<String> existsSubTable, String dbTableName) {
		if (finalTapTableNames.contains(dbTableName)
				|| existsSubTable.contains(dbTableName)
				|| !dynamicTableFilter.test(dbTableName)
				|| (null != syncSourcePartitionTableEnable && !syncSourcePartitionTableEnable && masterTables.contains(dbTableName))) {
			finalTapTableNames.remove(dbTableName);
			return false;
		}
		tableResult.add(dbTableName);
		removeTables.remove(dbTableName);
		return true;
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
				monitor(connectorNode);
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

	protected void monitor(ConnectorNode connectorNode) throws IOException {
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
		final Map<String, Set<String>> parentTableAndSubIdMap = new HashMap<>();
		final Set<String> masterTables = new HashSet<>(); //主表
		final Set<String> existsSubTable = new HashSet<>();//子表
		final List<TapTable> masterTapTables = partitionTableInfoSet(masterTables, existsSubTable, parentTableAndSubIdMap);

		List<String> finalTapTableNames = tapTableNames;
		PDKInvocationMonitor.invoke(connectorNode, PDKMethod.GET_TABLE_NAMES,
				() -> getTableNamesFunction.tableNames(connectorNode.getConnectorContext(), BATCH_SIZE, dbTableNames -> Optional.ofNullable(dbTableNames)
						.ifPresent(names -> names.stream()
								.filter(tableFilter)
								.forEach(dbTableName -> filterTable(finalTapTableNames, masterTables, existsSubTable, dbTableName))
						)
				), TAG);

		/**
		 * Dynamically add tables and load newly added sub tables based on the main table
		 * */
		if (Boolean.TRUE.equals(syncSourcePartitionTableEnable) && !masterTables.isEmpty()) {
			loadSubTableByPartitionTable(connectorNode, masterTapTables, parentTableAndSubIdMap, finalTapTableNames, existsSubTable);
		}

		if (CollectionUtils.isNotEmpty(tapTableNames)) {
			tableResult.removeAll(tapTableNames);
			removeTables.addAll(tapTableNames);
		}
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
