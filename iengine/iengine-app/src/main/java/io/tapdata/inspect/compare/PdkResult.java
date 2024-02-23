package io.tapdata.inspect.compare;

import cn.hutool.core.map.MapUtil;
import com.tapdata.entity.Connections;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.error.TaskInspectExCode_27;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.exception.node.NodeException;
import io.tapdata.pdk.apis.entity.*;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.source.BatchCountFunction;
import io.tapdata.pdk.apis.functions.connector.source.CountByPartitionFilterFunction;
import io.tapdata.pdk.apis.functions.connector.source.ExecuteCommandFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author samuel
 * @Description
 * @create 2022-06-06 17:39
 **/
public class PdkResult extends BaseResult<Map<String, Object>> {
	private final static Logger logger = LogManager.getLogger(PdkResult.class);

	private static final int BATCH_SIZE = 1000;
	private static final String TAG = PdkResult.class.getSimpleName();
	private final ConnectorNode connectorNode;
	private final LinkedBlockingQueue<Map<String, Object>> queue = new LinkedBlockingQueue<>(100);
	private final List<SortOn> sortOnList = new LinkedList<>();
	private final QueryByAdvanceFilterFunction queryByAdvanceFilterFunction;
	private final TapTable tapTable;
	private Throwable throwable;
	private final AtomicBoolean hasNext;
	private final AtomicBoolean running;
	private final TapCodecsFilterManager codecsFilterManager;
	private final TapCodecsFilterManager defaultCodecsFilterManager;
	private Projection projection;
	private int diffKeyIndex;
	private final List<String> dataKeys;
	private final List<List<Object>> diffKeyValues;
	private final AtomicReference<Thread> queryThreadAR = new AtomicReference<>();
	private final List<QueryOperator> conditions;
	private final AtomicBoolean firstTimeRead = new AtomicBoolean();
	private final boolean enableCustomCommand;
	private final Map<String, Object> customCommand;

	private final  ExecuteCommandFunction executeCommandFunction;

	public static final String PARAMS = "params";
	public PdkResult(List<String> sortColumns, Connections connections, String tableName, Set<String> columns, ConnectorNode connectorNode, boolean fullMatch, List<String> dataKeys, List<List<Object>> diffKeyValues, List<QueryOperator> conditions,
					 boolean enableCustomCommand,Map<String, Object> customCommand) {
		super(sortColumns, connections, tableName);
		this.connectorNode = connectorNode;
		this.enableCustomCommand = enableCustomCommand;
		this.customCommand = customCommand;
		for (String sortColumn : sortColumns) {
			sortOnList.add(SortOn.ascending(sortColumn));
		}
		this.executeCommandFunction = connectorNode.getConnectorFunctions().getExecuteCommandFunction();
		this.queryByAdvanceFilterFunction = connectorNode.getConnectorFunctions().getQueryByAdvanceFilterFunction();

		supportFunction();

		this.tapTable = connectorNode.getConnectorContext().getTableMap().get(tableName);
		if (null == tapTable) {
			throw new TapCodeException(TaskInspectExCode_27.TABLE_NO_EXISTS, "Table '" + connections.getName() + "'.'" + tableName + "' not exists.");
		}
		this.hasNext = new AtomicBoolean(true);
		this.running = new AtomicBoolean(true);
		this.codecsFilterManager = connectorNode.getCodecsFilterManager();
		this.defaultCodecsFilterManager = TapCodecsFilterManager.create(TapCodecsRegistry.create());
		assignProjection(fullMatch,sortColumns,columns);
		this.dataKeys = dataKeys;
		this.diffKeyValues = diffKeyValues;
		this.conditions = conditions;

		if (null != diffKeyValues && !diffKeyValues.isEmpty()) {
			Map<String, Object> keyMap;
			for (List<Object> keyValues : diffKeyValues) {
				if (dataKeys.size() != keyValues.size()) {
					throw new TapCodeException(TaskInspectExCode_27.PARAM_ERROR,
							String.format("The key name size and value size not equals, keys: %s, values: %s", dataKeys, keyValues));
				}

				keyMap = new LinkedHashMap<>();

				handleDataKeys(keyMap,keyValues);

				keyValues.clear();
				keyValues.addAll(keyMap.values());
			}
		}
		initTotal();
	}

	private void supportFunction(){
		if(enableCustomCommand && MapUtil.isNotEmpty(customCommand)){
			if (null == executeCommandFunction) {
				throw new TapCodeException(TaskInspectExCode_27.CONNECTOR_NOT_SUPPORT_FUNCTION,
						"Connector does not support customCommand function: " + connectorNode.getConnectorContext().getSpecification().getId());
			}
		}else {
			if (null == queryByAdvanceFilterFunction) {
				throw new TapCodeException(TaskInspectExCode_27.CONNECTOR_NOT_SUPPORT_FUNCTION
						,"Connector does not support query by filter function: " + connectorNode.getConnectorContext().getSpecification().getId());
			}
		}
	}

	private void assignProjection(boolean fullMatch, List<String> sortColumns, Set<String> columns) {
		if (!fullMatch) {
			projection = new Projection();
			sortColumns.forEach(projection::include);
		} else if (null != columns && !columns.isEmpty()) {
			projection = new Projection();
			sortColumns.forEach(projection::include);
			columns.forEach(s -> {
				if (!sortColumns.contains(s)) projection.include(s);
			});
		} else {
			projection = null;
		}

	}

	private void handleDataKeys(Map<String, Object> keyMap,List<Object> keyValues){
		int i =0;
		for (String s : dataKeys) {
			keyMap.put(s, keyValues.get(i++));

			TapField tapField = tapTable.getNameFieldMap().get(s);
			if (null != tapField.getDataType()) {
				switch (tapField.getDataType()) {
					case "OBJECT_ID":
						try {
							Class<?> clz = connectorNode.getConnectorClassLoader().loadClass("org.bson.types.ObjectId");
							Constructor<?> constructor = clz.getConstructor(String.class);
							keyMap.put(s, constructor.newInstance(keyMap.get(s)));
						} catch (Exception e) {
							logger.warn("Convert filed '{}' value '{}' failed: {}", s, keyMap.get(s), e.getMessage());
						}
						break;
					default:
						break;
				}
			}
		}
	}

	private void initTotal() {
		if (null == diffKeyValues) {
			ConnectorFunctions connectorFunctions = connectorNode.getConnectorFunctions();
			BatchCountFunction batchCountFunction = connectorFunctions.getBatchCountFunction();
			CountByPartitionFilterFunction countByPartitionFilterFunction = connectorFunctions.getCountByPartitionFilterFunction();
			ExecuteCommandFunction executeCommand = connectorFunctions.getExecuteCommandFunction();

			if (judgeExistFunction(batchCountFunction,countByPartitionFilterFunction,executeCommand)) {
				total = 0L;
				return;
			}
			if(enableCustomCommand && MapUtil.isNotEmpty(customCommand) && executeCommand !=null){
				customCommandCount();
			}else if (null != countByPartitionFilterFunction) {
				TapAdvanceFilter tapAdvanceFilter = TapAdvanceFilter.create();
				tapAdvanceFilter.setOperators(conditions);
				DataMap match = new DataMap();
				if (null != conditions) {
					conditions.stream().filter(op -> op.getOperator() == 5).forEach(op -> match.put(op.getKey(), op.getValue()));
				}
				tapAdvanceFilter.match(match);
				PDKInvocationMonitor.invoke(connectorNode, PDKMethod.COUNT_BY_PARTITION_FILTER,
						() -> total = countByPartitionFilterFunction.countByPartitionFilter(connectorNode.getConnectorContext(), tapTable, tapAdvanceFilter), TAG);
			} else {
				PDKInvocationMonitor.invoke(connectorNode, PDKMethod.SOURCE_BATCH_COUNT,
						() -> total = batchCountFunction.count(connectorNode.getConnectorContext(), tapTable), TAG);
			}
		} else {
			total = diffKeyValues.size();
		}
	}

	private void customCommandCount(){
		Map<String, Object> customCountCommand = TableRowCountInspectJob.setCommandCountParam(customCommand,connectorNode,tapTable);
		TapExecuteCommand tapExecuteCommand = TapExecuteCommand.create()
				.command((String) customCountCommand.get("command")).params((Map<String, Object>) customCountCommand.get(PARAMS));
		List<Map<String, Object>> maps = TableRowCountInspectJob.executeCommand(executeCommandFunction,tapExecuteCommand,connectorNode);
		if (CollectionUtils.isNotEmpty(maps)) {
			total = maps.get(0).values().stream().mapToLong(value -> Long.parseLong(value.toString())).sum();
		}else {
			total =0L;
		}
	}
	private boolean judgeExistFunction(BatchCountFunction batchCountFunction,
									   CountByPartitionFilterFunction countByPartitionFilterFunction,
									   ExecuteCommandFunction executeCommandFunction) {
		if ((null == batchCountFunction && null == countByPartitionFilterFunction && executeCommandFunction == null)
				|| (CollectionUtils.isNotEmpty(conditions) && null == countByPartitionFilterFunction)
				|| (enableCustomCommand && MapUtil.isNotEmpty(customCommand) && executeCommandFunction == null)) {
			return true;
		}
		return false;

	}

	public static void setCommandQueryParam(Map<String, Object> customCommand, ConnectorNode connectorNode, TapTable table,
											List<SortOn> sortOnList, Projection projection) {
		try {
			Map<String, Object> params = (Map<String, Object>) customCommand.get(PARAMS);
			if (!connectorNode.getTapNodeInfo().getTapNodeSpecification().getId().contains("mongodb")) {
				Object value = params.get("sql");
				if (value != null) {
					String sql = getSelectSql(value.toString(), sortOnList);
					params.put("sql", sql);
				}
			} else {
				params.put("collection", table.getId());
				if (projection == null) {
					params.put("projection", null);
				}else {
					Map<String, Object> projectInclude = new LinkedHashMap<>();
					projection.getIncludeFields().forEach(include->{
						projectInclude.put(include,1);
					});
					params.put("projection", projectInclude);
				}
				Map<String, Object> sortMap = new LinkedHashMap<>();
				sortOnList.forEach(sortOn -> {
					sortMap.put(sortOn.getKey(), 1);
				});
				params.put("sort", sortMap);
			}
		} catch (Exception e) {
			throw new TapCodeException(TaskInspectExCode_27.PARAM_ERROR,
					"SetCommandQueryParam error: " + e.getMessage()+" customCommand : "+customCommand);
		}
	}
	private static String getSelectSql(String customSql,List<SortOn> sortOnList) {
		String sql = customSql.trim().replaceAll("[\t\n\r]", "");
		Pattern orderByPattern = Pattern.compile(".+[Oo][Rr][Dd][Ee][Rr]\\s[Bb][Yy].+");
		if (!orderByPattern.matcher(sql).matches()) {
			StringBuilder builder = new StringBuilder();
			builder.append("  ORDER BY ");
			char escapeChar = '"';
			builder.append(sortOnList.stream().map(v -> v.toString(String.valueOf(escapeChar))).collect(Collectors.joining(", "))).append(' ');
			sql =  sql + builder;
		}
		return sql;
	}

	@Override
	long getTotal() {
		return total;
	}

	@Override
	long getPointer() {
		return pointer;
	}

	@Override
	public void close() throws IOException {
		running.set(false);
	}

	@Override
	public boolean hasNext() {
		if (null != throwable) throw new RuntimeException(throwable);
		offerInQueue();
		return null != queryThreadAR.get() || queue.size() > 0;
	}

	@Override
	public Map<String, Object> next() {
		while (isRunning() && hasNext()) {
			try {
				Map<String, Object> poll = queue.poll(100L, TimeUnit.MILLISECONDS);
				if (null != poll) {
					pointer++;
					codecsFilterManager.transformToTapValueMap(poll, tapTable.getNameFieldMap());
					defaultCodecsFilterManager.transformFromTapValueMap(poll);
					return poll;
				}
			} catch (InterruptedException e) {
				break;
			}
		}
		return null;
	}

	private void offerInQueue() {
		if (needQuery()) {
			queryNextBatch();
		}
	}

	private boolean needQuery() {
		return hasNext.get() && null == queryThreadAR.get();
	}

	private void queryNextBatch() {
		synchronized (queryThreadAR) {
			if (null == queryThreadAR.get()) {
				queryThreadAR.set(new Thread(() -> {
					Thread.currentThread().setName(String.format("INSPECT-QUERY-%s.%s", connections.getId(), tableName));
					try {
						TapAdvanceFilter tapAdvanceFilter = TapAdvanceFilter.create();

						// query one difference data, because pdk api not support 'or' conditions.
						if (null != diffKeyValues && diffKeyIndex < diffKeyValues.size()) {
							List<Object> objects = diffKeyValues.get(diffKeyIndex);
							if (objects.size() != dataKeys.size()) {
								throw new RuntimeException("The data key size not equals data value size: " + tapTable.getId());
							}
							for (int i = 0; i < dataKeys.size(); i++) {
								tapAdvanceFilter.match(DataMap.create().kv(dataKeys.get(i), objects.get(i)));
							}
							diffKeyIndex++;
						}

//						tapAdvanceFilter.setLimit(BATCH_SIZE); // can not add limit because queryByAdvanceFilterFunction not support 'or' conditions
						tapAdvanceFilter.setSortOnList(sortOnList);
						tapAdvanceFilter.setProjection(projection);
						tapAdvanceFilter.setOperators(conditions);
						DataMap match = new DataMap();
						if (null != conditions) {
							conditions.stream().filter(op -> op.getOperator() == 5).forEach(op -> match.put(op.getKey(), op.getValue()));
						}
						if (MapUtil.isEmpty(tapAdvanceFilter.getMatch())) {
							tapAdvanceFilter.match(match);
						}
						if (firstTimeRead.compareAndSet(false, true)) {
							logger.info("Inspect job[{}] read data from table '{}' by filter: {}", connections.getName(), tableName, tapAdvanceFilter);
						}
						if (enableCustomCommand && MapUtil.isNotEmpty(customCommand) && CollectionUtils.isEmpty(diffKeyValues)) {
							setCommandQueryParam(customCommand,connectorNode,tapTable,sortOnList,projection);
							TapExecuteCommand tapExecuteCommand = TapExecuteCommand.create()
									.command((String) customCommand.get("command")).params((Map<String, Object>) customCommand.get(PARAMS));
							executeQueryCommand(tapExecuteCommand);
						}else {
							tapAdvanceFilter(tapAdvanceFilter);
						}
						if (null == diffKeyValues || diffKeyIndex >= diffKeyValues.size()) {
							hasNext.set(false);
						}
					} catch (Exception e) {
						throwable = e;
					} finally {
						synchronized (queryThreadAR) {
							queryThreadAR.set(null);
						}
					}
				}));
				queryThreadAR.get().start();
			}
		}
	}

	public void tapAdvanceFilter(TapAdvanceFilter tapAdvanceFilter){
		PDKInvocationMonitor.invoke(connectorNode, PDKMethod.SOURCE_QUERY_BY_ADVANCE_FILTER,
				() -> queryByAdvanceFilterFunction.query(connectorNode.getConnectorContext(), tapAdvanceFilter, tapTable, filterResults -> {
					Throwable error = filterResults.getError();
					if (null != error) throwable = error;

					List<Map<String, Object>> results = filterResults.getResults();
					if (CollectionUtils.isEmpty(results)) return;

					handleQueryData(results);

				}), TAG);


	}

	public void executeQueryCommand(TapExecuteCommand tapExecuteCommand) {
		PDKInvocationMonitor.invoke(connectorNode, PDKMethod.SOURCE_QUERY_BY_ADVANCE_FILTER,
				() -> executeCommandFunction.execute(connectorNode.getConnectorContext(), tapExecuteCommand, executeResult -> {
					if (executeResult.getError() != null) {
						throw new NodeException("Execute error: " + executeResult.getError().getMessage(), executeResult.getError());
					}

					List<Map<String, Object>> results = (List<Map<String, Object>>) executeResult.getResult();
					if (CollectionUtils.isEmpty(results)) {
						return;
					}
					handleQueryData(results);

				}), TAG);
	}

    private void handleQueryData(List<Map<String, Object>> results){
		for (Map<String, Object> result : results) {
			if (!isRunning()) break;
			while (isRunning()) {
				try {
					if (queue.offer(result, 100L, TimeUnit.MILLISECONDS)) {
						break;
					}
				} catch (InterruptedException e) {
					logger.warn("Query data has Interrupted");
					Thread.currentThread().interrupt();
					return;
				}
			}
		}
	}


	private boolean isRunning() {
		return running.get() && !Thread.currentThread().isInterrupted();
	}

	public LinkedBlockingQueue<Map<String, Object>> getQueue() {
		return queue;
	}
}
