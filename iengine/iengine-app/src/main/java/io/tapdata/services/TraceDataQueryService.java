package io.tapdata.services;

import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectionUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.flow.engine.V2.entity.EmptyMap;
import io.tapdata.flow.engine.V2.task.impl.HazelcastTaskService;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.pdk.apis.entity.ExecuteResult;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.entity.TapExecuteCommand;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.source.ExecuteCommandFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.schema.EmptyTapTableMap;
import io.tapdata.schema.TapTableUtil;
import io.tapdata.service.skeleton.annotation.RemoteService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

@RemoteService
@Slf4j
public class TraceDataQueryService {

	private static final int DEFAULT_LIMIT = 10;
	private static final int DEFAULT_BATCH_SIZE = 10;
	private static final String TAG = TraceDataQueryService.class.getSimpleName();

	private final ObjectMapper objectMapper = new ObjectMapper();
	private final TapCodecsFilterManager originCodecsFilterManager;

	public TraceDataQueryService() {
		TapCodecsRegistry codecsRegistry = TapCodecsRegistry.create();
		codecsRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue ->
				formatTapDateTime(tapDateTimeValue.getValue(), "yyyy-MM-dd HH:mm:ss"));
		originCodecsFilterManager = TapCodecsFilterManager.create(codecsRegistry);
	}

	public List<Map<String, Object>> query(String connectionId, String tableName, String sql,
										   List<Map<String, Object>> filters, Boolean sqlMode,
										   Integer limit, Integer batchSize,
										   List<QueryOperator> queryOperators,
										   Map<String, Object> executeParams) {
		if (StringUtils.isAnyBlank(connectionId, tableName)) {
			return Collections.emptyList();
		}
		String associateId = "trace_query_" + connectionId + "_" + UUID.randomUUID();
		ConnectorNode connectorNode = null;
		try {
			ClientMongoOperator clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
			Connections connections = HazelcastTaskService.taskService().getConnection(connectionId);
			DatabaseTypeEnum.DatabaseType databaseType = ConnectionUtil.getDatabaseType(clientMongoOperator, connections.getPdkHash());
			TapTable tapTable = TapTableUtil.getTapTableByConnectionId(connectionId, tableName);
			connectorNode = createConnectorNode(associateId, (HttpClientMongoOperator) clientMongoOperator, databaseType, connections.getConfig());
			PDKInvocationMonitor.invoke(connectorNode, PDKMethod.INIT, connectorNode::connectorInit, TAG);
			if (Boolean.TRUE.equals(sqlMode) || StringUtils.isNotBlank(sql)) {
				return queryByExecuteCommand(connectorNode, tapTable, tableName, sql, filters, queryOperators, limit, batchSize, executeParams);
			}
			return queryByAdvanceFilter(connectorNode, tapTable, filters, queryOperators, limit, batchSize);
		} catch (Throwable e) {
			log.error("Trace data query failed, connectionId: {}, tableName: {}", connectionId, tableName, e);
			return Collections.emptyList();
		} finally {
			if (connectorNode != null) {
				try {
					PDKInvocationMonitor.invoke(connectorNode, PDKMethod.STOP, connectorNode::connectorStop, TAG);
				} catch (Exception e) {
					log.warn("Stop trace query connector failed, associateId: {}, message: {}", associateId, e.getMessage());
				}
			}
			PDKIntegration.releaseAssociateId(associateId);
		}
	}

	private List<Map<String, Object>> queryByExecuteCommand(ConnectorNode connectorNode, TapTable tapTable, String tableName,
															String sql, List<Map<String, Object>> filters,
															List<QueryOperator> queryOperators, Integer limit,
															Integer batchSize, Map<String, Object> executeParams) throws Throwable {
		ExecuteCommandFunction executeCommandFunction = connectorNode.getConnectorFunctions().getExecuteCommandFunction();
		if (executeCommandFunction == null) {
			log.warn("Trace query connector does not support executeCommand");
			return Collections.emptyList();
		}

		Map<String, Object> params = buildMongoExecuteParams(tableName, sql, filters, queryOperators, limit, batchSize, executeParams);
		AtomicReference<List<Map<String, Object>>> resultsRef = new AtomicReference<>(new ArrayList<>());
		AtomicReference<Throwable> errorRef = new AtomicReference<>();
		TapExecuteCommand command = TapExecuteCommand.create()
				.command("executeQuery")
				.params(params);
		executeCommandFunction.execute(connectorNode.getConnectorContext(), command, result -> {
			if (result == null) {
				return;
			}
			if (result.getError() != null) {
				errorRef.set(result.getError());
				return;
			}
			appendResult(resultsRef.get(), result);
		});
		if (errorRef.get() != null) {
			throw new IllegalStateException("Execute trace mongo query failed", errorRef.get());
		}
		return transformResults(connectorNode, tapTable, resultsRef.get());
	}

	private List<Map<String, Object>> queryByAdvanceFilter(ConnectorNode connectorNode, TapTable tapTable,
														   List<Map<String, Object>> filters,
														   List<QueryOperator> queryOperators, Integer limit,
														   Integer batchSize) throws Throwable {
		QueryByAdvanceFilterFunction queryFunction = connectorNode.getConnectorFunctions().getQueryByAdvanceFilterFunction();
		if (queryFunction == null || tapTable == null) {
			log.warn("Trace query connector does not support queryByAdvanceFilter or tapTable is null");
			return Collections.emptyList();
		}

		AtomicReference<List<Map<String, Object>>> resultsRef = new AtomicReference<>(new ArrayList<>());
		for (Map<String, Object> filter : normalizeFilters(filters)) {
			TapAdvanceFilter tapAdvanceFilter = TapAdvanceFilter.create()
					.limit(resolveLimit(limit))
					.batchSize(resolveBatchSize(batchSize));
			if (MapUtils.isNotEmpty(filter)) {
				tapAdvanceFilter.match(DataMap.create(new LinkedHashMap<>(filter)));
			}
			if (CollectionUtils.isNotEmpty(queryOperators)) {
				queryOperators.stream()
						.filter(Objects::nonNull)
						.forEach(tapAdvanceFilter::op);
			}
			queryFunction.query(connectorNode.getConnectorContext(), tapAdvanceFilter, tapTable, filterResults -> {
				if (filterResults != null && CollectionUtils.isNotEmpty(filterResults.getResults())) {
					resultsRef.get().addAll(filterResults.getResults());
				}
			});
		}
		return transformResults(connectorNode, tapTable, resultsRef.get());
	}

	private Map<String, Object> buildMongoExecuteParams(String tableName, String sql, List<Map<String, Object>> filters,
														List<QueryOperator> queryOperators, Integer limit,
														Integer batchSize, Map<String, Object> executeParams) {
		Map<String, Object> params = new LinkedHashMap<>();
		if (MapUtils.isNotEmpty(executeParams)) {
			params.putAll(executeParams);
		}
		params.putIfAbsent("collection", tableName);
		Object filter = params.get("filter");
		if (filter instanceof Map) {
			List<Map<String, Object>> mergedFilters = buildMongoFilters((Map<String, Object>) filter, filters, queryOperators);
			params.put("filter", mergedFilters.size() == 1 ? mergedFilters.get(0) : buildOrFilter(mergedFilters));
		} else if (filter == null) {
			List<Map<String, Object>> mergedFilters = buildMongoFilters(parseSqlFilter(sql), filters, queryOperators);
			params.put("filter", mergedFilters.size() == 1 ? mergedFilters.get(0) : buildOrFilter(mergedFilters));
		}
		params.putIfAbsent("limit", resolveLimit(limit));
		params.putIfAbsent("batchSize", resolveBatchSize(batchSize));
		return params;
	}

	private List<Map<String, Object>> buildMongoFilters(Map<String, Object> baseFilter,
														List<Map<String, Object>> filters,
														List<QueryOperator> queryOperators) {
		List<Map<String, Object>> mergedFilters = new ArrayList<>();
		for (Map<String, Object> filter : normalizeFilters(filters)) {
			Map<String, Object> mergedFilter = new LinkedHashMap<>();
			if (MapUtils.isNotEmpty(baseFilter)) {
				mergedFilter.putAll(baseFilter);
			}
			if (MapUtils.isNotEmpty(filter)) {
				mergedFilter.putAll(filter);
			}
			appendQueryOperators(mergedFilter, queryOperators);
			mergedFilters.add(mergedFilter);
		}
		return mergedFilters;
	}

	private Map<String, Object> buildOrFilter(List<Map<String, Object>> filters) {
		Map<String, Object> filter = new LinkedHashMap<>();
		filter.put("$or", filters);
		return filter;
	}

	private List<Map<String, Object>> normalizeFilters(List<Map<String, Object>> filters) {
		if (CollectionUtils.isEmpty(filters)) {
			return Collections.singletonList(Collections.emptyMap());
		}
		List<Map<String, Object>> normalized = filters.stream()
				.filter(MapUtils::isNotEmpty)
				.map(LinkedHashMap::new)
				.collect(java.util.stream.Collectors.toList());
		return normalized.isEmpty() ? Collections.singletonList(Collections.emptyMap()) : normalized;
	}

	private void appendQueryOperators(Map<String, Object> filter, List<QueryOperator> queryOperators) {
		if (filter == null || CollectionUtils.isEmpty(queryOperators)) {
			return;
		}
		for (QueryOperator queryOperator : queryOperators) {
			if (queryOperator == null || StringUtils.isBlank(queryOperator.getKey())) {
				continue;
			}
			String mongoOperator = toMongoOperator(queryOperator.getOperator());
			if (StringUtils.isBlank(mongoOperator)) {
				filter.put(queryOperator.getKey(), queryOperator.getValue());
				continue;
			}
			Object value = filter.get(queryOperator.getKey());
			Map<String, Object> range;
			if (value instanceof Map) {
				range = (Map<String, Object>) value;
			} else {
				range = new LinkedHashMap<>();
				filter.put(queryOperator.getKey(), range);
			}
			range.put(mongoOperator, queryOperator.getValue());
		}
	}

	private String toMongoOperator(int operator) {
		switch (operator) {
			case QueryOperator.GT:
				return "$gt";
			case QueryOperator.GTE:
				return "$gte";
			case QueryOperator.LT:
				return "$lt";
			case QueryOperator.LTE:
				return "$lte";
			default:
				return null;
		}
	}

	private Map<String, Object> parseSqlFilter(String sql) {
		if (StringUtils.isBlank(sql)) {
			return new LinkedHashMap<>();
		}
		try {
			return objectMapper.readValue(sql, new TypeReference<Map<String, Object>>() {});
		} catch (Exception jacksonError) {
			return JSON.parseObject(sql, LinkedHashMap.class);
		}
	}

	private ConnectorNode createConnectorNode(String associateId, HttpClientMongoOperator clientMongoOperator,
											  DatabaseTypeEnum.DatabaseType databaseType,
											  Map<String, Object> connectionConfig) {
		try {
			PdkUtil.downloadPdkFileIfNeed(clientMongoOperator,
					databaseType.getPdkHash(), databaseType.getJarFile(), databaseType.getJarRid());
			PDKIntegration.ConnectorBuilder<ConnectorNode> connectorBuilder = PDKIntegration.createConnectorBuilder()
					.withDagId(associateId)
					.withAssociateId(associateId)
					.withConfigContext(null)
					.withGroup(databaseType.getGroup())
					.withVersion(databaseType.getVersion())
					.withPdkId(databaseType.getPdkId())
					.withTableMap(new EmptyTapTableMap())
					.withStateMap(new EmptyMap())
					.withGlobalStateMap(new EmptyMap());
			if (MapUtils.isNotEmpty(connectionConfig)) {
				connectorBuilder.withConnectionConfig(DataMap.create(connectionConfig));
			}
			return connectorBuilder.build();
		} catch (Exception e) {
			throw new RuntimeException("Failed to create trace query connector node, database type: " + databaseType + ", message: " + e.getMessage(), e);
		}
	}

	private List<Map<String, Object>> transformResults(ConnectorNode connectorNode, TapTable tapTable,
													   List<Map<String, Object>> results) {
		if (CollectionUtils.isEmpty(results)) {
			return Collections.emptyList();
		}
		List<Map<String, Object>> transformed = new ArrayList<>(results.size());
		TapCodecsFilterManager codecsFilterManager = connectorNode.getCodecsFilterManager();
		for (Map<String, Object> result : results) {
			if (result == null) {
				continue;
			}
			Map<String, Object> row = new LinkedHashMap<>(result);
			if (tapTable != null && MapUtils.isNotEmpty(tapTable.getNameFieldMap())) {
				codecsFilterManager.transformToTapValueMap(row, tapTable.getNameFieldMap());
				originCodecsFilterManager.transformFromTapValueMap(row);
			}
			transformed.add(row);
		}
		return transformed;
	}

	private void appendResult(List<Map<String, Object>> results, ExecuteResult<?> result) {
		Object data = result.getResult();
		if (data instanceof List) {
			((List<?>) data).stream()
					.filter(Objects::nonNull)
					.filter(Map.class::isInstance)
					.map(item -> (Map<String, Object>) item)
					.forEach(results::add);
		} else if (data instanceof Map) {
			results.add((Map<String, Object>) data);
		}
	}

	private int resolveLimit(Integer limit) {
		return limit == null || limit <= 0 ? DEFAULT_LIMIT : limit;
	}

	private int resolveBatchSize(Integer batchSize) {
		return batchSize == null || batchSize <= 0 ? DEFAULT_BATCH_SIZE : batchSize;
	}

	public static String formatTapDateTime(DateTime dateTime, String pattern) {
		try {
			DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(pattern);
			final ZoneId zoneId = dateTime.getTimeZone() != null ? dateTime.getTimeZone().toZoneId() : ZoneId.of("GMT");
			LocalDateTime localDateTime = LocalDateTime.ofInstant(dateTime.toInstant(), zoneId);
			return dateTimeFormatter.format(localDateTime);
		} catch (Throwable e) {
			log.debug("Format tap datetime failed", e);
		}
		return null;
	}
}
