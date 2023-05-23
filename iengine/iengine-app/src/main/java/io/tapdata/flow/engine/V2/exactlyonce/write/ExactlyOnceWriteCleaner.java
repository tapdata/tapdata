package io.tapdata.flow.engine.V2.exactlyonce.write;

import com.hazelcast.core.HazelcastInstance;
import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectionUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.UUIDGenerator;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.construct.constructImpl.DocumentIMap;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.flow.engine.V2.entity.PdkStateMap;
import io.tapdata.flow.engine.V2.exactlyonce.ExactlyOnceUtil;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author samuel
 * @Description
 * @create 2023-05-17 19:26
 **/
public class ExactlyOnceWriteCleaner {
	private static final Logger logger = LogManager.getLogger(ExactlyOnceWriteCleaner.class);
	public static final int BATCH_SIZE = 100;
	public static final String TAG = ExactlyOnceWriteCleaner.class.getSimpleName();
	private final ConcurrentHashMap<String, ExactlyOnceWriteCleanerEntity> cleanerEntityMap = new ConcurrentHashMap<>();
	private static volatile ExactlyOnceWriteCleaner instance;

	public static ExactlyOnceWriteCleaner getInstance() {
		if (null == instance) {
			synchronized (ExactlyOnceWriteCleaner.class) {
				if (null == instance) {
					instance = new ExactlyOnceWriteCleaner();
				}
			}
		}
		return instance;
	}

	public ExactlyOnceWriteCleaner() {
		ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(1);
		scheduledExecutorService.scheduleAtFixedRate(() -> {
			Thread.currentThread().setName("Exactly_Once_Write_Cleaner");
			try {
				for (ExactlyOnceWriteCleanerEntity cleanerEntity : cleanerEntityMap.values()) {
					synchronized (cleanerEntity.getLock()) {
						String connectionId = cleanerEntity.getConnectionId();
						ClientMongoOperator clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
						if (null == clientMongoOperator) {
							break;
						}
						logger.info("Start clean exactly once cache: {}", cleanerEntity);
						Connections connections = clientMongoOperator.findOne(Query.query(Criteria.where("_id").is(connectionId)), ConnectorConstant.CONNECTION_COLLECTION, Connections.class);
						DatabaseTypeEnum.DatabaseType databaseType = ConnectionUtil.getDatabaseType(clientMongoOperator, connections.getPdkHash());
						ExactlyOnceWriteCleanerKVMap<TapTable> tapTableMap = new ExactlyOnceWriteCleanerKVMap<>();
						ConnectorNode connectorNode = PdkUtil.createNode(
								UUIDGenerator.uuid(),
								databaseType,
								clientMongoOperator,
								ExactlyOnceWriteCleaner.class.getSimpleName() + "_" + System.currentTimeMillis(),
								connections.getConfig(),
								tapTableMap,
								new ExactlyOnceWriteCleanerStateMap(),
								new ExactlyOnceWriteCleanerStateMap(),
								new ExactlyOnceWriteCleanerLog()
						);
						try {
							PDKInvocationMonitor.invoke(connectorNode, PDKMethod.INIT,
									connectorNode::connectorInit, TAG);
						} catch (Exception e) {
							logger.error(String.format("Init connector failed when clean exactly once cache: %s", connectorNode));
							continue;
						}
						try {
							ConnectorFunctions connectorFunctions = connectorNode.getConnectorFunctions();
							QueryByAdvanceFilterFunction queryByAdvanceFilterFunction = connectorFunctions.getQueryByAdvanceFilterFunction();
							WriteRecordFunction writeRecordFunction = connectorFunctions.getWriteRecordFunction();
							if (null == queryByAdvanceFilterFunction || null == writeRecordFunction) {
								logger.warn("Cannot clean exactly once cache, connector's write records or query by advance filter function is null: {}", cleanerEntity);
								continue;
							}
							TapTable tapTable = ExactlyOnceUtil.generateExactlyOnceTable(connectorNode);
							tapTableMap.put(tapTable.getId(), tapTable);

							TapAdvanceFilter tapAdvanceFilter = TapAdvanceFilter.create()
									//							.op(QueryOperator.lt(ExactlyOnceUtil.TIMESTAMP_COL_NAME, System.currentTimeMillis() - TimeUnit.DAYS.toMillis(cleanerEntity.getTimeWindowDay())))
									.op(QueryOperator.lt(ExactlyOnceUtil.TIMESTAMP_COL_NAME, 1684407986084L))
									.batchSize(BATCH_SIZE);
							PDKInvocationMonitor.invoke(
									connectorNode,
									PDKMethod.SOURCE_QUERY_BY_ADVANCE_FILTER,
									() -> queryByAdvanceFilterFunction.query(connectorNode.getConnectorContext(), tapAdvanceFilter, tapTable, rs -> {
										if (null != rs.getError()) {
											logger.error("Clean exactly once cache error when query by advance filter: {}", tapAdvanceFilter, rs.getError());
											return;
										}
										List<Map<String, Object>> results = rs.getResults();
										List<TapRecordEvent> tapRecordEvents = new ArrayList<>();
										for (Map<String, Object> result : results) {
											TapDeleteRecordEvent tapDeleteRecordEvent = TapDeleteRecordEvent.create()
													.before(result);
											tapRecordEvents.add(tapDeleteRecordEvent);
										}
										PDKInvocationMonitor.invoke(
												connectorNode,
												PDKMethod.TARGET_WRITE_RECORD,
												() -> writeRecordFunction.writeRecord(connectorNode.getConnectorContext(), tapRecordEvents, tapTable, delRs -> {
												}), TAG);
									}), TAG);
						} finally {
							PDKInvocationMonitor.invoke(connectorNode, PDKMethod.STOP, connectorNode::connectorStop, TAG);
						}
					}
				}
			} catch (Exception e) {
				logger.error("Clean exactly once cache error: ", e);
			}
		}, TimeUnit.HOURS.toMillis(1L), TimeUnit.DAYS.toMillis(1L), TimeUnit.MILLISECONDS);
	}

	public void registerCleaner(ExactlyOnceWriteCleanerEntity cleanerEntity) {
		if (null == cleanerEntity || cleanerEntity.isEmpty()) {
			return;
		}
		cleanerEntityMap.putIfAbsent(cleanerEntity.identity(), cleanerEntity);
		cleanerEntityMap.computeIfPresent(cleanerEntity.identity(), (k, v) -> v.timeWindowDay(cleanerEntity.getTimeWindowDay()));
	}

	public void unregisterCleaner(ExactlyOnceWriteCleanerEntity cleanerEntity) {
		if (null == cleanerEntity || cleanerEntity.isEmpty()) {
			return;
		}
		synchronized (cleanerEntity.getLock()) {
			cleanerEntityMap.remove(cleanerEntity.identity());
		}
	}

	private static class ExactlyOnceWriteCleanerKVMap<T> implements KVMap<T> {
		private final Map<String, T> map = new HashMap<>();

		@Override
		public void init(String mapKey, Class<T> valueClass) {
		}

		@Override
		public void put(String key, T t) {
			map.put(key, t);
		}

		@Override
		public T putIfAbsent(String key, T t) {
			return map.putIfAbsent(key, t);
		}

		@Override
		public T remove(String key) {
			return map.remove(key);
		}

		@Override
		public void clear() {
			map.clear();
		}

		@Override
		public void reset() {
			clear();
		}

		@Override
		public T get(String key) {
			return map.get(key);
		}
	}

	private static class ExactlyOnceWriteCleanerStateMap extends PdkStateMap {
		private final Map<String, Object> map = new HashMap<>();

		public ExactlyOnceWriteCleanerStateMap() {
			super();
		}

		public ExactlyOnceWriteCleanerStateMap(String nodeId, HazelcastInstance hazelcastInstance) {
			super(nodeId, hazelcastInstance);
		}

		public ExactlyOnceWriteCleanerStateMap(String nodeId, HazelcastInstance hazelcastInstance, TaskDto taskDto, Node<?> node, ClientMongoOperator clientMongoOperator, String func) {
			super(nodeId, hazelcastInstance, taskDto, node, clientMongoOperator, func);
		}

		@Override
		public void init(String mapKey, Class<Object> valueClass) {
		}

		@Override
		public void put(String key, Object o) {
			map.put(key, o);
		}

		@Override
		public Object putIfAbsent(String key, Object o) {
			return map.putIfAbsent(key, o);
		}

		@Override
		public Object remove(String key) {
			return map.remove(key);
		}

		@Override
		public void clear() {
			map.clear();
		}

		@Override
		public void reset() {
			clear();
		}

		@Override
		public Object get(String key) {
			return map.get(key);
		}

		@Override
		public DocumentIMap<Document> getConstructIMap() {
			return null;
		}
	}

	private static class ExactlyOnceWriteCleanerLog implements Log {
		@Override
		public void debug(String message, Object... params) {
		}

		@Override
		public void info(String message, Object... params) {
		}

		@Override
		public void warn(String message, Object... params) {

		}

		@Override
		public void error(String message, Object... params) {

		}

		@Override
		public void error(String message, Throwable throwable) {

		}

		@Override
		public void fatal(String message, Object... params) {

		}
	}

}
