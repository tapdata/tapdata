package io.tapdata.flow.engine.V2.sharecdc.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.persistence.ConstructType;
import com.hazelcast.persistence.PersistenceStorage;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.HazelcastUtil;
import com.tapdata.constant.JSONUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.OperationType;
import com.tapdata.entity.sharecdc.LogContent;
import com.tapdata.entity.task.NodeUtil;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.shareCdcTableMapping.ShareCdcTableMappingDto;
import io.tapdata.common.sharecdc.ShareCdcUtil;
import io.tapdata.construct.ConstructIterator;
import io.tapdata.construct.HazelcastConstruct;
import io.tapdata.construct.constructImpl.ConstructRingBuffer;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.ObjectSerializable;
import io.tapdata.flow.engine.V2.common.task.SyncTypeEnum;
import io.tapdata.flow.engine.V2.sharecdc.ShareCDCOffset;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcContext;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcTaskContext;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcTaskPdkContext;
import io.tapdata.flow.engine.V2.sharecdc.exception.ShareCdcUnsupportedException;
import io.tapdata.flow.engine.V2.util.ExternalStorageUtil;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.flow.engine.V2.util.SkipIdleProcessor;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.bson.BsonType;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.NotNull;
import org.springframework.data.mongodb.core.query.Query;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * @author samuel
 * @Description Shared incremental task reader
 * @create 2022-02-17 15:13
 **/
public class ShareCdcPDKTaskReader extends ShareCdcHZReader implements Serializable, MemoryFetcher {
	private static final long serialVersionUID = -8010918045236535239L;
	private static final int DEFAULT_THREAD_NUMBER = 8;
	private static final String THREAD_NAME_PREFIX = "Share-CDC-Task-Reader-";
	private static final String LOG_PREFIX = "[Share CDC Task HZ Reader] - ";
	public static final String TAG = ShareCdcPDKTaskReader.class.getSimpleName();
	private final static ObjectSerializable OBJECT_SERIALIZABLE = InstanceFactory.instance(ObjectSerializable.class);
	public static final int QUEUE_CAPACITY = 100;
	public static final int TABLE_NAME_PARTITION_SIZE = 10;
	private ExecutorService readThreadPool;
	private TaskDto logCollectorTaskDto;
	private HazelcastInstance hazelcastInstance;
	private String constructReferenceId;
	private List<String> tableNames;
	private String connNamespaceStr;
	private int threadNum = DEFAULT_THREAD_NUMBER;
	private List<ReadRunner> readRunners;
	private Map<String, Long> sequenceMap;
	private ExternalStorageDto logCollectorExternalStorage;
	private Future<?> future;
	private StreamReadConsumer streamReadConsumer;
	private CountDownLatch readCountDown;
	private final Map<String, MemoryMetrics> memoryMetricsMap = new HashMap<>();
	private AtomicBoolean readFirstData = new AtomicBoolean();

	ShareCdcPDKTaskReader(Object offset) {
		super();
		if (offset instanceof Map) {
			Map<String, Long> map = new HashMap<>();
			for (Map.Entry<?, ?> entry : ((Map<?, ?>) offset).entrySet()) {
				if (!(entry.getValue() instanceof Long)) {
					continue;
				}
				map.put(entry.getKey().toString(), Long.parseLong(((Long) entry.getValue()).toString()));
			}
			this.sequenceMap = new ConcurrentHashMap<>(map);
		} else {
			this.sequenceMap = new ConcurrentHashMap<>();
		}
	}

	@Override
	public void init(ShareCdcContext shareCdcContext) throws ShareCdcUnsupportedException {
		super.init(shareCdcContext);
		try {
			if (!(shareCdcContext instanceof ShareCdcTaskPdkContext)) {
				throw new IllegalArgumentException("Expected: " + ShareCdcTaskPdkContext.class.getName() + ", actual: " + this.shareCdcContext.getClass().getName());
			}
			ShareCdcTaskContext shareCdcTaskContext = (ShareCdcTaskContext) shareCdcContext;
			int step = 0;
			shareCdcContext.getObsLogger().info(logWrapper("Initializing share cdc reader..."));
			this.constructReferenceId = String.format("%s-%s-%s", getClass().getSimpleName(), shareCdcTaskContext.getNode().getTaskId(), shareCdcTaskContext.getNode().getId());
			this.hazelcastInstance = HazelcastUtil.getInstance(this.shareCdcContext.getConfigurationCenter());
			this.connNamespaceStr = Optional.ofNullable(shareCdcTaskContext.getConnections())
					.map(Connections::getNamespace)
					.map(ShareCdcUtil::joinNamespaces)
					.orElse(null);
			this.tableNames = NodeUtil.getTableNames(shareCdcTaskContext.getNode());
			step = canShareCdc(step);
			this.readRunners = new ArrayList<>();
			obsLogger.info(logWrapper(++step, "Init read thread pool completed"));
		} catch (IllegalArgumentException | ShareCdcUnsupportedException e) {
			throw e;
		} catch (Exception e) {
			throw new ShareCdcUnsupportedException("An internal error occurred when init share cdc reader; Error: " + e.getMessage(), e, false);
		}
		CommonUtils.ignoreAnyError(() -> PDKIntegration.registerMemoryFetcher(memoryFetchName((ShareCdcTaskPdkContext) shareCdcContext), this), TAG);
		obsLogger.info("Init share cdc reader completed");
	}

	private static String memoryFetchName(ShareCdcTaskPdkContext shareCdcContext) {
		return String.format("%s-%s-%s(%s)",
				TAG,
				shareCdcContext.getTaskDto().getName(),
				shareCdcContext.getNode().getName(),
				shareCdcContext.getNode().getId());
	}

	/**
	 * Check shared incremental mode is available
	 * If throw {@link ShareCdcUnsupportedException}, present cannot use shared incremental mode
	 */
	private int canShareCdc(int step) throws ShareCdcUnsupportedException {
		TaskDto taskDto = ((ShareCdcTaskContext) this.shareCdcContext).getTaskDto();
		Connections connections = ((ShareCdcTaskContext) this.shareCdcContext).getConnections();

		// Check connection whether to open enable share cdc
		if (!connections.isShareCdcEnable()) {
			throw new ShareCdcUnsupportedException("Connection " + connections.getName() + "(" + connections.getId() + ")" + " not enable share cdc", true);
		}
		obsLogger.info(logWrapper(++step, "Check connection " + connections.getName() + " enable share cdc: true"));

		// Check task whether to open enable share cdc
		if (!taskDto.getShareCdcEnable()) {
			throw new ShareCdcUnsupportedException("Task " + taskDto.getName() + " not enable share cdc", true);
		}
		obsLogger.info(logWrapper(++step, "Check task " + taskDto.getName() + " enable share cdc: true"));

		// Check log collector task is exists
		Map<String, String> shareCdcTaskId = taskDto.getShareCdcTaskId();
		if (MapUtils.isEmpty(shareCdcTaskId)) {
			throw new ShareCdcUnsupportedException("Not found any log collector task", false);
		}
		if (!shareCdcTaskId.containsKey(connections.getId())) {
			throw new ShareCdcUnsupportedException("Not found log collector task by connection id: " + connections.getId(), false);
		}
		this.logCollectorTaskDto = getLogCollectorSubTask();
		obsLogger.info(logWrapper(++step, "Found log collector task: " + this.logCollectorTaskDto.getName()));

		// Get log collector external storage config
		String shareCDCExternalStorageId = connections.getShareCDCExternalStorageId();
		if (StringUtils.isBlank(shareCDCExternalStorageId)) {
			logCollectorExternalStorage = ExternalStorageUtil.getDefaultExternalStorage();
		} else {
			logCollectorExternalStorage = clientMongoOperator.findOne(Query.query(where("_id").is(shareCDCExternalStorageId)), ConnectorConstant.EXTERNAL_STORAGE_COLLECTION, ExternalStorageDto.class);
		}
		if (null == logCollectorExternalStorage) {
			logCollectorExternalStorage = ExternalStorageUtil.getDefaultExternalStorage();
		}

		// Do not start ttl here
		logCollectorExternalStorage.setTtlDay(0);
		shareCdcContext.getObsLogger().info(logWrapper("Will use external storage: " + logCollectorExternalStorage));

		// Check start point valid of each table
		step = checkTableStartPointValid(step);
		return step;
	}

	private int checkTableStartPointValid(int step) throws ShareCdcUnsupportedException {
		logger.info(logWrapper(++step, "Check tables start point valid"));
		for (String tableName : tableNames) {
			ConstructRingBuffer<Document> constructRingBuffer = getConstruct(tableName);
			// Check cdc start timestamp is available in log storage
			try {
				if (sequenceMap.containsKey(tableName)) {
					continue;
				}
				String syncType = "";
				if (shareCdcContext instanceof ShareCdcTaskContext) {
					TaskDto taskDto = ((ShareCdcTaskContext) shareCdcContext).getTaskDto();
					syncType = taskDto.getSyncType();
				}
				if (null != this.shareCdcContext.getCdcStartTs() && this.shareCdcContext.getCdcStartTs().compareTo(0L) > 0) {
					// Dynamic add table to share task, maybe RingBuffer is uninitialized. check and wait to initialized
					while (true) {
						if (!running.get())
							throw new RuntimeException("Check table start point failed, because is stopping");
						if (constructRingBuffer.getRingbuffer().tailSequence() >= 0) break;

						Thread.sleep(1000L);
					}

					ConstructIterator<Document> iterator = constructRingBuffer.find();
					Document firstLogDocument = iterator.peek(15L, TimeUnit.SECONDS);
					if (null != firstLogDocument) {
						LogContent logContent = JSONUtil.map2POJO(firstLogDocument, new TypeReference<LogContent>() {
						});

						if (!syncType.equals(SyncTypeEnum.CDC.getSyncType())
								&& logContent.getType().equals(LogContent.LogContentType.SIGN.name())) {
							if (obsLogger.isDebugEnabled()) {
								obsLogger.debug("Found first log is a sign log: " + logContent);
							}
						} else if (logContent.getTimestamp() > this.shareCdcContext.getCdcStartTs()) {
							// First data's timestamp in storage must be lte task start cdc timestamp
							throw new ShareCdcUnsupportedException("Log storage[" + tableName + "] detected unusable, first log timestamp("
									+ Instant.ofEpochMilli(logContent.getTimestamp()) + ") is greater than task cdc start timestamp("
									+ Instant.ofEpochMilli(this.shareCdcContext.getCdcStartTs()) + ")", false);
						}
						if (obsLogger.isDebugEnabled()) {
							obsLogger.debug(logWrapper(++step, String.format("Log storage %s is available, first log timestamp: %s, task cdc start timestamp: %s",
									constructRingBuffer.getName(),
									Instant.ofEpochMilli(logContent.getTimestamp()),
									Instant.ofEpochMilli(this.shareCdcContext.getCdcStartTs()))
							));
						}
					} else {
						if (obsLogger.isDebugEnabled()) {
							obsLogger.debug(logWrapper(++step, String.format("Log storage %s is empty and available to use", constructRingBuffer.getName())));
						}
					}
				} else {
					if (obsLogger.isDebugEnabled()) {
						obsLogger.debug(logWrapper(++step, "Task incremental start timestamp less than 0, table [{}] will read from first line"), tableName);
					}
				}
			} catch (Exception e) {
				throw new ShareCdcUnsupportedException("Find from storage failed; Error: " + e.getMessage(), e, false);
			}
		}
		return step;
	}

	@NotNull
	private ConstructRingBuffer<Document> getConstruct(String tableName) {
		String constructName;
		if (null != connNamespaceStr) {
			constructName = ShareCdcUtil.getConstructName(this.logCollectorTaskDto, ShareCdcUtil.joinNamespaces(Arrays.asList(connNamespaceStr, tableName)));
		} else {
			constructName = ShareCdcUtil.getConstructName(this.logCollectorTaskDto, tableName);
		}
		constructName = constructName + "_" + ((ShareCdcTaskPdkContext) shareCdcContext).getTaskDto().getName();

		String connId = ((ShareCdcTaskPdkContext) shareCdcContext).getConnections().getId();
		String sign = ShareCdcTableMappingDto.genSign(connId, tableName);
		Query query = Query.query(where("sign").is(sign));
		ShareCdcTableMappingDto shareCdcTableMappingDto = clientMongoOperator.findOne(query, ConnectorConstant.SHARE_CDC_TABLE_MAPPING_COLLECTION, ShareCdcTableMappingDto.class);
		if (null == shareCdcTableMappingDto) {
			throw new RuntimeException("Share cdc table mapping not found, sign: " + sign);
		}
		obsLogger.info(logWrapper("Found share cdc table mapping: " + shareCdcTableMappingDto));
		tableName = shareCdcTableMappingDto.getExternalStorageTableName();
		logCollectorExternalStorage.setTable(tableName);
		logCollectorExternalStorage.setTtlDay(null);

		return new ConstructRingBuffer<>(
				hazelcastInstance,
				constructReferenceId,
				constructName,
				logCollectorExternalStorage
		);
	}

	private TaskDto getLogCollectorSubTask() throws ShareCdcUnsupportedException {
		TaskDto taskDto = ((ShareCdcTaskContext) this.shareCdcContext).getTaskDto();
		Connections connections = ((ShareCdcTaskContext) this.shareCdcContext).getConnections();
		Map<String, String> shareCdcTaskId = taskDto.getShareCdcTaskId();
		ObjectId logCollectorSubTaskId = new ObjectId(shareCdcTaskId.get(connections.getId()));
		this.logCollectorTaskDto = this.clientMongoOperator.findOne(new Query(where("_id").is(logCollectorSubTaskId)), ConnectorConstant.TASK_COLLECTION, TaskDto.class);
		if (this.logCollectorTaskDto == null) {
			throw new ShareCdcUnsupportedException("Cannot find sub task by id: " + logCollectorSubTaskId + "(" + logCollectorSubTaskId.getClass().getName() + ")", true);
		}
		return logCollectorTaskDto;
	}

	@Override
	public void listen(StreamReadConsumer streamReadConsumer) throws Exception {
		obsLogger.info(logWrapper("Starting listen share log storage..."));
		this.streamReadConsumer = streamReadConsumer;
		List<List<String>> partitionTableNames;
		if (!tableNames.isEmpty() && tableNames.size() <= TABLE_NAME_PARTITION_SIZE * 8) {
			int size = Math.max(1, tableNames.size() / threadNum);
			partitionTableNames = ListUtils.partition(tableNames, size);
			threadNum = partitionTableNames.size();
			obsLogger.info(logWrapper(String.format("Read table count: %s, partition size: %s, read thread number: %s", tableNames.size(), size, threadNum)));
		} else {
			partitionTableNames = ListUtils.partition(tableNames, TABLE_NAME_PARTITION_SIZE);
			threadNum = partitionTableNames.size();
			obsLogger.info(logWrapper(String.format("Read table count: %s, partition size: %s, read thread number: %s", tableNames.size(), TABLE_NAME_PARTITION_SIZE, threadNum)));
		}
		this.readThreadPool = new ThreadPoolExecutor(threadNum + 1, threadNum + 1, 0L, TimeUnit.SECONDS, new SynchronousQueue<>());
		int index = 1;
//		future = this.readThreadPool.submit(this::readPreVersionData);
		this.readCountDown = new CountDownLatch(partitionTableNames.size());
		for (List<String> partitionTableName : partitionTableNames) {
			ReadRunner readRunner = new ReadRunner(
					index++,
					partitionTableName,
					shareCdcContext
			);
			try {
				this.readThreadPool.submit(readRunner::read);
				this.readRunners.add(readRunner);
			} catch (RejectedExecutionException ignored) {
			}
		}
		try {
			poll(streamReadConsumer);
		} catch (Exception e) {
			String err = "An internal error occurred, will close; Error: " + e.getMessage();
			this.close();
			throw new Exception(err, e);
		}
	}

	private void readPreVersionData() {
		if (shareCdcContext instanceof ShareCdcTaskContext) {
			TaskDto taskDto = ((ShareCdcTaskContext) shareCdcContext).getTaskDto();
			Thread.currentThread().setName(THREAD_NAME_PREFIX + "-Old-Version-" + taskDto.getName());
		} else {
			return;
		}
		try {
			ConstructRingBuffer<Document> ringBuffer = new ConstructRingBuffer<>(hazelcastInstance, constructReferenceId, ShareCdcUtil.getConstructName(this.logCollectorTaskDto), logCollectorExternalStorage);
			if (ringBuffer.isEmpty()) {
				return;
			}
			Long cdcStartTs = this.shareCdcContext.getCdcStartTs();
			long sequence;
			if (null != cdcStartTs && cdcStartTs.compareTo(0L) > 0) {
				sequence = PersistenceStorage.getInstance().findSequence(ringBuffer.getRingbuffer(), cdcStartTs);
			} else {
				return;
			}
			if (sequence < 0) {
				return;
			}
			Map<String, Object> filter = new HashMap<>();
			filter.put(ConstructRingBuffer.SEQUENCE_KEY, sequence);
			ConstructIterator<Document> iterator;
			try {
				iterator = ringBuffer.find(filter);
			} catch (Exception e) {
				throw new RuntimeException(String.format("Find ringbuffer by sequence '%s' failed", sequence), e);
			}
			shareCdcContext.getObsLogger().info("Start read old version log data, name: {}, sequence: {}", ringBuffer.getName(), sequence);
			while (this.running.get()) {
				long start = System.currentTimeMillis();
				Document document = iterator.tryNext();
				if (null == document) {
					break;
				}
				if (!document.containsKey("fromTable")) {
					continue;
				}
				String fromTable = document.getString("fromTable");
				if (!tableNames.contains(fromTable)) {
					continue;
				}
				if (shareCdcContext.getObsLogger().isDebugEnabled()) {
					shareCdcContext.getObsLogger().debug(LOG_PREFIX + "Read old version log data, name: {}, sequence: {}, document: {}, try next time consuming: {}",
							ringBuffer.getName(), sequence, document, (System.currentTimeMillis() - start));
				}
				enqueue(tapEventWrapper(document));
			}
		} catch (Exception e) {
			String err = "Read old version log data failed";
			handleFailed(err, e);
		}
	}

	private class ReadRunner {
		private int index;
		private ShareCdcContext shareCdcContext;
		private List<String> tableNames;
		private TaskDto taskDto;
		private final Map<String, ShareCdcReaderResource> readerResourceMap;

		public ReadRunner(int index,
						  List<String> tableNames,
						  ShareCdcContext shareCdcContext) {
			this.index = index;
			this.tableNames = tableNames;
			if (shareCdcContext instanceof ShareCdcTaskContext) {
				this.taskDto = ((ShareCdcTaskContext) shareCdcContext).getTaskDto();
			}
			this.shareCdcContext = shareCdcContext;
			this.readerResourceMap = new HashMap<>();
			for (String tableName : tableNames) {
				ConstructRingBuffer<Document> constructRingBuffer = getConstruct(tableName);
				obsLogger.info("[{}] Successfully obtained construct, name: {}, external storage name: {}", LOG_PREFIX, constructRingBuffer.getName(), logCollectorExternalStorage.getTable());
				ShareCdcReaderResource shareCdcReaderResource = new ShareCdcReaderResource(constructRingBuffer);
				if (null != sequenceMap && sequenceMap.containsKey(tableName)) {
					shareCdcReaderResource.sequence(sequenceMap.get(tableName));
					if (logger.isDebugEnabled()) {
						obsLogger.debug("[{}] Found share cdc table[{}] offset, sequence: {}", LOG_PREFIX, tableNames, sequenceMap.get(tableName));
					}
				}
				readerResourceMap.put(tableName, shareCdcReaderResource);
			}
		}

		public void read() {
			Thread.currentThread().setName(THREAD_NAME_PREFIX + "-" + taskDto.getName() + "-" + index);
			obsLogger.info(logWrapper("Starting read log from hazelcast construct, tables: " + tableNames));
			/*while (running.get()) {
				if (null == future || future.isDone()) {
					break;
				}
				try {
					TimeUnit.MILLISECONDS.sleep(10L);
				} catch (InterruptedException e) {
					break;
				}
			}*/

			try (SkipIdleProcessor<String> skipIdleProcessor = new SkipIdleProcessor<>(() -> running.get(), tableNames)) {
				CommonUtils.ignoreAnyError(() -> PDKIntegration.registerMemoryFetcher(skipIdleMemoryFetchName((ShareCdcTaskPdkContext) shareCdcContext, index), skipIdleProcessor), TAG);
				final BiFunction<String, ShareCdcReaderResource, ConstructIterator<Document>> constructItFn = (tableName, shareCdcReaderResource) -> {
					HazelcastConstruct<Document> construct = shareCdcReaderResource.construct;
					if (null == shareCdcReaderResource.sequence) {
						try {
							// Find first sequence by timestamp
							if (null == this.shareCdcContext.getCdcStartTs()) {
								throw new RuntimeException("Cannot found table[" + tableName + "] share cdc start time from sync progress");
							}
							long sequenceFindByTs = construct.findSequence(this.shareCdcContext.getCdcStartTs());
							shareCdcReaderResource.sequence(sequenceFindByTs);
							sequenceMap.put(tableName, sequenceFindByTs);
							shareCdcContext.getObsLogger().info(logWrapper("Find sequence in construct(" + tableName + ") by timestamp(" + Instant.ofEpochMilli(this.shareCdcContext.getCdcStartTs()) + "): " + sequenceFindByTs));
						} catch (Exception e) {
							String err = "Find sequence by timestamp failed, timestamp: " + this.shareCdcContext.getCdcStartTs() + "; Error: " + e.getMessage();
							handleFailed(err, e);
							return null;
						}
					}
					if (shareCdcReaderResource.firstTime) {
						shareCdcReaderResource.firstTime = false;
						readCountDown.countDown();
						if (readCountDown.getCount() <= 0 && null != streamReadConsumer) {
							streamReadConsumer.streamReadStarted();
						}
						shareCdcContext.getObsLogger().info(logWrapper("Starting read '{}' log, sequence: {}"), tableName, shareCdcReaderResource.sequence);
					}
					// Find hazelcast construct iterator
					Map<String, Object> filter = new HashMap<>();
					filter.put(ConstructRingBuffer.SEQUENCE_KEY, shareCdcReaderResource.sequence);
					ConstructIterator<Document> iterator;
					if (null == shareCdcReaderResource.iterator) {
						try {
							iterator = construct.find(filter);
							shareCdcReaderResource.iterator = iterator;
							obsLogger.info(logWrapper("Find by " + tableName + " filter: " + filter));
						} catch (Exception e) {
							String err = "Find from hazelcast construct " + construct.getClass().getName() + " failed, filter: " + filter + "; Error: " + e.getMessage();
							handleFailed(err);
							return null;
						}
					}
					iterator = shareCdcReaderResource.iterator;
					if (iterator == null) {
						String err = "Find hazelcast construct " + construct.getClass().getName() + " failed, iterator result is null, filter: " + filter;
						handleFailed(err);
						return null;
					}
					return iterator;
				};

				final Boolean ctlStatusBreak = true, ctlStatusIdle = null, ctlStatusNormal = false;
				while (running.get()) {
					Boolean needBreak = skipIdleProcessor.process(readerResourceMap, (tableName, readerResourceMap) -> {
						MemoryMetrics memoryMetrics = memoryMetricsMap.computeIfAbsent(tableName, k -> new MemoryMetrics(tableName));
						ShareCdcReaderResource shareCdcReaderResource = readerResourceMap.get(tableName);
						if (null == shareCdcReaderResource) {
							if (running.get()) {
								throw new RuntimeException("Cannot found table[" + tableName + "] share cdc reader resource");
							} else {
								return ctlStatusIdle;
							}
						}

						ConstructIterator<Document> iterator = constructItFn.apply(tableName, shareCdcReaderResource);
						if (null == iterator) {
							return ctlStatusBreak;
						}

						List<Document> documents = new ArrayList<>();
						try {
							AtomicReference<Document> document = new AtomicReference<>();
							memoryMetrics.find(() -> document.set(iterator.tryNext()));
							if (null == document.get()) {
								return ctlStatusIdle;
							}

							if (document.get().containsKey("type") && LogContent.LogContentType.SIGN.name().equals(document.get().getString("type"))) {
								return ctlStatusNormal;
							}
							documents.add(document.get());
							if (readFirstData.compareAndSet(false, true)) {
								obsLogger.info(logWrapper("Successfully read first log data: " + document.get()));
							}
						} catch (DistributedObjectDestroyedException e) {
							return ctlStatusIdle;
						} catch (Exception e) {
							String err = "Find next failed, sequence: " + iterator.getSequence();
							handleFailed(err, e);
							return ctlStatusIdle;
						}

						if (obsLogger.isDebugEnabled()) {
							obsLogger.debug("Received log documents");
							shareCdcContext.getObsLogger().debug("Received log documents");
							documents.forEach(doc -> obsLogger.debug("  " + doc.toJson()));
						}
						sequenceMap.put(tableName, iterator.getSequence());
						memoryMetrics.setSequence(iterator.getSequence());
						documents.forEach(doc -> enqueue(tapEventWrapper(doc)));
						if (shareCdcReaderResource.firstData) {
							shareCdcContext.getObsLogger().info(logWrapper("Successfully read " + tableName + "'s first log data, will continue to read the log"));
							shareCdcReaderResource.firstData = false;
						}

						return ctlStatusNormal;
					});

					if (Boolean.TRUE.equals(needBreak)) break;
				}
			} catch (Exception e) {
				String err = "Reader occur unknown error, will stop";
				handleFailed(err, e);
			}
		}

		public void close() {
			String tag = ReadRunner.class.getSimpleName();
			for (String tableName : tableNames) {
				ShareCdcReaderResource readerResource = readerResourceMap.remove(tableName);
				if (null == readerResource) continue;
				CommonUtils.ignoreAnyError(() -> PersistenceStorage.getInstance().destroy(constructReferenceId, ConstructType.RINGBUFFER, readerResource.construct.getName()), tag);
			}
			CommonUtils.ignoreAnyError(() -> PDKIntegration.unregisterMemoryFetcher(skipIdleMemoryFetchName((ShareCdcTaskPdkContext) shareCdcContext, index)), TAG);
		}
	}

	private static String skipIdleMemoryFetchName(ShareCdcTaskPdkContext shareCdcTaskPdkContext, int index) {
		return String.format("%s-%s-%s-%s(%s)-%s", TAG, SkipIdleProcessor.class.getSimpleName(),
				shareCdcTaskPdkContext.getTaskDto().getName(), shareCdcTaskPdkContext.getNode().getName(),
				shareCdcTaskPdkContext.getNode().getId(), index);
	}

	private static class ShareCdcReaderResource {
		private final HazelcastConstruct<Document> construct;
		private Long sequence;
		private boolean firstTime = true;
		private boolean firstData = true;
		private ConstructIterator<Document> iterator;

		public ShareCdcReaderResource(HazelcastConstruct<Document> construct) {
			this.construct = construct;
		}

		public ShareCdcReaderResource sequence(long sequence) {
			this.sequence = sequence;
			return this;
		}

		public ShareCdcReaderResource iterator(ConstructIterator<Document> iterator) {
			this.iterator = iterator;
			return this;
		}
	}

	private ShareCDCReaderEvent tapEventWrapper(Document document) {
		if (null == document) {
			return null;
		}
		LogContent logContent = LogContent.valueOf(document);
		logContentVerify(logContent);

		TapEvent tapEvent = null;
		OperationType operationType = OperationType.fromOp(logContent.getOp());
		switch (operationType) {
			case INSERT:
				tapEvent = new TapInsertRecordEvent().init();
				handleData(logContent.getAfter());
				((TapInsertRecordEvent) tapEvent).setAfter(logContent.getAfter());
				break;
			case UPDATE:
				tapEvent = new TapUpdateRecordEvent().init();
				handleData(logContent.getBefore());
				((TapUpdateRecordEvent) tapEvent).setBefore(logContent.getBefore());
				handleData(logContent.getAfter());
				((TapUpdateRecordEvent) tapEvent).setAfter(logContent.getAfter());
				break;
			case DELETE:
				tapEvent = new TapDeleteRecordEvent().init();
				handleData(logContent.getBefore());
				((TapDeleteRecordEvent) tapEvent).setBefore(logContent.getBefore());
				break;
			case DDL:
				Object tapDDLEventObj = OBJECT_SERIALIZABLE.toObject(logContent.getTapDDLEvent(),
						new ObjectSerializable.ToObjectOptions().classLoader(logContent.getClass().getClassLoader()));
				if (tapDDLEventObj instanceof TapDDLEvent) {
					tapEvent = (TapDDLEvent) tapDDLEventObj;
				}
				break;
			default:
				logger.warn("Found unrecognized operation type: " + logContent.getOp() + ", will skip it: " + logContent);
				break;
		}
		if (null == tapEvent) {
			return null;
		}
		((TapBaseEvent) tapEvent).setReferenceTime(logContent.getTimestamp());
		((TapBaseEvent) tapEvent).setTableId(logContent.getFromTable());
		Object offsetObj;
		try {
			offsetObj = PdkUtil.decodeOffset(logContent.getOffsetString(), ((ShareCdcTaskPdkContext) shareCdcContext).getConnectorNode());
		} catch (Throwable e) {
			String err = "Decode offset string failed: " + logContent.getOffsetString() + "; Error: " + e.getMessage();
			throw new RuntimeException(err, e);
		}
		ShareCDCOffset shareCDCOffset = new ShareCDCOffset(sequenceMap, offsetObj);
		return new ShareCDCReaderEvent(tapEvent, shareCDCOffset);
	}

	private static String logWrapper(String message) {
		return LOG_PREFIX + message;
	}

	private String logWrapper(int step, String message) {
		return LOG_PREFIX + "Step " + step + " - " + message;
	}

	@Override
	public void close() throws IOException {
		try {
			CommonUtils.ignoreAnyError(() -> {
				if (CollectionUtils.isNotEmpty(readRunners)) {
					readRunners.forEach(ReadRunner::close);
				}
			}, TAG);
			CommonUtils.ignoreAnyError(() -> {
				if (null != this.readThreadPool) {
					this.readThreadPool.shutdownNow();
				}
			}, TAG);
			CommonUtils.ignoreAnyError(() -> PDKIntegration.unregisterMemoryFetcher(memoryFetchName((ShareCdcTaskPdkContext) shareCdcContext)), TAG);
		} finally {
			super.close();
		}
	}

	private static void handleData(Map<String, Object> data) {
		if (null == data) {
			return;
		}
		data.forEach((k, v) -> {
			byte[] bytes = null;
			if (v instanceof Binary) {
				bytes = ((Binary) v).getData();
			} else if (v instanceof byte[]) {
				bytes = (byte[]) v;
			}
			if (null != bytes && bytes.length > 0) {
				if (bytes.length == 26 && bytes[0] == 99 && bytes[bytes.length - 1] == 23) {
					byte[] dest = new byte[bytes.length - 2];
					System.arraycopy(bytes, 1, dest, 0, dest.length);
					TapStringValue tapStringValue = new TapStringValue();
					tapStringValue.setOriginValue(bytes);
					tapStringValue.setTapType(new TapString(24L, true));
					tapStringValue.setOriginType(BsonType.OBJECT_ID.name());
					tapStringValue.setValue(new String(dest));
					data.put(k, tapStringValue);
				}
			}
		});
	}

	@Override
	public DataMap memory(String keyRegex, String memoryLevel) {
		DataMap dataMap = new DataMap();
		try {
			if (MapUtils.isNotEmpty(memoryMetricsMap)) {
				for (Map.Entry<String, MemoryMetrics> entry : memoryMetricsMap.entrySet()) {
					try {
						dataMap.kv(entry.getKey(), entry.getValue().toString());
					} catch (Exception e) {
						dataMap.kv(entry.getKey() + " error", e.getMessage() + "; Stack: " + ExceptionUtils.getStackTrace(e));
					}
				}
			}
		} catch (Exception e) {
			dataMap.kv("error", e.getMessage() + "  \n" + ExceptionUtils.getStackTrace(e));
		}
		return dataMap;
	}

	private static class MemoryMetrics {
		private final String table;
		private Long sequence;
		private Long findAllCostMS;
		private Long findTime;
		private Long findMaxCostMS;

		public MemoryMetrics(String table) {
			this.table = table;
			this.sequence = -1L;
			this.findAllCostMS = 0L;
			this.findTime = 0L;
			this.findMaxCostMS = 0L;
		}

		public void setSequence(Long sequence) {
			this.sequence = sequence;
		}

		public void find(Runnable runnable) {
			if (null == runnable) {
				return;
			}
			long startMS = System.currentTimeMillis();
			runnable.run();
			long endMS = System.currentTimeMillis();
			long costMS = endMS - startMS;
			if (costMS > findMaxCostMS) {
				findMaxCostMS = costMS;
			}
			findTime++;
			findAllCostMS += costMS;
			if (findTime == 100000L) {
				findAllCostMS = costMS;
				findTime = 1L;
			}
		}

		@Override
		public String toString() {
			BigDecimal findAvgMS = BigDecimal.ZERO;
			if (findTime > 0L) {
				findAvgMS = BigDecimal.valueOf(findAllCostMS).divide(BigDecimal.valueOf(findTime), 2, RoundingMode.HALF_UP);
			}
			BigDecimal qps = BigDecimal.ZERO;
			if (findAvgMS.compareTo(BigDecimal.ZERO) > 0) {
				qps = BigDecimal.valueOf(1000).divide(findAvgMS, 2, RoundingMode.HALF_UP);
			}
			return String.format("Table: %s, Sequence: %s, FindAllCostMS: %s, FindTime: %s, FindAvgMS: %s, FindMaxCostMS: %s, QPS: %s",
					table, sequence, findAllCostMS, findTime, findAvgMS, findMaxCostMS, qps);
		}
	}
}
