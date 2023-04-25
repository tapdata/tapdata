package io.tapdata.flow.engine.V2.sharecdc.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.hazelcast.core.HazelcastInstance;
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
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.schema.value.TapStringValue;
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
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.BsonType;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.NotNull;
import org.springframework.data.mongodb.core.query.Query;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * @author samuel
 * @Description Shared incremental task reader
 * @create 2022-02-17 15:13
 **/
public class ShareCdcPDKTaskReader extends ShareCdcHZReader implements Serializable {
	private static final long serialVersionUID = -8010918045236535239L;
	private static final int DEFAULT_THREAD_NUMBER = 8;
	private static final String THREAD_NAME_PREFIX = "Share-CDC-Task-Reader-";
	private static final String LOG_PREFIX = "[Share CDC Task HZ Reader] - ";
	public static final String TAG = ShareCdcPDKTaskReader.class.getSimpleName();
	private final static ObjectSerializable OBJECT_SERIALIZABLE = InstanceFactory.instance(ObjectSerializable.class);
	public static final int QUEUE_CAPACITY = 100;
	private ExecutorService readThreadPool;
	private TaskDto logCollectorTaskDto;
	private HazelcastInstance hazelcastInstance;
	private List<String> tableNames;
	private int threadNum = DEFAULT_THREAD_NUMBER;
	private List<ReadRunner> readRunners;
	private Map<String, Long> sequenceMap;
	private ExternalStorageDto logCollectorExternalStorage;
	private Future<?> future;
	private StreamReadConsumer streamReadConsumer;
	private CountDownLatch readCountDown;

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
			int step = 0;
			shareCdcContext.getObsLogger().info(logWrapper("Initializing share cdc reader..."));
			this.running = new AtomicBoolean(true);
			this.hazelcastInstance = HazelcastUtil.getInstance(this.shareCdcContext.getConfigurationCenter());
			this.tableNames = NodeUtil.getTableNames(((ShareCdcTaskContext) shareCdcContext).getNode());
			step = canShareCdc(step);
			this.readRunners = new ArrayList<>();
			logger.info(logWrapper(++step, "Init read thread pool completed"));
		} catch (IllegalArgumentException | ShareCdcUnsupportedException e) {
			throw e;
		} catch (Exception e) {
			throw new ShareCdcUnsupportedException("An internal error occurred when init share cdc reader; Error: " + e.getMessage(), e, false);
		}
		logger.info("Init share cdc reader completed");
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
		logger.info(logWrapper(++step, "Check connection " + connections.getName() + " enable share cdc: true"));

		// Check task whether to open enable share cdc
		if (!taskDto.getShareCdcEnable()) {
			throw new ShareCdcUnsupportedException("Task " + taskDto.getName() + " not enable share cdc", true);
		}
		logger.info(logWrapper(++step, "Check task " + taskDto.getName() + " enable share cdc: true"));

		// Check log collector task is exists
		Map<String, String> shareCdcTaskId = taskDto.getShareCdcTaskId();
		if (MapUtils.isEmpty(shareCdcTaskId)) {
			throw new ShareCdcUnsupportedException("Not found any log collector task", false);
		}
		if (!shareCdcTaskId.containsKey(connections.getId())) {
			throw new ShareCdcUnsupportedException("Not found log collector task by connection id: " + connections.getId(), false);
		}
		this.logCollectorTaskDto = getLogCollectorSubTask();
		logger.info(logWrapper(++step, "Found log collector task: " + this.logCollectorTaskDto.getName()));

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
					ConstructIterator<Document> iterator = constructRingBuffer.find();
					Document firstLogDocument = iterator.peek(15L, TimeUnit.SECONDS);
					if (null != firstLogDocument) {
						LogContent logContent = JSONUtil.map2POJO(firstLogDocument, new TypeReference<LogContent>() {
						});

						if (!syncType.equals(SyncTypeEnum.CDC.getSyncType())
								&& logContent.getType().equals(LogContent.LogContentType.SIGN.name())) {
							if (logger.isDebugEnabled()) {
								logger.debug("Found first log is a sign log: " + logContent);
							}
						} else if (logContent.getTimestamp() > this.shareCdcContext.getCdcStartTs()) {
							// First data's timestamp in storage must be lte task start cdc timestamp
							throw new ShareCdcUnsupportedException("Log storage[" + tableName + "] detected unusable, first log timestamp("
									+ Instant.ofEpochMilli(logContent.getTimestamp()) + ") is greater than task cdc start timestamp("
									+ Instant.ofEpochMilli(this.shareCdcContext.getCdcStartTs()) + ")", false);
						}
						if (logger.isDebugEnabled()) {
							logger.debug(logWrapper(++step, String.format("Log storage %s is available, first log timestamp: %s, task cdc start timestamp: %s",
									constructRingBuffer.getName(),
									Instant.ofEpochMilli(logContent.getTimestamp()),
									Instant.ofEpochMilli(this.shareCdcContext.getCdcStartTs()))
							));
						}
					} else {
						if (logger.isDebugEnabled()) {
							logger.debug(logWrapper(++step, String.format("Log storage %s is empty and available to use", constructRingBuffer.getName())));
						}
					}
				} else {
					if (logger.isDebugEnabled()) {
						logger.debug(logWrapper(++step, "Task incremental start timestamp less than 0, table [{}] will read from first line"), tableName);
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
		return new ConstructRingBuffer<>(
				hazelcastInstance,
				ShareCdcUtil.getConstructName(this.logCollectorTaskDto, tableName),
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
		logger.info(logWrapper("Starting listen share log storage..."));
		this.streamReadConsumer = streamReadConsumer;
		int size = Math.max(1, tableNames.size() / threadNum);
		List<List<String>> partitionTableNames = ListUtils.partition(tableNames, size);
		threadNum = partitionTableNames.size();
		this.readThreadPool = new ThreadPoolExecutor(threadNum + 1, threadNum + 1, 0L, TimeUnit.SECONDS, new SynchronousQueue<>());
		int index = 1;
		future = this.readThreadPool.submit(this::readPreVersionData);
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
			ConstructRingBuffer<Document> ringBuffer = new ConstructRingBuffer<>(hazelcastInstance, ShareCdcUtil.getConstructName(this.logCollectorTaskDto), logCollectorExternalStorage);
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
				ShareCdcReaderResource shareCdcReaderResource = new ShareCdcReaderResource(constructRingBuffer);
				if (null != sequenceMap && sequenceMap.containsKey(tableName)) {
					shareCdcReaderResource.sequence(sequenceMap.get(tableName));
					if (logger.isDebugEnabled()) {
						logger.debug("Found share cdc table[{}] offset, sequence: {}", tableNames, sequenceMap.get(tableName));
						shareCdcContext.getObsLogger().debug("Found share cdc table[{}] offset, sequence: {}", tableNames, sequenceMap.get(tableName));
					}
				}
				readerResourceMap.put(tableName, shareCdcReaderResource);
			}
		}

		public void read() {
			Thread.currentThread().setName(THREAD_NAME_PREFIX + "-" + taskDto.getName() + "-" + index);
			shareCdcContext.getObsLogger().info(logWrapper("Starting read log from hazelcast construct, tables: " + tableNames));
			while (running.get()) {
				if (null == future || future.isDone()) {
					break;
				}
				try {
					TimeUnit.MILLISECONDS.sleep(10L);
				} catch (InterruptedException e) {
					break;
				}
			}
			try {
				while (running.get()) {
					for (String tableName : tableNames) {
						HazelcastConstruct<Document> construct = readerResourceMap.get(tableName).construct;
						if (null == readerResourceMap.get(tableName).sequence) {
							try {
								// Find first sequence by timestamp
								if (null == this.shareCdcContext.getCdcStartTs()) {
									throw new RuntimeException("Cannot found table[" + tableName + "] share cdc start time from sync progress");
								}
								long sequenceFindByTs = construct.findSequence(this.shareCdcContext.getCdcStartTs());
								readerResourceMap.get(tableName).sequence(sequenceFindByTs);
								sequenceMap.put(tableName, sequenceFindByTs);
								shareCdcContext.getObsLogger().info(logWrapper("Find sequence in construct(" + tableName + ") by timestamp(" + Instant.ofEpochMilli(this.shareCdcContext.getCdcStartTs()) + "): " + sequenceFindByTs));
							} catch (Exception e) {
								String err = "Find sequence by timestamp failed, timestamp: " + this.shareCdcContext.getCdcStartTs() + "; Error: " + e.getMessage();
								handleFailed(err, e);
								return;
							}
						}
						if (readerResourceMap.get(tableName).firstTime) {
							readerResourceMap.get(tableName).firstTime = false;
							readCountDown.countDown();
							if (readCountDown.getCount() <= 0 && null != streamReadConsumer) {
								streamReadConsumer.streamReadStarted();
							}
							shareCdcContext.getObsLogger().info(logWrapper("Starting read '{}' log, sequence: {}"), tableName, readerResourceMap.get(tableName).sequence);
						}
						// Find hazelcast construct iterator
						Map<String, Object> filter = new HashMap<>();
						filter.put(ConstructRingBuffer.SEQUENCE_KEY, readerResourceMap.get(tableName).sequence);
						ConstructIterator<Document> iterator;
						if (null == readerResourceMap.get(tableName).iterator) {
							try {
								iterator = construct.find(filter);
								readerResourceMap.get(tableName).iterator = iterator;
								logger.info(logWrapper("Find by " + tableName + " filter: " + filter));
							} catch (Exception e) {
								String err = "Find from hazelcast construct " + construct.getClass().getName() + " failed, filter: " + filter + "; Error: " + e.getMessage();
								handleFailed(err);
								return;
							}
						}
						iterator = readerResourceMap.get(tableName).iterator;
						if (iterator == null) {
							String err = "Find hazelcast construct " + construct.getClass().getName() + " failed, iterator result is null, filter: " + filter;
							handleFailed(err);
							return;
						}
						List<Document> documents = new ArrayList<>();
						try {
							Document document = iterator.tryNext();
							if (null == document) {
								continue;
							}
							if (document.containsKey("type") && LogContent.LogContentType.SIGN.name().equals(document.getString("type"))) {
								continue;
							}
							documents.add(document);
						} catch (DistributedObjectDestroyedException e) {
							break;
						} catch (Exception e) {
							String err = "Find next failed, sequence: " + iterator.getSequence();
							handleFailed(err, e);
							break;
						}
						if (CollectionUtils.isEmpty(documents)) {
							continue;
						}
						if (logger.isDebugEnabled()) {
							logger.debug("Received log documents");
							shareCdcContext.getObsLogger().debug("Received log documents");
							documents.forEach(doc -> logger.debug("  " + doc.toJson()));
						}
						sequenceMap.put(tableName, iterator.getSequence());
						documents.forEach(doc -> enqueue(tapEventWrapper(doc)));
						if (readerResourceMap.get(tableName).firstData) {
							shareCdcContext.getObsLogger().info(logWrapper("Successfully read " + tableName + "'s first log data, will continue to read the log"));
							readerResourceMap.get(tableName).firstData = false;
						}
					}
				}
			} catch (Exception e) {
				String err = "Reader occur unknown error, will stop";
				handleFailed(err, e);
			}
		}

		public void close() {
			CommonUtils.ignoreAnyError(() -> {
				if (MapUtils.isNotEmpty(readerResourceMap)) {
					readerResourceMap.values().forEach(r -> {
						try {
							PersistenceStorage.getInstance().destroy(r.construct.getName());
						} catch (Exception ignored) {
						}
					});
				}
			}, ReadRunner.class.getSimpleName());
		}
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
		} finally {
			super.close();
		}
	}

	private static void handleData(Map<String, Object> data) {
		if (null == data) {
			return;
		}
		data.forEach((k, v) -> {
			if (v instanceof Binary) {
				byte[] bytes = ((Binary) v).getData();
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
}
