package io.tapdata.flow.engine.V2.sharecdc.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.HazelcastUtil;
import com.tapdata.constant.JSONUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.OperationType;
import com.tapdata.entity.sharecdc.LogContent;
import com.tapdata.entity.task.NodeUtil;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.ConstructIterator;
import io.tapdata.HazelcastConstruct;
import io.tapdata.common.sharecdc.ShareCdcUtil;
import io.tapdata.constructImpl.ConstructRingBuffer;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.flow.engine.V2.sharecdc.ShareCDCOffset;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcContext;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcTaskContext;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcTaskPdkContext;
import io.tapdata.flow.engine.V2.sharecdc.exception.ShareCdcUnsupportedException;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.BsonType;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Query;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

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
	public static final int QUEUE_CAPACITY = 100;
	private ExecutorService readThreadPool;
	private TaskDto logCollectorTaskDto;
	private HazelcastInstance hazelcastInstance;
	private List<String> tableNames;
	private int threadNum = DEFAULT_THREAD_NUMBER;
	private List<ReadRunner> readRunners;
	private Map<String, Long> sequenceMap;

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
			logger.info(logWrapper("Initializing share cdc reader..."));
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

		logger.info(logWrapper(++step, "Check tables start point valid"));
		for (String tableName : tableNames) {
			ConstructRingBuffer<Document> constructRingBuffer = new ConstructRingBuffer<>(hazelcastInstance, ShareCdcUtil.getConstructName(this.logCollectorTaskDto, tableName));
			// Check cdc start timestamp is available in log storage
			try {
				if (sequenceMap.containsKey(tableName)) {
					continue;
				}
				if (null != this.shareCdcContext.getCdcStartTs() && this.shareCdcContext.getCdcStartTs().compareTo(0L) > 0) {
					ConstructIterator<Document> iterator = constructRingBuffer.find();
					Document firstLogDocument = iterator.peek(3L, TimeUnit.SECONDS);
					if (null != firstLogDocument) {
						LogContent logContent = JSONUtil.map2POJO(firstLogDocument, new TypeReference<LogContent>() {
						});
						// First data's timestamp in storage must be lte task start cdc timestamp
						if (logContent.getType().equals(LogContent.LogContentType.DATA.name())
								&& logContent.getTimestamp() > this.shareCdcContext.getCdcStartTs()) {
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
	public void listen(BiConsumer<TapEvent, Object> logContentConsumer) throws Exception {
		logger.info(logWrapper("Starting listen share log storage..."));
		int size = Math.max(1, tableNames.size() / threadNum);
		List<List<String>> partitionTableNames = ListUtils.partition(tableNames, size);
		threadNum = partitionTableNames.size();
		this.readThreadPool = new ThreadPoolExecutor(threadNum + 1, threadNum + 1, 0L, TimeUnit.SECONDS, new SynchronousQueue<>());
		int index = 1;
		for (List<String> partitionTableName : partitionTableNames) {
			ReadRunner readRunner = new ReadRunner(
					index++,
					partitionTableName,
					hazelcastInstance,
					shareCdcContext
			);
			this.readThreadPool.submit(readRunner::read);
			this.readRunners.add(readRunner);
		}
		try {
			poll(logContentConsumer);
		} catch (Exception e) {
			String err = "An internal error occurred, will close; Error: " + e.getMessage();
			this.close();
			throw new Exception(err, e);
		}
	}

	private class ReadRunner {
		private int index;
		private ShareCdcContext shareCdcContext;
		private List<String> tableNames;
		private TaskDto taskDto;
		private AtomicBoolean running;
		private final Map<String, ShareCdcReaderResource> readerResourceMap;

		public ReadRunner(int index,
						  List<String> tableNames,
						  HazelcastInstance hazelcastInstance,
						  ShareCdcContext shareCdcContext) {
			this.index = index;
			this.tableNames = tableNames;
			if (shareCdcContext instanceof ShareCdcTaskContext) {
				this.taskDto = ((ShareCdcTaskContext) shareCdcContext).getTaskDto();
			}
			this.shareCdcContext = shareCdcContext;
			this.readerResourceMap = new HashMap<>();
			for (String tableName : tableNames) {
				ConstructRingBuffer<Document> constructRingBuffer = new ConstructRingBuffer<>(hazelcastInstance, ShareCdcUtil.getConstructName(logCollectorTaskDto, tableName));
				ShareCdcReaderResource shareCdcReaderResource = new ShareCdcReaderResource(constructRingBuffer);
				if (null != sequenceMap && sequenceMap.containsKey(tableName)) {
					shareCdcReaderResource.sequence(sequenceMap.get(tableName));
					if (logger.isDebugEnabled()) {
						logger.debug("Found share cdc table[{}] offset, sequence: {}", tableNames, sequenceMap.get(tableName));
					}
				}
				readerResourceMap.put(tableName, shareCdcReaderResource);
			}
		}

		public void read() {
			Thread.currentThread().setName(THREAD_NAME_PREFIX + "-" + taskDto.getName() + "-" + index);
			this.running = new AtomicBoolean(true);
			logger.info(logWrapper("Starting read log from hazelcast construct, tables: " + tableNames));
			try {
				while (this.running.get()) {
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
								logger.info(logWrapper("Find sequence in construct(" + tableName + ") by timestamp(" + Instant.ofEpochMilli(this.shareCdcContext.getCdcStartTs()) + "): " + sequenceFindByTs));
							} catch (Exception e) {
								String err = "Find sequence by timestamp failed, timestamp: " + this.shareCdcContext.getCdcStartTs() + "; Error: " + e.getMessage();
								handleFailed(err, e);
								return;
							}
						}
						if (readerResourceMap.get(tableName).firstTime) {
							readerResourceMap.get(tableName).firstTime = false;
							logger.info(logWrapper("Starting read '{}' log, sequence: {}"), tableName, readerResourceMap.get(tableName).sequence);
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
						Document document = null;

						try {
							document = iterator.tryNext();
						} catch (DistributedObjectDestroyedException e) {
							break;
						} catch (Exception e) {
							String err = "Find next failed. Last document: " + document;
							handleFailed(err, e);
							break;
						}
						if (logger.isDebugEnabled()) {
							logger.debug("Received log document: " + document);
						}
						if (document == null) {
							continue;
						}
						if (document.containsKey("type") && LogContent.LogContentType.SIGN.name().equals(document.getOrDefault("type", "").toString())) {
							continue;
						}
						sequenceMap.put(tableName, iterator.getSequence());
						enqueue(tapEventWrapper(document));
						if (readerResourceMap.get(tableName).firstData) {
							logger.info(logWrapper("Successfully read " + tableName + "'s first log data, will continue to read the log"));
							readerResourceMap.get(tableName).firstData = false;
						}
					}
				}
			} catch (Exception e) {
				String err = "Share cdc reader occur an unknown error";
				handleFailed(err, e);
			}
		}

		public void close() {
			CommonUtils.ignoreAnyError(() -> {
				if (MapUtils.isNotEmpty(readerResourceMap)) {
					readerResourceMap.values().forEach(r -> {
						try {
							r.construct.destroy();
						} catch (Exception ignored) {
						}
					});
				}
			}, ReadRunner.class.getSimpleName());
			this.running.compareAndSet(true, false);
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
		LogContent logContent = LogContent.valueOf(document);
		logDocumentVerify(document);

		if (OperationType.isDml(logContent.getOp())) {
			if (null == logContent.getBefore() && null == logContent.getAfter()) {
				throw new IllegalArgumentException("Log data unusable, op: " + logContent.getOp() + ", before and after are both null");
			}
			if (StringUtils.isBlank(logContent.getFromTable())) {
				throw new IllegalArgumentException("Log data unusable, op:" + logContent.getOp() + ", from table is null");
			}
		}

		TapEvent tapEvent = null;
		OperationType operationType = OperationType.fromOp(logContent.getOp());
		switch (operationType) {
			case INSERT:
				tapEvent = new TapInsertRecordEvent().init();
				if (MapUtils.isNotEmpty(logContent.getAfter())) {
					handleData(logContent.getAfter());
					((TapInsertRecordEvent) tapEvent).setAfter(logContent.getAfter());
				} else {
					throw new RuntimeException("Insert event must have after data: " + logContent);
				}
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
				if (MapUtils.isNotEmpty(logContent.getBefore())) {
					handleData(logContent.getBefore());
					((TapDeleteRecordEvent) tapEvent).setBefore(logContent.getBefore());
				} else {
					throw new RuntimeException("Delete event must have before data: " + logContent);
				}
				break;
			default:
				logger.warn("Found unrecognized operation type: " + logContent.getOp() + ", will skip it: " + logContent);
				break;
		}
		if (null == tapEvent) {
			return null;
		}
		((TapRecordEvent) tapEvent).setReferenceTime(logContent.getTimestamp());
		((TapRecordEvent) tapEvent).setTableId(logContent.getFromTable());
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
