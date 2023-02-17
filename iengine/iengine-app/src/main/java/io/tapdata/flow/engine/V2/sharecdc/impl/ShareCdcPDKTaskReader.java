package io.tapdata.flow.engine.V2.sharecdc.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.HazelcastUtil;
import com.tapdata.constant.JSONUtil;
import com.tapdata.constant.Log4jUtil;
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
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcContext;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcTaskContext;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcTaskPdkContext;
import io.tapdata.flow.engine.V2.sharecdc.exception.ShareCdcUnsupportedException;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.BsonType;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Query;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * @author samuel
 * @Description Shared incremental task reader
 * @create 2022-02-17 15:13
 **/
public class ShareCdcPDKTaskReader extends ShareCdcHZReader implements Serializable {

	private static final long serialVersionUID = -8010918045236535239L;
	private static final String THREAD_NAME_PREFIX = "Share-CDC-Task-Reader-";
	private static final String LOG_PREFIX = "[Share CDC Task HZ Reader] - ";
	private static final long WAIT_FOR_AT_LEAST_ONE_LOG_INTERVAL_MS = 3000L;
	public static final String TAG = ShareCdcPDKTaskReader.class.getSimpleName();

	private ExecutorService readThreadPool;
	private Future<?> readFuture;
	private TaskDto logCollectorTaskDto;
	private HazelcastInstance hazelcastInstance;
	private HazelcastConstruct<Document> hazelcastConstruct;
	private AtomicLong headSequence = new AtomicLong();
	private List<String> tableNames;

	ShareCdcPDKTaskReader() {
		super();
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
			step = canShareCdc(step);
			initThreadPool();
			this.tableNames = NodeUtil.getTableNames(((ShareCdcTaskContext) shareCdcContext).getNode());
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
			throw new ShareCdcUnsupportedException("Not found any log collector task", true);
		}
		if (!shareCdcTaskId.containsKey(connections.getId())) {
			throw new ShareCdcUnsupportedException("Not found log collector task by connection id: " + connections.getId(), true);
		}
		this.logCollectorTaskDto = getLogCollectorSubTask();
		logger.info(logWrapper(++step, "Found log collector task: " + this.logCollectorTaskDto.getName()));

		// Get log collector external storage config
		String shareCDCExternalStorageId = connections.getShareCDCExternalStorageId();
		ExternalStorageDto logCollectorExternalStorage = clientMongoOperator.findOne(Query.query(where("_id").is(shareCDCExternalStorageId)), ConnectorConstant.EXTERNAL_STORAGE_COLLECTION, ExternalStorageDto.class);

		// Do not start ttl here
		logCollectorExternalStorage.setTtlDay(0);
		this.hazelcastConstruct = new ConstructRingBuffer<>(hazelcastInstance, ShareCdcUtil.getConstructName(this.logCollectorTaskDto), logCollectorExternalStorage);
		logger.info(logWrapper(++step, "Init hazelcast construct completed"));

		// Check cdc start timestamp is available in log storage
		try {
			if (this.shareCdcContext.getCdcStartTs().compareTo(0L) > 0) {
				if (this.hazelcastConstruct.isEmpty()) {
					logger.info(logWrapper(++step, "Log storage is empty and available to use"));
				} else {
					ConstructIterator<Document> iterator = this.hazelcastConstruct.find();
					Document firstLogDocument = iterator.peek();
					if (null == firstLogDocument) {
						logger.info(logWrapper(++step, "Log storage is empty and available to use"));
					} else {
						LogContent logContent = JSONUtil.map2POJO(firstLogDocument, new TypeReference<LogContent>() {
						});
						// First data's timestamp in storage must be lte task start cdc timestamp
						if (logContent.getTimestamp() > this.shareCdcContext.getCdcStartTs()) {
							throw new ShareCdcUnsupportedException("Log storage detected unusable, first log timestamp("
									+ Instant.ofEpochMilli(logContent.getTimestamp()) + ") is greater than task cdc start timestamp("
									+ Instant.ofEpochMilli(this.shareCdcContext.getCdcStartTs()) + ")", true);
						}
						logger.info(logWrapper(++step, "Log storage is available, first log timestamp: "
								+ Instant.ofEpochMilli(logContent.getTimestamp()) + ", task cdc start timestamp: "
								+ Instant.ofEpochMilli(this.shareCdcContext.getCdcStartTs())));
					}
				}
			} else {
				logger.info(logWrapper(++step, "Task incremental start timestamp less than 0, will read from first line"));
			}
		} catch (Exception e) {
			throw new ShareCdcUnsupportedException("Find from storage failed; Error: " + e.getMessage(), e, true);
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

	private void initThreadPool() {
		TaskDto taskDto = ((ShareCdcTaskContext) this.shareCdcContext).getTaskDto();
		this.readThreadPool = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>(), r -> {
			Log4jUtil.setThreadContext(((ShareCdcTaskContext) this.shareCdcContext).getTaskDto());
			return new Thread(r, THREAD_NAME_PREFIX + taskDto.getName() + "-" + taskDto.getId());
		});
	}

	@Override
	public void listen(BiConsumer<TapEvent, Object> logContentConsumer) throws Exception {
		logger.info(logWrapper("Starting listen share log storage..."));
		// start read threadS
		this.readFuture = this.readThreadPool.submit(this::read);

		try {
			poll(logContentConsumer);
		} catch (Exception e) {
			String err = "An internal error occurred, will close; Error: " + e.getMessage();
			this.close();
			throw new Exception(err, e);
		}
	}

	private void read() {
		logger.info(logWrapper("Starting read log from hazelcast construct: " + this.hazelcastConstruct.getName() + "(" + this.hazelcastConstruct.getType() + ")"));
		int step = 0;
		try {
			// Find first sequence by timestamp
			long sequenceFindByTs = this.hazelcastConstruct.findSequence(this.shareCdcContext.getCdcStartTs());
			this.headSequence.set(sequenceFindByTs);
			logger.info(logWrapper(++step, "Find sequence by timestamp(" + Instant.ofEpochMilli(this.shareCdcContext.getCdcStartTs()) + "): " + sequenceFindByTs));
		} catch (Exception e) {
			String err = "Find sequence by timestamp failed, timestamp: " + this.shareCdcContext.getCdcStartTs() + "; Error: " + e.getMessage();
			handleFailed(err, e);
			return;
		}

		// Find hazelcast construct iterator
		Map<String, Object> filter = new HashMap<>();
		filter.put(ConstructRingBuffer.SEQUENCE_KEY, this.headSequence);
		ConstructIterator<Document> iterator;
		try {
			iterator = this.hazelcastConstruct.find(filter);
			logger.info(logWrapper(++step, "Find by filter: " + filter));
		} catch (Exception e) {
			String err = "Find from hazelcast construct " + this.hazelcastConstruct.getClass().getName() + " failed, filter: " + filter + "; Error: " + e.getMessage();
			handleFailed(err);
			return;
		}
		if (iterator == null) {
			String err = "Find hazelcast construct " + this.hazelcastConstruct.getClass().getName() + " failed, iterator result is null, filter: " + filter;
			handleFailed(err);
			return;
		}
		Document document = null;
		AtomicBoolean firstDocument = new AtomicBoolean(true);

		// Loop iterator, produce message entity and put in queue
		while (running.get()) {
			try {
				document = iterator.next();
			} catch (DistributedObjectDestroyedException e) {
				break;
			} catch (Exception e) {
				String err = "Find next failed;+" + (document != null ? "Last document: " + document : "") + " Error: " + e.getMessage();
				handleFailed(err, e);
				break;
			}
			if (logger.isDebugEnabled()) {
				logger.debug("Received log document: " + document);
			}
			if (document == null) {
				continue;
			}
			String tableName = document.getString("fromTable");
			if (StringUtils.isNotBlank(tableName) && !tableNames.contains(tableName)) {
				continue;
			}
			enqueue(tapEventWrapper(document));
			if (firstDocument.compareAndSet(true, false)) {
				logger.info(logWrapper(++step, "Successfully read the first log data, will continue to read the log"));
			}
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
		return new ShareCDCReaderEvent(tapEvent, offsetObj);
	}

	private String logWrapper(String message) {
		return LOG_PREFIX + message;
	}

	private String logWrapper(int step, String message) {
		return LOG_PREFIX + "Step " + step + " - " + message;
	}

	@Override
	public void close() throws IOException {
		try {
			CommonUtils.ignoreAnyError(() -> {
				if (null != this.hazelcastConstruct) {
					this.hazelcastConstruct.destroy();
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

	private void handleData(Map<String, Object> data) {
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
