package io.tapdata.common;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.ConnectorContext;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.constant.MessageUtil;
import com.tapdata.constant.OffsetUtil;
import com.tapdata.constant.TapdataOffset;
import com.tapdata.constant.TapdataShareContext;
import com.tapdata.entity.DataQualityTag;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.Job;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.RelateDataBaseTable;
import com.tapdata.entity.RelateDatabaseField;
import com.tapdata.entity.RuntimeInfo;
import com.tapdata.entity.SyncStageEnum;
import com.tapdata.entity.TapLog;
import com.tapdata.entity.UnSupportedDDL;
import io.tapdata.ConverterProvider;
import io.tapdata.cdc.event.CdcEventHandler;
import io.tapdata.debug.DebugConstant;
import io.tapdata.debug.DebugProcessor;
import io.tapdata.exception.DDLException;
import io.tapdata.exception.SourceException;
import io.tapdata.schema.SchemaList;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author tapdata
 * @date 25/03/2018
 */
public class MemoryMessageConsumer {

	private Logger logger = LogManager.getLogger(getClass());

	private final static int FLUSH_MESSAGES_TO_QUEUE_TIMEOUT_TS = 3 * 1000;

	private ConnectorContext context;

	private LinkedBlockingQueue<List<MessageEntity>> messageQueue;

	private ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1);

	private ScheduledFuture<?> scheduledFuture;

	private List<MessageEntity> cacheMsgs;

	private String syncType = ConnectorConstant.SYNC_TYPE_INITIAL_SYNC;

	private Lock lock = new ReentrantLock();

	private boolean isLib = false;

	private ConverterProvider converterProvider;


	private DebugProcessor debugProcessor;

	private ProcessorHandler processorHandler;

	private SettingService settingService;

	private CdcEventHandler cdcEventHandler;

	private AtomicLong tapEventSerialNo;

	private MessageEntity prevProcessedMsg;

	private TapdataShareContext tapdataShareContext;

	private int readBatchSize;

	private long lastFlushMessageTs = 0L;

	public Runnable scheduledRunnable() {
		Runnable runnable = () -> {
			Thread.currentThread().setName("Memory flush msg runner-[" + context.getJob().getId() + "]-" + context.getJob().getName());
			setThreadContext(context.getJob());
			boolean locked = false;
			try {
				final long currentTimeMillis = System.currentTimeMillis();
				locked = lock.tryLock();
				if (locked) {
					if (lastFlushMessageTs > 0 && currentTimeMillis - lastFlushMessageTs < FLUSH_MESSAGES_TO_QUEUE_TIMEOUT_TS) {
						return;
					}
					flushMsgsToQueue();
				}
			} catch (Throwable e) {
				context.getJob().jobError(e, true, OffsetUtil.getSyncStage(cacheMsgs), logger, ConnectorConstant.WORKER_TYPE_CONNECTOR,
						TapLog.ERROR_0004.getMsg(), null, e.getMessage());
			} finally {
				if (locked) {
					lock.unlock();
				}
			}

			if (!context.isRunning()) {
				scheduledFuture.cancel(false);
				executorService.shutdown();

				processorHandler.stop();
			}
		};
		return runnable;
	}

	public MemoryMessageConsumer(ConnectorContext context,
								 LinkedBlockingQueue<List<MessageEntity>> messageQueue,
								 ConverterProvider converterProvider,
								 DebugProcessor debugProcessor,
								 SettingService settingService,
								 CdcEventHandler cdcEventHandler,
								 TapdataShareContext tapdataShareContext) {
		this.context = context;

		this.messageQueue = messageQueue;

		this.readBatchSize = context.getJob().getCdcConcurrency() ? context.getJob().getReadBatchSize() * context.getJob().getTransformerConcurrency() : context.getJob().getReadBatchSize();
		cacheMsgs = new ArrayList<>(readBatchSize);

		this.converterProvider = converterProvider;

		scheduledFuture = executorService.scheduleWithFixedDelay(scheduledRunnable(), FLUSH_MESSAGES_TO_QUEUE_TIMEOUT_TS, FLUSH_MESSAGES_TO_QUEUE_TIMEOUT_TS, TimeUnit.MILLISECONDS);
		this.debugProcessor = debugProcessor;

		this.settingService = settingService;
		this.processorHandler = new ProcessorHandler(
				context,
				debugProcessor,
				settingService,
				this::enqueue,
				context.getCacheService()
		);
		this.cdcEventHandler = cdcEventHandler;

		tapEventSerialNo = new AtomicLong();
		long processedSerialNo = getProcessedSerialNo(context.getJob().getOffset());
		tapEventSerialNo.set(processedSerialNo);
		this.tapdataShareContext = tapdataShareContext;
	}

	public void sourceDataHandler(Object sourceData) {
		Map<String, Long> total = context.getJob().getStats().getTotal();
		if (sourceData instanceof List) {
			List<MessageEntity> msgs;
			try {
				msgs = (List<MessageEntity>) sourceData;
			} catch (Exception e) {
				logger.error(TapLog.ERROR_MQ_0002.getMsg(), e.getMessage(), e);
				return;
			}

			int processableMessageSize = msgs.size();
			context.getJob().getStats().getTotal().put("source_received", total.getOrDefault("source_received", 0L) + (long) processableMessageSize);
			try {
				if (converterProvider != null) {
					valuesConverter(msgs);
				}
			} catch (Exception e) {
				logger.error(TapLog.ERROR_MQ_0004.getMsg(), e.getMessage(), e);
			}

			if (!context.isRunning()) {
				return;
			}

			if (logger.isTraceEnabled()) {
				logger.trace("Converted messages batch size {}, messages", msgs.size(), msgs);
			}
			this.pushMsgs(msgs);

		} else if (sourceData instanceof MessageEntity) {
			MessageEntity msg;

			try {
				msg = (MessageEntity) sourceData;

				if (MessageUtil.isProcessableMessage(msg)) {
					long sourceReceived = 0L;
					sourceReceived++;
					context.getJob().getStats().getTotal().put("source_received", total.getOrDefault("source_received", 0L) + sourceReceived);
				}
			} catch (Exception e) {
				logger.error(TapLog.ERROR_MQ_0003.getMsg(), e.getMessage(), e);
				return;
			}

			try {
				if (converterProvider != null) {
					valueConverter(msg);
				}
			} catch (Exception e) {
				logger.error(TapLog.ERROR_MQ_0005.getMsg(), e.getMessage(), e);
			}

			if (!context.isRunning()) {
				return;
			}

			if (logger.isTraceEnabled()) {
				logger.trace("Converted message {}", msg);
			}
			this.intervalPushMsg(msg);
		} else if (sourceData == null) {
			this.intervalPushMsg(null);
		}
	}

	private List<MessageEntity> valuesConverter(List<MessageEntity> msgs) {
		if (CollectionUtils.isNotEmpty(msgs)) {
			List<MessageEntity> newMsgs = new ArrayList<>();
			for (MessageEntity msg : msgs) {
				if (!context.isRunning()) {
					break;
				}
				valueConverter(msg);
				newMsgs.add(msg);
			}
			return newMsgs;
		} else {
			return msgs;
		}
	}

	private boolean shouldUseTapValue(RelateDatabaseField field) {
		// only use Tap Value containers in data migration
		if (!Objects.equals(context.getJob().getMapping_template(), ConnectorConstant.MAPPING_TEMPLATE_CLUSTER_CLONE)) {
			return false;
		}

		// only v2(with type mapping setting) can use Tap Value
		if (!"v2".equalsIgnoreCase(context.getJob().getTransformModelVersion())) {
			return false;
		}

		if (field.getTapType() == null) {
			return false;
		}
		// only add tap value convert for mysql and the tap value converter is provided
		boolean source = StringUtils.equalsAny(
				context.getJobSourceConn().getDatabase_type(),
				DatabaseTypeEnum.MYSQL.getType(),
				DatabaseTypeEnum.POSTGRESQL.getType(),
				DatabaseTypeEnum.ALIYUN_POSTGRESQL.getType(),
				DatabaseTypeEnum.GREENPLUM.getType(),
				DatabaseTypeEnum.ORACLE.getType(),
				DatabaseTypeEnum.MSSQL.getType(),
				DatabaseTypeEnum.ALIYUN_MSSQL.getType(),
				DatabaseTypeEnum.CLICKHOUSE.getType(),
				DatabaseTypeEnum.HIVE.getType(),
				DatabaseTypeEnum.HANA.getType(),
				DatabaseTypeEnum.ELASTICSEARCH.getType(),
				DatabaseTypeEnum.MQ.getType(),
				DatabaseTypeEnum.KUNDB.getType(),
				DatabaseTypeEnum.ADB_MYSQL.getType(),
				DatabaseTypeEnum.ALIYUN_MYSQL.getType(),
				DatabaseTypeEnum.ADB_POSTGRESQL.getType(),
				DatabaseTypeEnum.GAUSSDB200.getType(),
				DatabaseTypeEnum.TIDB.getType()
		);
		boolean target = StringUtils.equalsAny(
				context.getJobTargetConn().getDatabase_type(),
				DatabaseTypeEnum.MYSQL.getType(),
				DatabaseTypeEnum.POSTGRESQL.getType(),
				DatabaseTypeEnum.ALIYUN_POSTGRESQL.getType(),
				DatabaseTypeEnum.GREENPLUM.getType(),
				DatabaseTypeEnum.ORACLE.getType(),
				DatabaseTypeEnum.MSSQL.getType(),
				DatabaseTypeEnum.ALIYUN_MSSQL.getType(),
				DatabaseTypeEnum.CLICKHOUSE.getType(),
				DatabaseTypeEnum.HIVE.getType(),
				DatabaseTypeEnum.HANA.getType(),
				DatabaseTypeEnum.ELASTICSEARCH.getType(),
				DatabaseTypeEnum.MQ.getType(),
				DatabaseTypeEnum.KUNDB.getType(),
				DatabaseTypeEnum.ADB_MYSQL.getType(),
				DatabaseTypeEnum.ALIYUN_MYSQL.getType(),
				DatabaseTypeEnum.ADB_POSTGRESQL.getType(),
				DatabaseTypeEnum.GAUSSDB200.getType(),
				DatabaseTypeEnum.TIDB.getType()
		);

		return source && target;
	}

	private void valueConverter(MessageEntity msg) {

		if (msg == null) {
			return;
		}

		if (StringUtils.isNotBlank(msg.getTableName()) &&
				(MapUtils.isNotEmpty(msg.getAfter()) || MapUtils.isNotEmpty(msg.getBefore()))) {
			String fromTable = msg.getTableName();
			Map<String, Object> after = msg.getAfter();
			Map<String, Object> before = msg.getBefore();
			if (MapUtils.isNotEmpty(after)) {
				valueConverter(after, fromTable, context.getJob(), msg.getOffset());
				msg.setAfter(after);
			}

			if (MapUtils.isNotEmpty(before)) {
				valueConverter(before, fromTable, context.getJob(), msg.getOffset());
				msg.setBefore(before);
			}
		}
	}

	private void valueConverter(Map<String, Object> dataMap, String fromTable, Job job, Object offset) {
		List<RelateDataBaseTable> tables = context.getJobSourceConn().getSchema().get("tables");
		Map<String, RelateDatabaseField> fieldMap = ((SchemaList<String, RelateDataBaseTable>) tables).getFieldMap(fromTable);
		for (Map.Entry<String, Object> entry : dataMap.entrySet()) {
			Object value = entry.getValue();
			String fieldName = entry.getKey();

			RelateDatabaseField relateDatabaseField = fieldMap.get(fieldName);
			if (relateDatabaseField == null) {
				continue;
			}
			ConverterUtil.ConvertVersion conversionVer = ConverterUtil.ConvertVersion.V1;
			try {
				if (value != null && shouldUseTapValue(relateDatabaseField)) {
					logger.debug("Convert field {}, field data type is {}, the type of value is {}",
							relateDatabaseField.getField_name(),
							relateDatabaseField.getData_type(),
							value.getClass().getName()
					);
					conversionVer = ConverterUtil.ConvertVersion.V1;
					value = converterProvider.convertToTapValue(context, relateDatabaseField, value);
				} else {
					value = converterProvider.sourceValueConverter(relateDatabaseField, value);
				}
				// convert all value to tap value according to the tap type
				dataMap.put(fieldName, value);
			} catch (Exception e) {
				String syncStage = offset == null ? SyncStageEnum.SNAPSHOT.name() : SyncStageEnum.CDC.name();
				String err = e.getMessage() + "; \nstack: " + Log4jUtil.getStackString(e);
				if (!job.jobError(e, false, syncStage, logger, ConnectorConstant.WORKER_TYPE_CONNECTOR, err, null)) {
					break;
				}
			}
		}
	}

	public void intervalPushMsg(MessageEntity msg) {
		while (context.isRunning()) {
			try {
				lock.lock();
				if (msg != null) {
					cacheMsgs.add(msg);
					if (cacheMsgs.size() >= readBatchSize) {
						flushMsgsToQueue();
					}
				} else {
					flushMsgsToQueue();
					if (logger.isDebugEnabled()) {
						logger.debug("Pushed empty messages to queue");
					}
					while (context.isRunning()) {
						if (messageQueue.offer(new ArrayList<>(0), 1L, TimeUnit.SECONDS)) {
							break;
						}
					}
				}
				break;
			} catch (InterruptedException e) {
				logger.info(TapLog.JOB_LOG_0008.getMsg());
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e1) {
					// abort it.
				}
			} catch (Exception e) {
				if (e instanceof SourceException) {
					SourceException srcException = (SourceException) e;
					boolean needStop = srcException.isNeedStop();
					if (needStop) {
						context.getJob().jobError(srcException, true, OffsetUtil.getSyncStage(msg), logger, ConnectorConstant.WORKER_TYPE_CONNECTOR,
								"IntervalPushMsg push message error, {}", null, e.getMessage());
						break;
					} else {
						logger.warn("IntervalPushMsg push message error, {}, stacks: {}", e.getMessage(), Log4jUtil.getStackString(srcException));
					}
				} else {
					if (!context.getJob().jobError(e, false, OffsetUtil.getSyncStage(msg), logger, ConnectorConstant.WORKER_TYPE_CONNECTOR,
							"IntervalPushMsg push message error, {}", null, e.getMessage())) {
						break;
					}
				}
			} finally {
				lock.unlock();
			}
		}
	}

	public void flushMsgsToQueue() throws InterruptedException {
		if (CollectionUtils.isNotEmpty(cacheMsgs)) {

//			while (messageQueue.size() > 20 && context.isRunning()) {
//				logger.info(TapLog.JOB_LOG_0004.getMsg(), messageQueue.size());
//				Thread.sleep(2000);
//			}
			// handle ddl
			cacheMsgs = handleDDL(cacheMsgs);

			// save cdc events
			saveCdcEvents(cacheMsgs);

			cleanTapd8Field();

			if (needToSerial()) {

				serialDispatcheMessage(cacheMsgs);
				cacheMsgs.clear();
			} else {
				dispatchMessage(cacheMsgs);
			}

		}
	}

	public void serialDispatcheMessage(List<MessageEntity> messages) throws InterruptedException {
		for (MessageEntity cacheMsg : messages) {
			Object offset = cacheMsg.getOffset();
			if (offset != null && offset instanceof TapdataOffset) {

				TapdataOffset tapdataOffset = (TapdataOffset) offset;
				tapdataOffset.setTapEventSerialNo(tapEventSerialNo.incrementAndGet());
				if (MessageUtil.isProcessableMessage(cacheMsg)) {
					waitingInSerialMode(cacheMsg, prevProcessedMsg);
				}
			}

			List<MessageEntity> list = new ArrayList<>();
			list.add(cacheMsg);


			dispatchMessage(list);

			prevProcessedMsg = cacheMsg;

			if (!context.isRunning()) {
				break;
			}
		}
	}

	public void dispatchMessage(List<MessageEntity> messages) {
		MessageUtil.dispatcherMessage(messages, false, processableMsgs -> {

			if (!context.isRunning()) {
				return;
			}

			try {
				processorHandler.handle(
						processableMsgs
				);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}, msg -> {
			List<MessageEntity> msgs = new ArrayList<>(1);
			msgs.add(msg);
			enqueue(msgs);
		});
	}

	public void enqueue(List<MessageEntity> msgs) {
		MessageConsumerTopLimitationManager manager = MessageConsumerTopLimitationManager.getInstance();
		if (!manager.shouldLimit()) {
			offerMessages(msgs);
		} else {
			manager.pushMessage(msgs, this::offerMessages);
		}

	}

	public void offerMessages(List<MessageEntity> msgs) {
		if (msgs.isEmpty()) {
			return;
		}
		try {
			boolean offered = false;
			while (context.isRunning() && !offered) {
				offered = messageQueue.offer(new ArrayList<>(msgs), 500, TimeUnit.MILLISECONDS);
				if (!offered) {
					logger.info(TapLog.JOB_LOG_0004.getMsg(), messageQueue.size());
				}
			}
		} catch (InterruptedException ignore) {
		}
	}

	public void pushMsgs(List<MessageEntity> msgs) {

		try {
			if (CollectionUtils.isNotEmpty(msgs)) {
				for (MessageEntity msg : msgs) {
					intervalPushMsg(msg);
				}
			} else if (msgs != null) {
				intervalPushMsg(null);
			}
		} catch (Exception e) {
			logger.error("PushMsgs push message fail, {}", e, e);
		}
	}

	private void waitingInSerialMode(MessageEntity currMsg, MessageEntity prevMsg) throws InterruptedException {

		if (prevMsg == null) {
			return;
		}

		TapdataOffset currTapdataOffset = (TapdataOffset) currMsg.getOffset();
		if (currTapdataOffset == null) {
			return;
		}

		if (TapdataOffset.SYNC_STAGE_SNAPSHOT.equals(currTapdataOffset.getSyncStage())) {
			return;
		}

		long processedSerialNo = getProcessedSerialNo(tapdataShareContext.getProcessedOffset());

		Object prevMsgOffset = prevMsg.getOffset();
		if (prevMsgOffset instanceof TapdataOffset) {
			TapdataOffset prevTapdataOffset = (TapdataOffset) prevMsgOffset;

			int logCount = 1;
			long waitingStartTs = System.currentTimeMillis();
			long logStartTs = System.currentTimeMillis();
			while (processedSerialNo < prevTapdataOffset.getTapEventSerialNo() && context.isRunning()) {
				Thread.sleep(1L);

				long endTs = System.currentTimeMillis();
				if (endTs - logStartTs > (logCount * 5000L)) {
					logger.info("Waiting for message {} apply to target stage.", prevMsg);
					logCount++;
					logStartTs = System.currentTimeMillis();
				}

				if (endTs - waitingStartTs > 120 * 1000L) {
					logger.warn("Waiting message apply to target stage was timeout, will pass msg {} to target.", currMsg);
					break;
				}

				processedSerialNo = getProcessedSerialNo(tapdataShareContext.getProcessedOffset());
			}
		}
	}

	private long getProcessedSerialNo(Object offset) {
		long processedSerialNo = 0L;
		if (offset != null && offset instanceof TapdataOffset) {
			TapdataOffset tapdataOffset = (TapdataOffset) offset;
			processedSerialNo = tapdataOffset.getTapEventSerialNo();
			if (tapdataOffset.getOffset() != null && tapdataOffset.getOffset() instanceof TapdataOffset) {
				long tapEventSerialNo = ((TapdataOffset) tapdataOffset.getOffset()).getTapEventSerialNo();
				processedSerialNo = tapEventSerialNo > processedSerialNo ? tapEventSerialNo : processedSerialNo;
			}
		}
		return processedSerialNo;
	}

	private boolean needToSerial() {
		if (!context.getJob().getIsSerialMode()) {
			return false;
		}
		// check msgs offset's sync stage=cdc
		if (cacheMsgs.stream().anyMatch(msg -> {
			Object offset = msg.getOffset();
			return offset instanceof TapdataOffset && offset != null
					&& TapdataOffset.SYNC_STAGE_CDC.equals(((TapdataOffset) offset).getSyncStage());
		})) {
			return true;
		}

		return false;
	}

	public String getSyncType() {
		return syncType;
	}

	public void setSyncType(String syncType) {
		this.syncType = syncType;
	}

	public void setLib(boolean lib) {
		isLib = lib;
	}

	private static void setThreadContext(Job job) {
		ThreadContext.clearAll();

		ThreadContext.put("userId", job.getUser_id());
		ThreadContext.put("jobId", job.getId());
		ThreadContext.put("jobName", job.getName());
		ThreadContext.put("app", "connector");
		if (StringUtils.isNotBlank(job.getDataFlowId())) {
			ThreadContext.put(DebugConstant.SUB_DATAFLOW_ID, job.getDataFlowId());
		}
	}

	public DebugProcessor getDebugProcessor() {
		return debugProcessor;
	}

	public void setDebugProcessor(DebugProcessor debugProcessor) {
		this.debugProcessor = debugProcessor;
	}

	private void saveCdcEvents(List<MessageEntity> messageEntities) {
		if (cdcEventHandler == null || settingService == null) {
			return;
		}

		if (CdcEventUtil.needSaveCdcEvent(settingService)) {
			cdcEventHandler.saveCdcEvents(messageEntities);
		}
	}

	/**
	 * 删除__tapd8字段
	 */
	private void cleanTapd8Field() {
		if (CollectionUtils.isEmpty(cacheMsgs)) {
			return;
		}

		for (MessageEntity cacheMsg : cacheMsgs) {
			if (MapUtils.isNotEmpty(cacheMsg.getBefore())) {
				cacheMsg.getBefore().remove(DataQualityTag.SUB_COLUMN_NAME);
			}

			if (MapUtils.isNotEmpty(cacheMsg.getAfter())) {
				cacheMsg.getAfter().remove(DataQualityTag.SUB_COLUMN_NAME);
			}
		}
	}


	/**
	 * 处理队列中的ddl语句
	 *
	 * @param cacheMsgs
	 */
	private List<MessageEntity> handleDDL(List<MessageEntity> cacheMsgs) {

		try {
			checkConfirmDDl();
		} catch (DDLException e) {
			context.getJob().jobError(e, true, SyncStageEnum.CDC.getSyncStage(), logger, ConnectorConstant.WORKER_TYPE_CONNECTOR,
					e.getMessage(), null);
		}
		return cacheMsgs;
	}

	private void checkConfirmDDl() {
		RuntimeInfo runtimeInfo = context.getJob().getRuntimeInfo();
		if (runtimeInfo == null) {
			runtimeInfo = new RuntimeInfo(true, new ArrayList<>());
			context.getJob().setRuntimeInfo(runtimeInfo);
		}

		boolean ddlConfirm = runtimeInfo.getDdlConfirm();
		List<UnSupportedDDL> unSupportedDDLList = runtimeInfo.getUnSupportedDDLS();
		if (unSupportedDDLList == null) {
			runtimeInfo.setUnSupportedDDLS(new ArrayList<>());
		}

		if (!ddlConfirm) {
			throw new DDLException("There are still unhandled ddl. Please restart the job to confirm");
		}
	}
}
