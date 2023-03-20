package io.tapdata.flow.engine.V2.node.hazelcast.data;

import com.tapdata.constant.*;
import com.tapdata.entity.*;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.Source;
import io.tapdata.common.ClassScanner;
import io.tapdata.entity.SourceContext;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.exception.SourceException;
import io.tapdata.flow.engine.V2.monitor.MonitorManager;
import io.tapdata.flow.engine.V2.progress.SnapshotProgressManager;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 负责对接Jet的source节点
 *
 * @author jackin
 * @date 2021/3/10 9:17 PM
 **/
public class HazelcastTaskSource extends HazelcastDataBaseNode {

	private Logger logger = LogManager.getLogger(HazelcastTaskSource.class);

	private SourceContext sourceContext;

	protected List<Mapping> mappings;

	private Source source;

	private LinkedBlockingQueue<TapdataEvent> eventQueue = new LinkedBlockingQueue<>(10);

	private Context jetContext;

	private AtomicBoolean cdcSync = new AtomicBoolean(false);

	private List<String> baseURLs;

	private int retryTime = 5;

	protected ExecutorService sourceThreadPool;
	protected Future<?> sourceRunnerFuture;

	private SyncProgress syncProgress;
	private AtomicLong eventSerialNo = new AtomicLong(0L);

	//  private ShareCdcReader shareCdcReader;
	private MonitorManager monitorManager;
	private AtomicBoolean firstCdcEvent = new AtomicBoolean(false);
	private SnapshotProgressManager snapshotProgressManager;

	private TapdataEvent pendingEvent;

	protected DataProcessorContext dataProcessorContext;

	public HazelcastTaskSource(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
		this.dataProcessorContext = dataProcessorContext;
		ConfigurationCenter configurationCenter = dataProcessorContext.getConfigurationCenter();
		if (null != configurationCenter) {
			baseURLs = (List<String>) configurationCenter.getConfig(ConfigurationCenter.BASR_URLS);
			retryTime = (int) configurationCenter.getConfig(ConfigurationCenter.RETRY_TIME);
		}
		this.monitorManager = new MonitorManager();
	}

	@Override
	protected void doInit(@Nonnull Context context) throws Exception {
		try {
			TaskDto taskDto = dataProcessorContext.getTaskDto();
			Node<?> node = dataProcessorContext.getNode();
			ConfigurationCenter configurationCenter = dataProcessorContext.getConfigurationCenter();

			Log4jUtil.setThreadContext(taskDto);

			running.compareAndSet(false, true);
			super.doInit(context);
			this.jetContext = context;

			this.sourceThreadPool = new ThreadPoolExecutor(2, 2, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>());
			Class<?> sourceClazz = getSourceClazz();
			source = (Source) sourceClazz.newInstance();
//      initTimeZone();
			initJobOffset();
			source.sourceInit(sourceContext);
			monitorManager.startMonitor(MonitorManager.MonitorType.SOURCE_TS_MONITOR, taskDto, dataProcessorContext.getSourceConn());

			ConnectorContext connectorContext = new ConnectorContext();
			connectorContext.setJob(sourceContext.getJob());
			connectorContext.setJobSourceConn(sourceContext.getSourceConn());
			connectorContext.setJobTargetConn(sourceContext.getTargetConn());

			// 启动同步线程
//      startWorker();
			sourceContext.getJob().setJobErrorNotifier(this::errorHandle);
		} catch (Exception e) {
			logger.error("An internal error occurred: " + e.getMessage(), e);
			throw e;
		}
	}

	private void initJobOffset() {
		try {
			TaskDto taskDto = dataProcessorContext.getTaskDto();
			this.syncProgress = foundSyncProgress(taskDto.getAttrs());

			if (this.syncProgress == null || StringUtils.isBlank(this.syncProgress.getOffset()) || this.sourceContext == null || this.sourceContext.getJob() == null) {
				return;
			}
			eventSerialNo.set(syncProgress.getEventSerialNo());
			String offset = this.syncProgress.getOffset();
			Map<String, Object> offsetMap;
			try {
				offsetMap = JSONUtil.json2Map(offset);
			} catch (IOException e) {
				throw new RuntimeException("Convert offset string to map failed. Offset string: " + offset + "; Error: " + e.getMessage(), e);
			}
			this.sourceContext.getJob().setOffsetStr(offset);
			this.sourceContext.getJob().setOffset(offsetMap);
//      new OffsetConvertUtil(this.sourceContext.getJob(), sourceConn).convert();
			logger.info("Init job offset result: " + this.sourceContext.getJob().getOffset() + "(" + this.sourceContext.getJob().getOffset().getClass().getName() + ")");
		} catch (Exception e) {
			throw new RuntimeException("Init job offset failed; Error: " + e.getMessage() + "\n" + Log4jUtil.getStackString(e), e);
		}
	}

	@NotNull
	private Class<?> getSourceClazz() {
		String databaseType = dataProcessorContext.getSourceConn().getDatabase_type();
		Class<?> sourceClazz = ClassScanner.getClazzByDatabaseType(databaseType, ClassScanner.SOURCE);
		return sourceClazz;
	}

//  private void initTimeZone() throws Exception {
//    Connections sourceConn = dataProcessorContext.getSourceConn();
//    if (sourceConn != null) {
//      // source timezone
//      String timezone = TimeZoneUtil.getZoneIdByDatabaseType(sourceConn);
//      String sysTimezone = TimeZoneUtil.getSysZoneIdByDatabaseType(sourceConn);
//      String sysDate = TimeZoneUtil.getDateByDatabaseType(sourceConn, timezone, sysTimezone);
//      try {
//        sourceConn.setZoneId(ZoneId.of(timezone));
//        sourceConn.setSysZoneId(ZoneId.of(sysTimezone));
//        sourceConn.setDbCurrentTime(sysDate);
//      } catch (Exception e) {
//        logger.warn("Set {} time zone error: {}, use system default time zone: {}", sourceConn.getDatabase_type(), timezone, ZoneId.systemDefault());
//      }
//      // custom timezone
//      sourceConn.initCustomTimeZone();
//      logger.info("Source connection time zone: " + sourceConn.getCustomZoneId());
//      ConnectorJobManager.processSourceDBSyncTime(sourceContext.getJob(), timezone, sysDate, clientMongoOperator);
//    }
//
//    Connections targetConn = dataProcessorContext.getTargetConn();
//    if (targetConn != null) {
//      // target timezone
//      String targetTimezone = TimeZoneUtil.getZoneIdByDatabaseType(targetConn);
//      String targetSysTimezone = TimeZoneUtil.getSysZoneIdByDatabaseType(targetConn);
//      String targetSysDate = TimeZoneUtil.getDateByDatabaseType(targetConn, targetTimezone, targetSysTimezone);
//      try {
//        targetConn.setZoneId(ZoneId.of(targetTimezone));
//        targetConn.setSysZoneId(ZoneId.of(targetSysTimezone));
//        targetConn.setDbCurrentTime(targetSysDate);
//      } catch (Exception e) {
//        logger.warn("Set {} time zone error: {}, use system default time zone: {}", targetConn.getDatabase_type(), targetTimezone, ZoneId.systemDefault());
//      }
//      targetConn.initCustomTimeZone();
//      logger.info("Target connection time zone: " + targetConn.getCustomZoneId());
//    }
//  }

//  private void startWorker() {
//    SubTaskDto subTaskDto = dataProcessorContext.getSubTaskDto();
//    Node<?> node = dataProcessorContext.getNode();
//    this.sourceRunnerFuture = this.sourceThreadPool.submit(() -> {
//      Thread.currentThread().setName("source-read-thread-" + subTaskDto.getName() + "-" + node.getName());
//
//      Log4jUtil.setThreadContext(subTaskDto);
//
//      try {
//        if (need2InitialSync(this.syncProgress)) {
//          try {
//            cdcSync.compareAndSet(true, false);
//            // Milestone-READ_SNAPSHOT-RUNNING
//            MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.READ_SNAPSHOT, MilestoneStatus.RUNNING);
//            snapshotProgressManager = new SnapshotProgressManager(subTaskDto, clientMongoOperator, source);
//            snapshotProgressManager.startStatsSnapshotEdgeProgress(node);
//            source.initialSync();
//            sourceContext.getMessageConsumer().accept(null);
//            snapshotProgressManager.close();
//            // Milestone-READ_SNAPSHOT-FINISH
//            MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.READ_SNAPSHOT, MilestoneStatus.FINISH);
//          } catch (SourceException e) {
//            // Milestone-READ_SNAPSHOT-ERROR
//            MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.READ_SNAPSHOT, MilestoneStatus.ERROR, e.getMessage() + "\n  " + Log4jUtil.getStackString(e));
//            logger.error("An internal error occurred: " + e.getMessage(), e);
//            throw e;
//          }
//        }
//
//        if (need2CDC()) {
//          try {
//            // Milestone-READ_CDC_EVENT-RUNNING
//            MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.READ_CDC_EVENT, MilestoneStatus.RUNNING);
//            doCdc();
//          } catch (Exception e) {
//            // Milestone-READ_CDC_EVENT-ERROR
//            MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.READ_CDC_EVENT, MilestoneStatus.ERROR, e.getMessage() + "\n  " + Log4jUtil.getStackString(e));
//            logger.error("An internal error occurred: " + e.getMessage(), e);
//            throw e;
//          }
//        }
//      } catch (Throwable throwable) {
//        error = throwable;
//      } finally {
//        running.compareAndSet(true, false);
//      }
//    });
//  }

//  private void doCdc() throws Exception {
//    Node<?> node = dataProcessorContext.getNode();
//    cdcSync.compareAndSet(false, true);
//    if (node.isLogCollectorNode()) {
//      // Mining tasks force traditional increments
//      doNormalCdc();
//    } else {
//      try {
//        // Try to start with share cdc
//        doShareCdc();
//      } catch (ShareCdcUnsupportedException e) {
//        if (e.isContinueWithNormalCdc()) {
//          // If share cdc is unavailable, and continue with normal cdc is true
//          logger.info("Share cdc unusable, will use normal cdc mode, reason: " + e.getMessage());
//          doNormalCdc();
//        } else {
//          // Stop task with error status
//          // TODO internal error task
//          throw e;
//        }
//      } catch (Exception e) {
//        // Stop task with error status
//        // TODO internal error task
//        throw e;
//      }
//    }
//  }

//  private void doShareCdc() throws Exception {
//    ShareCdcTaskContext shareCdcTaskContext = new ShareCdcTaskContext(getCdcStartTs(), sourceContext.getConfigurationCenter(),
//      dataProcessorContext.getSubTaskDto(), dataProcessorContext.getNode(), dataProcessorContext.getSourceConn());
//    logger.info("Starting incremental sync, use share log storage...");
//    // Init share cdc reader, if unavailable, will throw ShareCdcUnsupportedException
//    this.shareCdcReader = ShareCdcFactory.shareCdcReader(ReaderType.TASK_HAZELCAST, shareCdcTaskContext);
//    // Start listen message entity from share storage log
//    this.shareCdcReader.listen(messageEntity -> {
//      List<MessageEntity> msgs = new ArrayList<>();
//      msgs.add(messageEntity);
//      this.sourceContext.getMessageConsumer().accept(msgs);
//    });
//  }

	private Long getCdcStartTs() {
		Long cdcStartTs;
		Connections sourceConn = dataProcessorContext.getSourceConn();
		try {
			if (null != this.syncProgress && null != this.syncProgress.getEventTime() && this.syncProgress.getEventTime().compareTo(0L) > 0) {
				cdcStartTs = this.syncProgress.getEventTime();
			} else {
				ZoneId customZoneId = sourceConn.getCustomZoneId();
				String dbCurrentTime = sourceConn.getDbCurrentTime();
				if (null != customZoneId && StringUtils.isNotBlank(dbCurrentTime)) {
					try {
						String dateFormat = DateUtil.determineDateFormat(dbCurrentTime);
						long epochSecond = LocalDateTime.parse(dbCurrentTime, DateTimeFormatter.ofPattern(dateFormat)).atZone(customZoneId).toInstant().getEpochSecond();
						cdcStartTs = epochSecond * 1000L;
					} catch (Exception e) {
						throw new RuntimeException("Get timestamp from db current time failed, time: " + dbCurrentTime + ", time zone: " + customZoneId + "; Error: " + e.getMessage(), e);
					}
				} else {
					cdcStartTs = 0L;
				}
			}
		} catch (Exception e) {
			throw new RuntimeException("Get cdc start ts failed; Error: " + e.getMessage(), e);
		}
		return cdcStartTs;
	}

	private void doNormalCdc() {
		logger.info("Starting incremental sync, use connection source: " + source.getClass().getName());
		source.increamentalSync();
	}

	@Override
	public boolean complete() {
		try {
			TaskDto taskDto = dataProcessorContext.getTaskDto();
			Log4jUtil.setThreadContext(taskDto);
			TapdataEvent dataEvent = null;
			if (pendingEvent != null) {
				dataEvent = pendingEvent;
				pendingEvent = null;
			} else {
				dataEvent = eventQueue.poll(5, TimeUnit.SECONDS);
			}

			if (dataEvent != null) {
				if (!offer(dataEvent)) {
					pendingEvent = dataEvent;
					return false;
				}
				dmlCount(dataEvent);
			}

			if (!running()) {
				if (error != null) {
					logger.error("Running task {} failed {}", taskDto.getId(), error.getMessage(), error);
					throw new RuntimeException(error);
				}
				return true;
			}
		} catch (Exception e) {
			logger.error("Source sync failed {}.", e.getMessage(), e);
			throw new SourceException(e, true);
		}

		return false;
	}

	@NotNull
	public AtomicInteger dmlCount(TapdataEvent dataEvent) {
		AtomicInteger dmlCount = new AtomicInteger(0);
		if (null == dataEvent) {
			return dmlCount;
		}
		final MessageEntity messageEntity = dataEvent.getMessageEntity();
		if (messageEntity != null) {
			if (OperationType.isDml(messageEntity.getOp())) {
				dmlCount.incrementAndGet();
			}
		} else {
			final TapEvent tapEvent = dataEvent.getTapEvent();
			if (tapEvent instanceof TapRecordEvent) {
				dmlCount.incrementAndGet();
			}
		}

//    Metrics.metric(JetDataFlowClient.stageIdToMetricName(node.getId(), JetDataFlowClient.JET_METRIC_DELIMITER_OUTPUT)).increment(dmlCount.get());
		Map<String, Long> total = sourceContext.getJob().getStats().getTotal();
		total.put(Stats.SOURCE_RECEIVED_FIELD_NAME, total.getOrDefault(Stats.SOURCE_RECEIVED_FIELD_NAME, 0L) + dmlCount.get());
		return dmlCount;
	}

//  @Override
//  public void close() throws Exception {
//    Optional.ofNullable(sourceContext).ifPresent(context -> context.getJob().setStatus(ConnectorConstant.STOPPING));
//    if (source != null) {
//      source.sourceStop(false);
//    }
//    if (this.shareCdcReader != null && this.shareCdcReader.isRunning()) {
//      try {
//        this.shareCdcReader.close();
//        logger.info("Share cdc reader stop completed");
//      } catch (IOException ignore) {
//      }
//    }
//    ExecutorUtil.shutdown(this.sourceThreadPool, 60, TimeUnit.SECONDS);
//    monitorManager.close();
//
//    // should call super since there are some clean up jobs in super
//    super.close();
//  }

	private boolean running() {
		return running.get();
	}

	public io.tapdata.entity.Context getOldTapdataContext() {
		return sourceContext;
	}

	public LinkedBlockingQueue<TapdataEvent> getEventQueue() {
		return eventQueue;
	}

	private void findEdgeSnapshotProgress() {

	}
}
