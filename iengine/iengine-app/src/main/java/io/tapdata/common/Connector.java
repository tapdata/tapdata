package io.tapdata.common;

import com.tapdata.cache.ICacheService;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.constant.MappingUtil;
import com.tapdata.constant.TapdataShareContext;
import com.tapdata.entity.Connections;
import com.tapdata.entity.JavaScriptFunctions;
import com.tapdata.entity.Job;
import com.tapdata.entity.Mapping;
import com.tapdata.entity.MessageEntity;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.processor.Processor;
import io.tapdata.ConverterProvider;
import io.tapdata.Source;
import io.tapdata.cdc.event.CdcEventHandler;
import io.tapdata.debug.DebugProcessor;
import io.tapdata.milestone.MilestoneJobService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class Connector {

	private Logger logger = LogManager.getLogger(Connector.class);

	private ExecutorService executorService;

	private List<Runnable> runnableList;

	private Job job;

	private Connections connections;

	private Connections targetConn;

	private LinkedBlockingQueue<List<MessageEntity>> messageQueue;

	// source lib
	private Source source;

	private final static int FETCH_SIZE = 25000;

	private List<Processor> processors;

	private SettingService settingService;

	private String threadName;

	private ConverterProvider converterProvider;

	private String baseUrl;

	private String accessCode;

	private int restRetryTime;

	private String userId;

	private Integer roleId;

	private DebugProcessor debugProcessor;

	private List<JavaScriptFunctions> javaScriptFunctions;

	private ClientMongoOperator clientMongoOperator;

	private ICacheService cacheService;

	private CdcEventHandler cdcEventHandler;

	private TapdataShareContext tapdataShareContext;

	private MilestoneJobService milestoneJobService;

	private boolean isCloud;

	private ConfigurationCenter configurationCenter;

	public Connector(Job job, LinkedBlockingQueue<List<MessageEntity>> messageQueue, ClientMongoOperator clientMongoOperator, Connections connections, ConverterProvider converterProvider, ICacheService cacheService, ConfigurationCenter configurationCenter) {
		executorService = Executors.newCachedThreadPool();
		runnableList = new ArrayList<>();
		this.job = job;
		this.messageQueue = messageQueue;
		this.connections = connections;
		this.converterProvider = converterProvider;
		this.clientMongoOperator = clientMongoOperator;
		this.cacheService = cacheService;
		this.configurationCenter = configurationCenter;

		Map<String, List<Mapping>> tableMappings = MappingUtil.adaptMappingsToTableMappings(job.getMappings());
		job.setTableMappings(tableMappings);
	}
//
//  public List<Runnable> getRunnableList() {
//    return runnableList == null ? new ArrayList<>() : runnableList;
//  }
//
//  public void start() {
//    Optional.ofNullable(runnableList).ifPresent(runnables -> {
//      for (Runnable runnable : runnables) {
//        run(runnable);
//      }
//    });
//    if (StringUtils.equalsAny(job.getStatus(),
//      ConnectorConstant.SCHEDULED, ConnectorConstant.RUNNING
//    )) {
//      job.setStatus(ConnectorConstant.RUNNING);
//      // run source lib
//      runSource();
//    } else {
//      logger.info("Job status is {} in memory, cannot running, please wait and retry.", job.getStatus());
//    }
//  }
//
//  private void run(Runnable runnable) {
//    Optional.ofNullable(runnable).ifPresent(runner -> {
//      checkExecutor();
//      executorService.execute(runnable);
//    });
//  }
//
//  private void checkExecutor() {
//    if (executorService == null) {
//      executorService = Executors.newCachedThreadPool();
//    }
//  }
//
//  public boolean stop() throws InterruptedException {
//    if (ConnectorConstant.RUNNING.equals(job.getStatus())) {
//      setJobStatus(ConnectorConstant.STOPPING);
//    }
//
//    stopEmbeddedEngineIfNeed();
//
//    stopSource(false);
//
//    long waitMills = job.getStopWaitintMills() == -1 ? Long.MAX_VALUE : job.getStopWaitintMills();
//    new ExecutorUtil().shutdown(executorService, this::forceStop, waitMills, TimeUnit.MILLISECONDS);
//
//    clearMessageQueue();
//
//    if (CollectionUtils.isNotEmpty(processors)) {
//      for (Processor processor : processors) {
//        if (processor != null) {
//          processor.stop();
//        }
//      }
//    }
//
//    Optional.ofNullable(cdcEventHandler).ifPresent(ceh -> ceh.stop(false));
//    return true;
//  }
//
//  public boolean forceStop() {
//    if (ConnectorConstant.STOPPING.equals(job.getStatus()) ||
//      ConnectorConstant.RUNNING.equals(job.getStatus())) {
//      setJobStatus(ConnectorConstant.FORCE_STOPPING);
//    }
//
//    stopSource(true);
//
//    clearMessageQueue();
//
//    if (CollectionUtils.isNotEmpty(processors)) {
//      for (Processor processor : processors) {
//        if (processor != null) {
//          processor.stop();
//        }
//      }
//    }
//
//    Optional.ofNullable(executorService).ifPresent(executor -> {
//      if (!executor.isTerminated()) {
//        executor.shutdownNow();
//      }
//    });
//
//    Optional.ofNullable(cdcEventHandler).ifPresent(ceh -> ceh.stop(true));
//
//    return true;
//  }
//
//  public Job getJob() {
//    return job;
//  }
//
//  public void setJob(Job job) {
//    this.job = job;
//  }
//
//  private void clearMessageQueue() {
//    Optional.ofNullable(messageQueue).ifPresent(messageQueue -> messageQueue.clear());
//  }
//
//  private void setJobStatus(String status) {
//    Optional.ofNullable(job).ifPresent(job -> job.setStatus(status));
//  }
//
//  private void stopEmbeddedEngineIfNeed() {
//    Optional.ofNullable(runnableList).ifPresent(runnables -> {
//      for (Runnable runnable : runnables) {
//        if (runnable instanceof EmbeddedEngine) {
//          ((EmbeddedEngine) runnable).stop();
//        } else if (runnable instanceof io.debezium.embedded.EmbeddedEngine) {
//          ((io.debezium.embedded.EmbeddedEngine) runnable).stop();
//        }
//      }
//    });
//  }
//
//  public void setTargetConn(Connections targetConn) {
//    this.targetConn = targetConn;
//  }
//
//  private void runSource() {
//    if (source != null) {
//      Runnable libRunnable = () -> {
//        Logger logger = LogManager.getLogger(source.getClass());
//        if (StringUtils.isNotBlank(threadName)) {
//          Thread.currentThread().setName(threadName);
//        }
//        Log4jUtil.setThreadContext(job);
//        String syncType = job.getSync_type();
//        TapdataOffset tapdataOffset;
//        SourceContext sourceContext;
//        try {
//          ConnectorContext connectorContext = new ConnectorContext(job, connections, clientMongoOperator, targetConn, processors, cacheService, configurationCenter);
//          connectorContext.setMilestoneService(this.milestoneJobService);
//
//          MemoryMessageConsumer messageConsumer;
//          try {
//            messageConsumer = ConnectorJobManager.buildConsumer(connectorContext,
//              messageQueue,
//              converterProvider,
//              debugProcessor,
//              settingService,
//              cdcEventHandler,
//              tapdataShareContext);
//            messageConsumer.setLib(true);
//
//            if (job.getOffset() != null) {
//              tapdataOffset = (TapdataOffset) job.getOffset();
//            } else {
//              tapdataOffset = new TapdataOffset(TapdataOffset.SYNC_STAGE_SNAPSHOT, new HashMap<>());
//            }
//            Object offset = null;
//            if (tapdataOffset != null) {
//              offset = tapdataOffset.getOffset();
//            }
//
//            sourceContext = new SourceContext(job, logger, offset, settingService, connections, targetConn, messageConsumer::sourceDataHandler, baseUrl, accessCode, restRetryTime,
//              userId, roleId, debugProcessor, javaScriptFunctions, clientMongoOperator, cacheService, converterProvider, milestoneJobService, isCloud, connectorContext.getConfigurationCenter());
//
//            source.sourceInit(sourceContext);
//
//          } catch (Exception e) {
//            MilestoneUtil.updateMilestone(milestoneJobService, MilestoneStage.INIT_CONNECTOR, MilestoneStatus.ERROR, e.getMessage() + "  \n" + Log4jUtil.getStackString(e));
//            throw new SourceException(e, true);
//          }
//
//          // Milestone-INIT_CONNECTOR-FINISH
//          MilestoneUtil.updateMilestone(milestoneJobService, MilestoneStage.INIT_CONNECTOR, MilestoneStatus.FINISH);
//
//          List<Mapping> originalMappings = null;
//
//          if (job.needInitial()) {
//
//            // 添加创建索引消息
//            IndicesUtil.generateCreateMessageEntityList(
//              job.getNeedToCreateIndex(),
//              targetConn,
//              connections.getSchema().get("tables"),
//              job.getMappings(),
//              messageConsumer::dispatchMessage,
//              false
//            );
//
//            // initial sync
//            if (job.isOnlyInitialAddMapping()) {
//              originalMappings = new ArrayList<>(job.getMappings());
//              job.setMappings(job.getAddInitialMapping());
//            }
//
//            DatabaseTypeEnum databaseTypeEnum = DatabaseTypeEnum.fromString(sourceContext.getSourceConn().getDatabase_type());
//            try {
//              logger.info("Start {} initial sync", connections.getDatabase_type());
//
//              // Milestone-READ_SNAPSHOT-RUNNING
//              updateReadSnapshotMilestone(databaseTypeEnum, MilestoneStatus.RUNNING, "");
//
//              source.initialSync();
//
//              // initial sync end point
//              messageConsumer.pushMsgs(new ArrayList<>(0));
//
//              if (job.isOnlyInitialAddMapping()) {
//                job.setMappings(originalMappings);
//              }
//
//              // Milestone-READ_SNAPSHOT-FINISH
//              updateReadSnapshotMilestone(databaseTypeEnum, MilestoneStatus.FINISH, "");
//            } catch (SourceException e) {
//
//              String errMsg = String.format("Read snapshot failed, job name: %s, database type: %s, connection name: %s, err: %s, stacks: %s",
//                job.getName(), connections.getDatabase_type(), connections.getName(), e.getMessage(), Log4jUtil.getStackString(e));
//              // Milestone-READ_SNAPSHOT-ERROR
//              updateReadSnapshotMilestone(databaseTypeEnum, MilestoneStatus.ERROR, errMsg);
//
//              if (e.isNeedStop()) {
//                job.jobError(e, true, TapdataOffset.SYNC_STAGE_SNAPSHOT, logger, ConnectorConstant.WORKER_TYPE_CONNECTOR,
//                  TapLog.CONN_ERROR_0028.getMsg(), null, connections.getDatabase_type(),
//                  SupportConstant.INITIAL_SYNC, e.isNeedStop(), e.getMessage());
//                return;
//              } else {
//                logger.warn(TapLog.CONN_ERROR_0028.getMsg(),
//                  connections.getDatabase_type(),
//                  SupportConstant.INITIAL_SYNC,
//                  e.isNeedStop(), e.getMessage());
//              }
//            }
//          }
//
//          if (ConnectorConstant.SYNC_TYPE_CDC.equalsIgnoreCase(syncType) ||
//            ConnectorConstant.SYNC_TYPE_INITIAL_SYNC_CDC.equalsIgnoreCase(syncType)) {
//
//            // check no pk table(s)
//            if (!job.getNoPrimaryKey()) {
//              ConnectorJobManager.tableNoPKWarn(connections, job.getMappings());
//            }
//
//            // incremental sync
//            try {
//              logger.info("Start {} cdc", connections.getDatabase_type());
//
//              messageConsumer.setSyncType(ConnectorConstant.SYNC_TYPE_CDC);
//
//              // Milestone-READ_CDC_EVENT-RUNNING
//              MilestoneUtil.updateMilestone(milestoneJobService, MilestoneStage.READ_CDC_EVENT, MilestoneStatus.RUNNING);
//
//              source.increamentalSync();
//
//            } catch (SourceException e) {
//              if (e.isNeedStop()) {
//                job.jobError(e, true, TapdataOffset.SYNC_STAGE_CDC, logger, ConnectorConstant.WORKER_TYPE_CONNECTOR,
//                  TapLog.CONN_ERROR_0028.getMsg(), null, connections.getDatabase_type(),
//                  SupportConstant.INCREAMENTAL_SYNC, e.isNeedStop(), e.getMessage());
//              } else {
//                logger.warn(TapLog.CONN_ERROR_0028.getMsg(), connections.getDatabase_type(),
//                  SupportConstant.INCREAMENTAL_SYNC, e.isNeedStop(), e.getMessage());
//              }
//            }
//          }
//        } catch (Exception e) {
//          if (e instanceof SourceException) {
//            if (((SourceException) e).isNeedStop()) {
//              job.jobError(e, true, "", logger, ConnectorConstant.WORKER_TYPE_CONNECTOR,
//                TapLog.CONN_ERROR_0029.getMsg(), null, e.getMessage(), ((SourceException) e).isNeedStop());
//            } else {
//              logger.warn(TapLog.CONN_ERROR_0029.getMsg(), e.getMessage(), ((SourceException) e).isNeedStop());
//            }
//          } else {
//            String error = "Run source failed, cause: " + e.getMessage() + "  \n" + Log4jUtil.getStackString(e) + ". Will stop job";
//            job.jobError(e, true, "", logger, ConnectorConstant.WORKER_TYPE_CONNECTOR, error, null);
//          }
//
//        } finally {
//          ThreadContext.clearAll();
//        }
//
//      };
//
//      run(libRunnable);
//    }
//  }
//
//  public Source getSource() {
//    return source;
//  }
//
//  public Connector setSource(Source source) {
//    this.source = source;
//    return this;
//  }
//
//  public void setProcessors(List<Processor> processors) {
//    this.processors = processors;
//  }
//
//  private void stopSource(boolean force) {
//    if (source != null) {
//      source.sourceStop(force);
//    }
//  }
//
//  public void setSettingService(SettingService settingService) {
//    this.settingService = settingService;
//  }
//
//  public void setThreadName(String threadName) {
//    this.threadName = threadName;
//  }
//
//  public void setBaseUrl(String baseUrl) {
//    this.baseUrl = baseUrl;
//  }
//
//  public void setAccessCode(String accessCode) {
//    this.accessCode = accessCode;
//  }
//
//  public void setRestRetryTime(int restRetryTime) {
//    this.restRetryTime = restRetryTime;
//  }
//
//  public void setUserId(String userId) {
//    this.userId = userId;
//  }
//
//  public void setRoleId(Integer roleId) {
//    this.roleId = roleId;
//  }
//
//  public void setDebugProcessor(DebugProcessor debugProcessor) {
//    this.debugProcessor = debugProcessor;
//  }
//
//  public void setJavaScriptFunctions(List<JavaScriptFunctions> javaScriptFunctions) {
//    this.javaScriptFunctions = javaScriptFunctions;
//  }
//
//  public void setCdcEventHandler(CdcEventHandler cdcEventHandler) {
//    this.cdcEventHandler = cdcEventHandler;
//  }
//
//  public TapdataShareContext getTapdataShareContext() {
//    return tapdataShareContext;
//  }
//
//  public void setTapdataShareContext(TapdataShareContext tapdataShareContext) {
//    this.tapdataShareContext = tapdataShareContext;
//  }
//
//  public void setMilestoneJobService(MilestoneJobService milestoneJobService) {
//    this.milestoneJobService = milestoneJobService;
//  }
//
//  private void updateReadSnapshotMilestone(DatabaseTypeEnum databaseTypeEnum, MilestoneStatus milestoneStatus, String errMsg) {
//    String databaseType = databaseTypeEnum.getType();
//    if (!StringUtils.equalsAny(databaseType,
//      DatabaseTypeEnum.DB2.getType(),
//      DatabaseTypeEnum.POSTGRESQL.getType(),
//      DatabaseTypeEnum.ALIYUN_POSTGRESQL.getType(),
//      DatabaseTypeEnum.ADB_POSTGRESQL.getType(),
//      DatabaseTypeEnum.GREENPLUM.getType(),
//      DatabaseTypeEnum.GBASE8S.getType(),
//      DatabaseTypeEnum.GAUSSDB200.getType()
//    )) {
//      MilestoneUtil.updateMilestone(milestoneJobService, MilestoneStage.READ_SNAPSHOT, milestoneStatus, errMsg);
//    }
//  }

}
