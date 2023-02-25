package io.tapdata.Schedule;

import com.tapdata.constant.*;
import com.tapdata.entity.*;
import com.tapdata.entity.dataflow.StageRuntimeStats;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.common.*;
import io.tapdata.dao.MessageDao;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.filter.BurstFilter;
import org.apache.logging.log4j.core.filter.CompositeFilter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.*;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * Created by tapdata on 29/03/2018.
 */
@Component
@DependsOn("connectorManager")
public class TransformerManager {

	private Logger logger = LogManager.getLogger(TransformerManager.class);

	private static ExecutorService executor = Executors.newCachedThreadPool();

	private static Map<String, ExecutorService> RUNNING_JOB_THREAD_POOL_MAP = new ConcurrentHashMap<>();
	private static Map<String, Future<?>> RUNNING_JOB_THREAD_FUTURE_MAP = new ConcurrentHashMap<>();

//  private static final Map<String, Transformer> JOB_MAP = new ConcurrentHashMap<>();

	private static final ConcurrentHashMap<String, Map<String, Long>> JOB_STATS = new ConcurrentHashMap<>();

	private static final ConcurrentHashMap<String, List<StageRuntimeStats>> JOB_STAGE_STATS = new ConcurrentHashMap<>();

	private static final String TRANSFORMER = "Transformer";

	// unit: seconds
	private static final int STATS_FAIL_RETRY_INTERVAL = 10;

	private String instanceNo = "tapdata-agent-transformer";

	@Autowired
	private ClientMongoOperator clientMongoOperator;

	@Autowired
	private ClientMongoOperator pingClientMongoOperator;

	private Map<String, EventExecutor> eventExecutors = new HashMap<>();

	private WarningMaker warningMaker;

	@Autowired
	private ConfigurationCenter configCenter;

	@Autowired
	private SettingService settingService;

	@Autowired
	private MessageDao messageDao;

	private String version;

	private String originLog4jFilterInterval;

	private String originLog4jFilterRate;

	private int availableProcessors;

	private ExecutorService stopJobThreadPool;
	private String stopJobThreadName = "Stop Transformer Runner Thread-%s-[%s]";
//  private ConcurrentHashMap<String, TransformerStopJob> stopJobMap = new ConcurrentHashMap<>();

	private String jobTags = "";
	private String region = "";
	private String zone = "";
	private String tapdataWorkDir;

	@PostConstruct
	public void init() {
		try {

			availableProcessors = Runtime.getRuntime().availableProcessors();
			this.tapdataWorkDir = System.getenv("TAPDATA_WORK_DIR");

			instanceNo = ConfigurationCenter.processId;
			if (StringUtils.isBlank(instanceNo)) {
				instanceNo = AgentUtil.readAgentId(tapdataWorkDir);
			}

			version = VersionCheck.getVersion();

			try {
				jobTags = configCenter.getConfig(ConfigurationCenter.JOB_TAGS).toString();
				region = configCenter.getConfig(ConfigurationCenter.REGION).toString();
				zone = configCenter.getConfig(ConfigurationCenter.ZONE).toString();
			} catch (Exception ignore) {
			}

			Map<String, Object> params = new HashMap<>();
			params.put("process_id", instanceNo);
			params.put("worker_type", ConnectorConstant.WORKER_TYPE_TRANSFORMER);

			List<Worker> workers = clientMongoOperator.find(params, ConnectorConstant.WORKER_COLLECTION, Worker.class);

			if (CollectionUtils.isNotEmpty(workers)) {
				params.clear();
				for (Worker worker : workers) {
					String id = worker.getId();
					params.put("id", id);

					Map<String, Object> updateData = new HashMap<>();
					updateData.put("createTime", new Date());

					if (StringUtils.isNoneBlank(region, zone)) {
						if (StringUtils.isNoneBlank(region, zone)) {
							Map<String, String> platformInfo = new HashMap<>();
							platformInfo.put("region", region);
							platformInfo.put("zone", zone);
							updateData.put("platformInfo", platformInfo);
						}
					}

					clientMongoOperator.updateAndParam(params, updateData, ConnectorConstant.WORKER_COLLECTION);
				}
			}

			warningMaker = new WarningMaker(clientMongoOperator);

			if (isAdminRole()) {

				eventExecutors.put(Event.EventName.WARN_EMAIL.name, new WarningEmailEventExecutor(settingService, configCenter));
				eventExecutors.put(Event.EventName.DDL_WARN_EMAIL.name, new DDLConfirmEmailEventExecutor(settingService));
				eventExecutors.put(Event.EventName.CDC_LAG_WARN_EMAIL.name, new WarningEmailEventExecutor(settingService, configCenter));
				eventExecutors.put(Event.EventName.TEST_CONNECTION_EMAIL.name, new WarningEmailEventExecutor(settingService, configCenter));
				eventExecutors.put(Event.EventName.JOB_OPERATION_NOTICE_EMAIL.name, new WarningEmailEventExecutor(settingService, configCenter));
				eventExecutors.put(Event.EventName.AGENT_NOTICE_EMAIL.name, new WarningEmailEventExecutor(settingService, configCenter));
				eventExecutors.put(Event.EventName.TIMEOUT_TXN_EMAIL.name, new WarningEmailEventExecutor(settingService, configCenter));
			}

			stopJobThreadPool = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
					0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>());

			logger.info("Transformer init variables\n - process id: {}\n - job tags: {}\n - region: {}\n - zone: {}\n - worker dir: {}",
					this.instanceNo, this.jobTags, this.region, this.zone, tapdataWorkDir);

		} catch (Exception e) {
			logger.error("Init transformer manager failed {}.", e.getMessage(), e);
		}
	}

	/**
	 * 轮询监听待启动的Job
	 */
//  @Scheduled(fixedDelay = 2000L)
//  public void scanJob() {
//    Thread.currentThread().setName(String.format(ConnectorConstant.START_JOB_THREAD, TRANSFORMER, instanceNo.substring(instanceNo.length() - 6)));
//    try {
//      LinkedBlockingQueue<String> runningJobs = messageDao.getRunningJobsQueue();
//      String runningJobId = runningJobs.poll();
//      Job runningJob = runningJob(runningJobId);
//      if (runningJob != null && !JOB_MAP.containsKey(runningJob.getId())) {
//        MilestoneJobService milestoneJobService = null;
//        Transformer runnable = null;
//        runningJob.setClientMongoOperator(clientMongoOperator);
//        JobCustomerLogger jobCustomerLogger = new JobCustomerLogger(runningJob.getDataFlowId(), runningJob.getName(), clientMongoOperator);
//        runningJob.setJobCustomerLogger(jobCustomerLogger);
//        try {
//          if (ConnectorConstant.SCHEDULED.equals(runningJob.getStatus())) {
//            runningJob.setStatus(ConnectorConstant.RUNNING);
//          }
//          Log4jUtil.setThreadContext(runningJob);
//
//          Stats stats = runningJob.getStats();
//          if (stats == null) {
//            stats = new Stats();
//            runningJob.setStats(stats);
//          }
//
//          // 获取源端和目标的数据库信息
//          Connections sourceConn = runningJob.getConn(true, clientMongoOperator, null);
//          if (CollectionUtils.isNotEmpty(runningJob.getStages()) &&
//            runningJob.getStages().stream().anyMatch(stage -> Stage.StageTypeEnum.LOG_COLLECT.getType().equals(stage.getType()))) {
//            sourceConn.setDatabase_type(DatabaseTypeEnum.LOG_COLLECT.getType());
//          }
//          Connections targetConn = runningJob.getConn(false, clientMongoOperator, sourceConn);
//
//          LinkedBlockingQueue<List<MessageEntity>> messageQueue = messageDao.createJobMessageQueue(runningJob);
//
//          // 获取数据过滤规则
//          Map<String, List<DataRules>> dataRulesMap = MongodbUtil.getDataRules(clientMongoOperator, targetConn);
//          if (dataRulesMap == null) {
//            logger.error(TapLog.CONN_ERROR_0025.getMsg());
//          }
//
//          // 初始化里程碑业务类
//          try {
//            milestoneJobService = MilestoneFactory.getJobMilestoneService(runningJob, clientMongoOperator);
//          } catch (Exception e) {
//            logger.warn("Init job milestone failed, id: {}, name: {}, err: {}", runningJob.getId(), runningJob.getName(), e.getMessage(), e);
//          }
//
//          // Milestone-INIT_TRANSFORMER-RUNNING
//          MilestoneUtil.updateMilestone(milestoneJobService, MilestoneStage.INIT_TRANSFORMER, MilestoneStatus.RUNNING);
//
//          DebugContext debugContext = new DebugContext(runningJob, clientMongoOperator, sourceConn, targetConn);
//          DebugProcessor debugProcessor = new DebugProcessor(debugContext);
//
//          jobReset(runningJob, sourceConn, targetConn, clientMongoOperator);
//
//          TapdataShareContext tapdataShareContext = messageDao.getCacheTapdataShareContext(runningJob);
//          logger.info(TapLog.JOB_LOG_0001.getMsg(), runningJob.getName());
//          runnable = TransformerJobManager.prepare(
//            runningJob,
//            sourceConn,
//            targetConn,
//            messageQueue,
//            settingService,
//            clientMongoOperator,
//            dataRulesMap,
//            debugProcessor,
//            messageDao.getCacheService(),
//            tapdataShareContext,
//            (Boolean) configCenter.getConfig(ConfigurationCenter.IS_CLOUD),
//            milestoneJobService
//          );
//          if (runnable != null && runningJob.isRunning()) {
//            startedJob(runningJob, false);
//            ExecutorService transformerExecutorService = Executors.newFixedThreadPool(1);
//            RUNNING_JOB_THREAD_POOL_MAP.put(runningJobId, transformerExecutorService);
//            JOB_STATS.put(runningJob.getId(), new HashMap<>(stats.getTotal()));
//            List<StageRuntimeStats> stageRuntimeStats = stats.getStageRuntimeStats();
//            if (stageRuntimeStats != null) {
//              JOB_STAGE_STATS.put(runningJob.getId(), stageRuntimeStats);
//            }
//            JOB_MAP.put(runningJob.getId(), runnable);
//            // Milestone-INIT_TRANSFORMER-FINISH
//            MilestoneUtil.updateMilestone(milestoneJobService, MilestoneStage.INIT_TRANSFORMER, MilestoneStatus.FINISH);
//            logger.info(TapLog.TRAN_LOG_0033.getMsg());
//            Future<?> submit = TransformerJobManager.startTransform(runnable, transformerExecutorService);
//            RUNNING_JOB_THREAD_FUTURE_MAP.put(runningJobId, submit);
//          } else {
//            String errMsg = String.format("Start job[%s] failed, transformer thread failed to init", runningJob.getName());
//
//            // Milestone-INIT_TRANSFORMER-ERROR
//            MilestoneUtil.updateMilestone(milestoneJobService, MilestoneStage.INIT_TRANSFORMER, MilestoneStatus.ERROR, errMsg);
//
//            startedJob(runningJob, true);
//
//            throw new RuntimeException(errMsg);
//          }
//
//        } catch (ManagementException e) {
//          logger.warn(TapLog.JOB_WARN_0004.getMsg(), runningJob.getName(), Log4jUtil.getStackString(e));
//        } catch (Exception e) {
//          String errMsg = String.format(TapLog.JOB_ERROR_0007.getMsg(), runningJob.getName(), e.getMessage());
//
//          // Milestone-INIT_TRANSFORMER-ERROR
//          MilestoneUtil.updateMilestone(milestoneJobService, MilestoneStage.INIT_TRANSFORMER, MilestoneStatus.ERROR, errMsg);
//          jobCustomerLogger.error(ErrorCodeEnum.FATAL_INIT_TRANSFORMER_FAILED);
//          logger.error(JobCustomerLogger.CUSTOMER_ERROR_LOG_PREFIX + errMsg, e);
//          jobError(runningJob.getId());
//          flushJobStats(runnable);
//          interruptJob(runningJob);
//
//        } finally {
//          ThreadContext.clearAll();
//        }
//      }
//    } catch (Exception e) {
//      logger.error("scanJob happen exception:", e);
//    }
//  }
//
//  @Scheduled(fixedDelay = 2000L)
//  private void scanStopJob() {
//    Thread.currentThread().setName(String.format(ConnectorConstant.STOP_JOB_THREAD, TRANSFORMER, instanceNo.substring(instanceNo.length() - 6)));
//    // stop the paused job
//    try {
//      List<Job> stoppedJobs = stoppingJob(false);
//      if (CollectionUtils.isNotEmpty(stoppedJobs)) {
//        stoppedJobs.forEach(job -> {
//          String jobId = job.getId();
//          if (JOB_MAP.containsKey(jobId)) {
//            try {
//              Log4jUtil.setThreadContext(job);
//
//              stopJob(job, false);
//
//            } catch (Exception e) {
//              logger.error(TapLog.JOB_ERROR_0003.getMsg(), job.getName(), e.getMessage(), e);
//              Optional.ofNullable(JOB_MAP).ifPresent(jobMap -> {
//                if (jobMap.containsKey(jobId)) {
//                  Transformer transformer = jobMap.get(jobId);
//                  if (transformer != null) {
//                    transformer.forceStop();
//                  }
//                  removeJobMapIfNeed(jobId);
//                  stoppedJob(job);
//                }
//              });
//            } finally {
//              ThreadContext.clearAll();
//            }
//          } else {
//            stoppedJob(job);
//            logger.info(TapLog.JOB_LOG_0003.getMsg(), job.getName());
//          }
//        });
//      }
//    } catch (Exception e) {
//      logger.error("Scan stopping job failed {}", e.getMessage(), e);
//    }
//
//    if (MapUtils.isNotEmpty(stopJobMap)) {
//      Iterator<String> iterator = stopJobMap.keySet().iterator();
//      while (iterator.hasNext()) {
//        try {
//          String jobId = iterator.next();
//          TransformerStopJob transformerStopJob = stopJobMap.get(jobId);
//          Job job = transformerStopJob.getJob();
//          Log4jUtil.setThreadContext(job);
//
//          // if stop connector runner
//          if (transformerStopJob.getFuture().isDone()) {
//            logger.info(TapLog.JOB_LOG_0011.getMsg(), job.getName());
//            Optional.ofNullable(JOB_MAP.get(jobId)).ifPresent(this::flushJobStats);
//            logger.info(TapLog.JOB_LOG_0012.getMsg(), job.getName());
//
//            messageDao.removeJobMessageQueue(jobId);
//            messageDao.removeJobCache(jobId);
//            messageDao.removeTapdataShareContext(jobId);
//
//            removeJobMapIfNeed(jobId);
//            logger.info(TapLog.JOB_LOG_0013.getMsg(), job.getName());
//
//            stoppedJob(job);
//            logger.info(TapLog.JOB_LOG_0003.getMsg(), job.getName());
//
//            iterator.remove();
//          }
//        } finally {
//          ThreadContext.clearAll();
//        }
//      }
//    }
//  }
//
//  @Scheduled(fixedDelay = 2000L)
//  private void scanErrorJob() {
//    Thread.currentThread().setName(String.format(ConnectorConstant.ERROR_JOB_THREAD, TRANSFORMER, instanceNo.substring(instanceNo.length() - 6)));
//    try {
//      // if the job happen runtime exception, remove the jobmap
//      for (Iterator<Map.Entry<String, Transformer>> it = JOB_MAP.entrySet().iterator(); it.hasNext(); ) {
//        Map.Entry<String, Transformer> entry = it.next();
//        Transformer transformer = entry.getValue();
//        TransformerContext context = transformer.getContext();
//        if (context != null) {
//          Job job = context.getJob();
//          if (ConnectorConstant.ERROR.equals(job.getStatus())) {
//            Log4jUtil.setThreadContext(job);
//            logger.info("Found the error job: {}[{}], will stop it", job.getName(), job.getId());
//            stopJob(job, true);
//          }
//        }
//      }
//    } catch (Exception e) {
//      logger.error("Scan error job failed {}", e.getMessage(), e);
//    }
//  }
//
//  @Scheduled(fixedDelay = 5000L)
//  public void jobProgressRateStats() {
//    Thread.currentThread().setName(String.format(ConnectorConstant.PROGRESS_STATS_JOB_THREAD, TRANSFORMER, instanceNo.substring(instanceNo.length() - 6)));
//    try {
//      long currentTimeMillis = System.currentTimeMillis();
//      JOB_MAP.forEach((jobId, transformer) -> {
//
//        Log4jUtil.setThreadContext(transformer.getContext().getJob());
//
//        long startTs = System.currentTimeMillis();
//        Job job = transformer.getContext().getJob();
//        MongoClient targetMongoClient = null;
//
//        if (StringUtils.equalsAnyIgnoreCase(transformer.getContext().getJobTargetConn().getDatabase_type(), DatabaseTypeEnum.MONGODB.getType(), DatabaseTypeEnum.ALIYUN_MONGODB.getType())) {
//          if (transformer.getContext().getTargetClientOperator() != null) {
//            targetMongoClient = transformer.getContext().getTargetClientOperator().getMongoClient();
//          }
//        }
//        long nextProgressStatTS = job.getNextProgressStatTS();
//        if (nextProgressStatTS <= currentTimeMillis && targetMongoClient != null) {
//
//          if (!job.is_test_write()) {
//            Log4jUtil.setThreadContext(job);
//
//            String sourceId = job.getConnections().getSource();
//            String targetId = job.getConnections().getTarget();
//            try {
//              Query query = new Query(Criteria.where("_id").is(sourceId));
//
//              List<Connections> sourceConns = MongodbUtil.getConnections(query, null, clientMongoOperator, true);
//              query = new Query(Criteria.where("_id").is(targetId));
//
//
//              List<Connections> targetConns = MongodbUtil.getConnections(query, null, clientMongoOperator, true);
//              if (CollectionUtils.isNotEmpty(sourceConns) && CollectionUtils.isNotEmpty(targetConns) && CollectionUtils.isNotEmpty(job.getMappings())) {
//                Connections sourceConn = sourceConns.get(0);
//                Connections targetConn = targetConns.get(0);
//
//                new OffsetConvertUtil(job, sourceConn).convert();
//                List<Target> targets = transformer.targets();
//                ProgressRateStats stats = jobProgressRateStats.progressRateStats(job, sourceConn, targetConn, targetMongoClient, targets);
//
//                if (stats != null) {
//                  job.setProgressRateStats(stats);
//                  Update update = new Update().set("row_count", stats.getRow_count()).set("ts", stats.getTs());
//
//                  if (update != null) {
//                    clientMongoOperator.update(new Query(where("_id").is(jobId)), update, ConnectorConstant.JOB_COLLECTION);
//                    // reset stats fail info.
//                    job.setProgressFailCount(0);
//                    job.setNextProgressStatTS(0L);
//                  }
//                } else {
//                  int progressFailCount = job.getProgressFailCount();
//                  progressFailCount++;
//                  job.setProgressFailCount(progressFailCount);
//                  job.setNextProgressStatTS(progressFailCount * STATS_FAIL_RETRY_INTERVAL * 1000 + System.currentTimeMillis());
//                }
//              }
//
//            } catch (IllegalStateException ise) {
//              // mongo client is close, then abort.
//            } catch (Exception e) {
//              logger.warn(TapLog.ERROR_0008.getMsg(), job.getName(), e.getMessage(), e);
//            } finally {
//              ThreadContext.clearAll();
//            }
//          }
//
//        }
//
//        long endTs = System.currentTimeMillis();
//
//        if (endTs - startTs >= 1000) {
//          logger.info("Stats job {} progress spent {}ms.", job.getName(), endTs - startTs);
//        }
//      });
//
//    } catch (Exception e) {
//      logger.warn(TapLog.ERROR_0005.getMsg(), e.getMessage(), e);
//    }
//  }
//
//  @Scheduled(fixedDelay = 2000L)
//  public void scanForceStopJob() {
//    Thread.currentThread().setName(String.format(ConnectorConstant.FORCE_STOP_JOB_THREAD, TRANSFORMER, instanceNo.substring(instanceNo.length() - 6)));
//    try {
//      List<Job> stoppedJobs = stoppingJob(true);
//      if (CollectionUtils.isNotEmpty(stoppedJobs)) {
//        stoppedJobs.forEach(job -> {
//          String jobId = job.getId();
//          if (JOB_MAP.containsKey(jobId)) {
//            try {
//              Log4jUtil.setThreadContext(job);
//              logger.info(TapLog.TRAN_LOG_0019.getMsg());
//
//              stopJob(job, true);
//
//            } catch (Exception e) {
//              logger.error(TapLog.TRAN_ERROR_0026.getMsg(), job.getName(), e.getMessage(), e);
//            } finally {
//              ThreadContext.clearAll();
//            }
//          } else {
//            stoppedJob(job);
//            logger.info(TapLog.JOB_LOG_0003.getMsg(), job.getName());
//          }
//        });
//      }
//    } catch (Exception e) {
//      logger.error("Scan force stopping jobs failed {}", e.getMessage(), e);
//    }
//  }
//
//	@Scheduled(fixedDelay = 5000L)
	public void perSecondflushJobStats() {
		Thread.currentThread().setName(String.format(ConnectorConstant.STATS_JOB_THREAD, TRANSFORMER, instanceNo.substring(instanceNo.length() - 6)));
		try {
//      JOB_MAP.forEach((jobId, tansformer) -> {
//        flushJobStats(tansformer);
//      });

			changeLogLevel();
		} catch (Exception e) {
			logger.error("Flush jobs' stats to db failed {}", e.getMessage(), e);
		}
	}
//
//
//  @Scheduled(fixedDelay = 60000L)
//  public void statsRunningJobDataSize() {
//    Thread.currentThread().setName(String.format(ConnectorConstant.STATS_DATA_SIZE_THREAD, TRANSFORMER, instanceNo.substring(instanceNo.length() - 6)));
//    try {
//      for (Map.Entry<String, Transformer> entry : JOB_MAP.entrySet()) {
//        Job job = entry.getValue().getContext().getJob();
//        String target = job.getConnections().getTarget();
//        Log4jUtil.setThreadContext(job);
//
//        MongoClient mongoClient = null;
//        try {
//          Query query = new Query(where("_id").is(target));
//          query.fields().exclude("schema");
//          List<Connections> targetConns = MongodbUtil.getConnections(query, null, clientMongoOperator, true);
//          if (CollectionUtils.isNotEmpty(targetConns) && StringUtils.equalsAnyIgnoreCase(targetConns.get(0).getDatabase_type(), DatabaseTypeEnum.MONGODB.getType(), DatabaseTypeEnum.ALIYUN_MONGODB.getType())) {
//
//            mongoClient = MongodbUtil.createMongoClient(targetConns.get(0));
//            String database = MongodbUtil.getDatabase(targetConns.get(0));
//            List<Mapping> mappings = job.getMappings();
//
//            Set<String> targetCollectionName = new HashSet<>();
//            if (CollectionUtils.isNotEmpty(mappings)) {
//              for (Mapping mapping : mappings) {
//                targetCollectionName.add(mapping.getTo_table());
//              }
//
//              long totalDataSize = 0L;
//
//              if (job.getStats() != null) {
//
//                MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
//                MongoIterable<String> collectionNames = mongoDatabase.listCollectionNames();
//                for (String collectionName : collectionNames) {
//                  if (targetCollectionName.contains(collectionName)) {
//                    Document document = mongoDatabase.runCommand(new Document("collStats", collectionName));
//                    Object size = document.get("size");
//                    if (size instanceof Integer) {
//                      totalDataSize += new BigDecimal((Integer) size).longValue();
//                    } else if (size instanceof Double) {
//                      totalDataSize += new BigDecimal((Double) size).longValue();
//                    }
//                  }
//                }
//
//                job.getStats().getTotal().put(Stats.TOTAL_FILE_LENGTH_FIELD_NAME, totalDataSize);
//              }
//            }
//          }
//        } catch (Exception e) {
//          logger.warn("Stats running job target mongodb data size failed {}, will retry after 60s, stack {}", e.getMessage(), Log4jUtil.getStackString(e));
//        } finally {
//          MongodbUtil.releaseConnection(mongoClient, null);
//          ThreadContext.clearAll();
//        }
//      }
//    } catch (Exception e) {
//      logger.warn("Stats running job data size failed {}, will retry after 60s, stack {}", e.getMessage(), Log4jUtil.getStackString(e));
//    }
//
//  }
//
//  private void flushJobStats(Job job, Transformer transformer) {
//    try {
//      Log4jUtil.setThreadContext(job);
//      if (JOB_MAP.containsKey(job.getId())) {
//        job = JOB_MAP.get(job.getId()).getContext().getJob();
//      }
//      Stats stats = job.getStats();
//      Map<String, LinkedList<Long>> perSecond = stats.getPer_second();
//
//      Map<String, Long> total = null;
//      long currentTimeMillis = 0;
//      Map<String, Object> params;
//
//      params = new HashMap<>(1);
//      params.put("_id", job.getId());
//      params.put("process_id", instanceNo);
//
//      Map<String, Object> update = new HashMap<>();
//      if (transformer != null) {
//        total = transformer.getStats();
//        Map<String, Long> previousTotal = JOB_STATS.get(job.getId());
//        if (MapUtils.isEmpty(previousTotal)) {
//          previousTotal = total;
//        }
//        Long inserted = total.get("target_inserted");
//        Long updated = total.get("target_updated");
//        Long processed = total.get("processed");
//        Long totalUpdated = total.get("total_updated");
//        Long totalDeleted = total.get("total_deleted");
//        Long totalDataSize = total.getOrDefault(Stats.TOTAL_FILE_LENGTH_FIELD_NAME, 0L);
//        Long totalDataQuality = total.getOrDefault(Stats.TOTAL_DATA_QUAILTY_FIELD_NAME, 0L);
//
//        Long previousInsert = previousTotal.getOrDefault("target_inserted", new Long(0));
//        Long previousUpdate = previousTotal.getOrDefault("target_updated", new Long(0));
//        Long previousProcess = previousTotal.getOrDefault("processed", new Long(0));
//
//        LinkedList<Long> targetInserted = perSecond.getOrDefault("target_inserted", new LinkedList<>());
//        LinkedList<Long> targetUpdated = perSecond.getOrDefault("target_updated", new LinkedList<>());
//        LinkedList<Long> perProcessed = perSecond.getOrDefault("processed", new LinkedList<>());
//        if (targetInserted.size() >= 20) {
//          targetInserted.removeFirst();
//        }
//        if (targetUpdated.size() >= 20) {
//          targetUpdated.removeFirst();
//        }
//        if (perProcessed.size() >= 20) {
//          perProcessed.removeFirst();
//        }
//
//        currentTimeMillis = System.currentTimeMillis();
//        long lastStatsTimestamp = job.getLastStatsTimestamp();
//        long intervalSecs = (currentTimeMillis - lastStatsTimestamp) / 1000;
//        intervalSecs = intervalSecs <= 0 ? 1 : intervalSecs;
//        targetInserted.addLast((inserted - previousInsert) / intervalSecs);
//        targetUpdated.addLast((updated - previousUpdate) / intervalSecs);
//        perProcessed.addLast((processed - previousProcess) / intervalSecs);
//
//        update.put("stats.per_second.target_inserted", targetInserted);
//        update.put("stats.per_second.target_updated", targetUpdated);
//        update.put("stats.per_second.processed", perProcessed);
//
//        List<StageRuntimeStats> stageRuntimeStats = JOB_STAGE_STATS.get(job.getId());
//        if (stageRuntimeStats != null) {
//          update.put("stats.stageRuntimeStats", stageRuntimeStats);
//        }
//        Job cacheJob = messageDao.getCacheJob(job);
//        if (cacheJob != null && stageRuntimeStats != null) {
//          List<StageRuntimeStats> cacheStageRuntimeStats = cacheJob.getStats().getStageRuntimeStats();
//          for (StageRuntimeStats cacheStageRuntimeStat : cacheStageRuntimeStats) {
//            for (StageRuntimeStats stageRuntimeStat : stageRuntimeStats) {
//              if (stageRuntimeStat.getStageId().equals(cacheStageRuntimeStat.getStageId())) {
//                stageRuntimeStat.mergeStats(cacheStageRuntimeStat);
//                break;
//              }
//            }
//          }
//        }
//
//        update.put("stats.total.target_inserted", inserted);
//        update.put("stats.total.target_updated", updated);
//        update.put("stats.total.processed", processed);
//        update.put("stats.total.total_updated", totalUpdated);
//        update.put("stats.total.total_deleted", totalDeleted);
//        update.put("stats.total." + Stats.TOTAL_FILE_LENGTH_FIELD_NAME, totalDataSize);
//        update.put("stats.total." + Stats.TOTAL_DATA_QUAILTY_FIELD_NAME, totalDataQuality);
//
//        Object processOffset = transformer.getProcessOffset();
//        if (processOffset != null && processOffset instanceof TapdataOffset && ((TapdataOffset) processOffset).getOffset() != null) {
//          update.put("offset", processOffset);
//        }
//      }
//
//      update.put("transformerErrorEvents", job.getTransformerErrorEvents() == null ? new ArrayList<>() : job.getTransformerErrorEvents());
//      update.put("transformerLastSyncStage", job.getTransformerLastSyncStage());
//      TapdataShareContext cacheTapdataShareContext = messageDao.getCacheTapdataShareContext(job);
//      if (cacheTapdataShareContext != null && CollectionUtils.isNotEmpty(cacheTapdataShareContext.getInitialStats())) {
//        cacheTapdataShareContext.getInitialStats().forEach(initialStat -> {
//            if (initialStat == null) {
//              return;
//            }
//            if (initialStat.getSourceRowNum().compareTo(0L) > 0 && initialStat.getSourceRowNum().compareTo(initialStat.getTargetRowNum()) < 0) {
//              initialStat.setTargetRowNum(initialStat.getSourceRowNum());
//            }
//          });
//          update.put("stats.initialStats", cacheTapdataShareContext.getInitialStats());
//      }
//
//      if (!job.isEditDebug()) {
//        clientMongoOperator.updateAndParam(params, update, ConnectorConstant.JOB_COLLECTION);
//      }
//      job.setLastStatsTimestamp(currentTimeMillis);
//      if (transformer != null) {
//        JOB_STATS.put(job.getId(), new HashMap<>(total));
//      }
//    } catch (Exception e) {
//      logger.warn(TapLog.W_JOG_LOG_0002.getMsg(), e.getMessage(), Log4jUtil.getStackString(e));
//    } finally {
//      ThreadContext.clearAll();
//    }
//  }
//
//  private void flushJobStats(Transformer transformer) {
//    if (transformer == null) {
//      return;
//    }
//    TransformerContext context = transformer.getContext();
//    Job job = context.getJob();
//    flushJobStats(job, transformer);
//  }
//
//  @Scheduled(fixedDelay = 2000L)
//  public void stopJobIfNeed() {
//    Thread.currentThread().setName(String.format(ConnectorConstant.STOP_JOB_THREAD, TRANSFORMER, instanceNo.substring(instanceNo.length() - 6)));
//    try {
//      LinkedBlockingQueue<String> stopJobs = messageDao.getStopJobs();
//      while (stopJobs.size() > 0) {
//        String jobId = stopJobs.poll();
//        if (StringUtils.isNotBlank(jobId) && JOB_MAP.containsKey(jobId)) {
//          Transformer transformer = JOB_MAP.get(jobId);
//          Job job = transformer.getContext().getJob();
//          try {
//            transformer.forceStop();
//            removeJobMapIfNeed(jobId);
//          } catch (Exception e) {
//            logger.error(TapLog.JOB_ERROR_0010.getMsg(), job.getId(), job.getName(), e.getMessage(), e);
//          }
//        }
//      }
//    } catch (Exception e) {
//      logger.error("Stop jobs failed {}", e.getMessage(), e);
//    }
//  }

//

	/**
	 * worker heart beat
	 */
	@Scheduled(fixedDelay = 5000L)
	public void workerHeartBeat() {
		Thread.currentThread().setName(String.format(ConnectorConstant.WORKER_HEART_BEAT_THREAD, TRANSFORMER, instanceNo.substring(instanceNo.length() - 6)));
		try {
			String hostname = SystemUtil.getHostName();
			Double processCpuLoad = SystemUtil.getProcessCpuLoad();
			long usedMemory = SystemUtil.getUsedMemory();
			String userId = (String) configCenter.getConfig(ConfigurationCenter.USER_ID);
			Integer threshold = 1;
			Setting thresholdSetting = settingService.getSetting("threshold");
			if (thresholdSetting != null) {
				threshold = Integer.valueOf(thresholdSetting.getDefault_value());
				if (NumberUtils.isDigits(thresholdSetting.getValue())) {
					threshold = Integer.valueOf(thresholdSetting.getValue());
				}
			}
			Map<String, Object> params = new HashMap<>();
			params.put("process_id", instanceNo);
			params.put("worker_type", ConnectorConstant.WORKER_TYPE_TRANSFORMER);

			Map<String, Object> value = new HashMap<>();
			value.put("total_thread", threshold);
			value.put("process_id", instanceNo);
			value.put("user_id", userId);
			value.put("version", version);
			value.put("hostname", hostname);
			value.put("cpuLoad", processCpuLoad);
			value.put("usedMemory", usedMemory);
			value.put("worker_type", ConnectorConstant.WORKER_TYPE_TRANSFORMER);

			if (StringUtils.isNoneBlank(region, zone)) {
				if (StringUtils.isNoneBlank(region, zone)) {
					Map<String, String> platformInfo = new HashMap<>();
					platformInfo.put("region", region);
					platformInfo.put("zone", zone);
					value.put("platformInfo", platformInfo);
				}
			}

			ConnectorManager.sendWorkerHeartbeat(
					value,
					v -> pingClientMongoOperator.insertOne(v, ConnectorConstant.WORKER_COLLECTION + "/health"));
		} catch (Exception e) {
			logger.error("Transformer heartbeat failed {}", e.getMessage(), e);
		}
	}

//  private void jobError(String jobId) {
//    Optional.ofNullable(JOB_MAP).ifPresent(jobMap -> {
//      if (jobMap.containsKey(jobId)) {
//        Job job = (jobMap.get(jobId)).getContext().getJob();
//        if (job != null) {
//          job.jobError();
//        }
//      }
//    });
//  }

	//	@Scheduled(fixedDelay = 30000L)
//  @Deprecated
//  public void eventExecutor() throws InterruptedException {
//    Thread.currentThread().setName(String.format(ConnectorConstant.EVENT_THREAD, TRANSFORMER, instanceNo.substring(instanceNo.length() - 6)));
//    String workerTimeout = settingService.getString("lastHeartbeat", "60");
//
//    if (isAdminRole() && AgentUtil.isFirstWorker(clientMongoOperator, instanceNo, null, Double.valueOf(workerTimeout))) {
//      int totalRetry = 3;
//      long intervalRetry = 60000;
//
//      Query query = new Query(new Criteria().orOperator(
//        where("event_status").is(Event.EVENT_STATUS_WAITING),
//        new Criteria().andOperator(
//          where("event_status").is(Event.EVENT_STATUS_FAILED)
//            .and("failed_result.retry").lte(totalRetry).and("failed_result.next_retry").lte(Double.valueOf(System.currentTimeMillis()))
//        )
//      ));
//      List<Event> events = clientMongoOperator.find(query, ConnectorConstant.EVENT_COLLECTION, Event.class);
//      if (CollectionUtils.isNotEmpty(events)) {
//        for (Event event : events) {
//          try {
//            if (StringUtils.isNotBlank(event.getJob_id())) {
//              ThreadContext.put("jobId", event.getJob_id());
//              ThreadContext.put("threadName", Thread.currentThread().getName());
//            }
//            String name = event.getName();
//            EventExecutor eventExecutor = eventExecutors.get(name);
//            if (eventExecutor != null) {
//              event = eventExecutor.execute(event);
//              if (Event.EVENT_STATUS_FAILED.equals(event.getEvent_status())) {
//                Map<String, Object> failedResult = event.getFailed_result();
//                int retry = (int) failedResult.getOrDefault("retry", 0);
//                retry++;
//                long nextRetry = retry * intervalRetry + System.currentTimeMillis();
//                failedResult.put("retry", retry);
//                failedResult.put("next_retry", nextRetry);
//
//                clientMongoOperator.update(new Query(where("_id").is(event.getId())),
//                  new Update().set("failed_result", failedResult).set("event_status", Event.EVENT_STATUS_FAILED), ConnectorConstant.EVENT_COLLECTION
//                );
//              } else if (Event.EVENT_STATUS_SUCCESSED.equals(event.getEvent_status())) {
//                clientMongoOperator.update(new Query(where("_id").is(event.getId())),
//                  new Update().set("event_status", Event.EVENT_STATUS_SUCCESSED), ConnectorConstant.EVENT_COLLECTION
//                );
//              }
//
//              Thread.sleep(10000);
//            } else {
//              logger.warn("Abort this event {}, can not find executor.");
//            }
//          } finally {
//            ThreadContext.clearAll();
//          }
//        }
//      }
//    }
//  }

//  @Scheduled(fixedDelay = 2000L)
//  public void statsStageRumtimeStats() {
//    Thread.currentThread().setName(String.format(ConnectorConstant.STATS_STAGES_THREAD, TRANSFORMER, instanceNo.substring(instanceNo.length() - 6)));
//    if (MapUtils.isEmpty(JOB_STAGE_STATS)) {
//      return;
//    }
//    try {
//      for (Map.Entry<String, Transformer> entry : JOB_MAP.entrySet()) {
//        Job job = entry.getValue().getContext().getJob();
//        String jobId = job.getId();
//
//        if (!JOB_STAGE_STATS.containsKey(jobId)) {
//          continue;
//        }
//
//        Job cacheJob = messageDao.getCacheJob(job);
//        if (cacheJob != null && cacheJob.getStats() != null) {
//          List<StageRuntimeStats> cacheJobStats = cacheJob.getStats().getStageRuntimeStats();
//          job.getStats().mergeStageStats(cacheJobStats);
//
//          List<StageRuntimeStats> stageRuntimeStats = job.getStats().getStageRuntimeStats();
//          List<StageRuntimeStats> cloneStats = new ArrayList<>();
//          ListUtil.cloneableCloneList(stageRuntimeStats, cloneStats);
//
//          List<StageRuntimeStats> lastStageStats = JOB_STAGE_STATS.get(jobId);
//          Map<String, StageRuntimeStats> lastStageStatsMap = new HashMap<>();
//          for (StageRuntimeStats lastStageStat : lastStageStats) {
//            lastStageStatsMap.put(lastStageStat.getStageId(), lastStageStat);
//          }
//
//          for (StageRuntimeStats stageRuntimeStat : stageRuntimeStats) {
//            String stageId = stageRuntimeStat.getStageId();
//            StageRuntimeStats lastStageStat = lastStageStatsMap.get(stageId);
//            stageRuntimeStat.speedCalculate(lastStageStat);
//          }
//
//          JOB_STAGE_STATS.put(jobId, cloneStats);
//        }
//      }
//    } catch (Exception e) {
//      logger.error("Stats job stages' runtime stats failed {}", e.getMessage(), e);
//    }
//  }

	private void changeLogLevel() {

		Setting levelSetting = settingService.getSetting("logLevel");
		String scriptEngineHttpApender = settingService.getString("scriptEngineHttpAppender", "false");
		if (levelSetting != null) {
			Level level = logLevel(levelSetting.getValue());

			String debug = System.getenv("DEBUG");
			if ("true".equalsIgnoreCase(debug)) {
				level = Level.DEBUG;
			}

			if (level != null) {
				LoggerContext context = LoggerContext.getContext(false);
				Collection<org.apache.logging.log4j.core.Logger> loggers = context.getLoggers();

				for (org.apache.logging.log4j.core.Logger logger1 : loggers) {
					final String loggerName = logger1.getName();
					if (
							StringUtils.startsWithIgnoreCase(loggerName, "io.tapdata") ||
									StringUtils.startsWithIgnoreCase(loggerName, "com.tapdata")
					) {
						logger1.setLevel(level);
						if (StringUtils.contains(loggerName, "CustomProcessor")) {
							final Map<String, Appender> appenders = logger1.get().getAppenders();
							if ("false".equals(scriptEngineHttpApender)) {
								if (appenders.containsKey("httpAppender")) {
									logger1.setAdditive(false);
									final Map<String, Appender> rootAppenders = context.getRootLogger().getAppenders();
									for (Appender appender : rootAppenders.values()) {
										logger1.addAppender(appender);
									}
									logger1.get().removeAppender("httpAppender");
								}
							} else if (!appenders.containsKey("httpAppender")) {
								logger1.setAdditive(true);
//								if (!appenders.containsKey("httpAppender")) {
//									final CustomHttpAppender httpAppender = context.getConfiguration().getAppender("httpAppender");
//									logger1.addAppender(httpAppender);
//								}
							}
						}
					}/* else {
						logger1.setLevel(level);
					}*/
				}
			}
		}

		udpateLogFilter();
	}

	private void udpateLogFilter() {
		String newLog4jFilterInterval = settingService.getString("log4jFilterInterval", "20");
		String newLog4jFilterRate = settingService.getString("log4jFilterRate", "16");

		LogUtil logUtil = new LogUtil(settingService);
		org.apache.logging.log4j.core.Logger rootLogger = (org.apache.logging.log4j.core.Logger) LogManager.getRootLogger();
		CustomHttpAppender httpAppender = rootLogger.getContext().getConfiguration().getAppender("httpAppender");
		CompositeFilter compositeFilter = (CompositeFilter) httpAppender.getFilter();
		Filter[] filtersArray = compositeFilter.getFiltersArray();
		for (Filter filter : filtersArray) {
			if (filter instanceof TapdataLog4jFilter
					&& StringUtils.isNotBlank(originLog4jFilterInterval)
					&& !originLog4jFilterInterval.equals(newLog4jFilterInterval)) {

				httpAppender.removeFilter(filter);
				httpAppender.addFilter(logUtil.buildFilter());
			}

			if (filter instanceof BurstFilter
					&& StringUtils.isNotBlank(originLog4jFilterRate)
					&& !originLog4jFilterRate.equals(newLog4jFilterRate)) {

				httpAppender.removeFilter(filter);
				httpAppender.addFilter(logUtil.buildBurstFilter());
			}
		}
		originLog4jFilterInterval = newLog4jFilterInterval;
		originLog4jFilterRate = newLog4jFilterRate;
	}

	private Level logLevel(String levelName) {

		String debug = System.getenv("DEBUG");
		if ("true".equalsIgnoreCase(debug)) {
			levelName = "debug";
		}

		if (StringUtils.isBlank(levelName)) {
			return Level.INFO;
		}
		switch (levelName.toLowerCase()) {
			case "info":
				return Level.INFO;
			case "debug":
				return Level.DEBUG;
			case "trace":
				return Level.TRACE;
			case "warn":
				return Level.WARN;
			case "error":
				return Level.ERROR;
			default:
				return Level.INFO;
		}

	}

	public Job runningJob(String jobId) {

		if (StringUtils.isNotBlank(jobId)) {
			Query query = new Query(where("_id").is(jobId));
			query.fields().exclude("editorData");
			List<Job> jobs = clientMongoOperator.find(query, ConnectorConstant.JOB_COLLECTION, Job.class);
			if (CollectionUtils.isNotEmpty(jobs)) {
				return jobs.get(0);
			}
		}

		return null;
	}

	//
//  private void interruptJob(Job job) {
//    String jobId = job.getId();
////		Map<String, Object> params = new HashMap<>();
////		params.put("_id", jobId);
//
//    Query query = new Query(
//      where("_id").is(jobId)
//    );
//    query.fields().include("_id");
//
////		Map<String, Object> update = new HashMap<>();
////		update.put(ConnectorConstant.JOB_STATUS_FIELD, ConnectorConstant.ERROR);
//    Update update = new Update().set(ConnectorConstant.JOB_STATUS_FIELD, ConnectorConstant.ERROR);
//
//    clientMongoOperator.findAndModify(query, update, Job.class, ConnectorConstant.JOB_COLLECTION);
//
//  }
//
//  private List<Job> stoppingJob(boolean isForceStop) {
//
//    Criteria transformerStopped;
//    Criteria stoppingWhere;
//    if (isForceStop) {
//      stoppingWhere = where(ConnectorConstant.JOB_STATUS_FIELD).is(ConnectorConstant.FORCE_STOPPING);
//      transformerStopped = new Criteria().orOperator(
//        where(ConnectorConstant.JOB_TRANSFORMER_STOPPED_FIELD).is(false),
//        where(ConnectorConstant.JOB_TRANSFORMER_STOPPED_FIELD).is(true).and(ConnectorConstant.JOB_CONNECTOR_STOPPED_FIELD).is(true)
//      );
//    } else {
//      stoppingWhere = new Criteria().orOperator(where(ConnectorConstant.JOB_STATUS_FIELD).is(ConnectorConstant.STOPPING),
//        where(ConnectorConstant.JOB_STATUS_FIELD).is(ConnectorConstant.ERROR));
//      transformerStopped = where(ConnectorConstant.JOB_TRANSFORMER_STOPPED_FIELD).is(false);
//    }
//
//    Query query = new Query(new Criteria().andOperator(currentUserCriteria(), stoppingWhere, transformerStopped));
//    List<Job> jobs;
//    try {
//      jobs = clientMongoOperator.find(query, ConnectorConstant.JOB_COLLECTION, Job.class);
//    } catch (Exception e) {
//      jobs = new ArrayList<>();
//    }
//
//    if (!isForceStop) {
//      for (Map.Entry<String, Transformer> entry : JOB_MAP.entrySet()) {
//        String jobId = entry.getKey();
////				Map<String, Object> params = new HashMap<>();
//        Query existsQuery = new Query(where("_id").is(jobId));
//        existsQuery.fields().include("status");
////				params.clear();
////				params.put("_id", jobId);
//
//        if (stopJobMap.containsKey(jobId)) {
//          continue;
//        }
//
//        try {
//          List<Job> list = clientMongoOperator.find(existsQuery, ConnectorConstant.JOB_COLLECTION, Job.class);
//
//          // 库中不存在停止job，请求TM失败保持任务
//          if (CollectionUtils.isEmpty(list) || ConnectorConstant.PAUSED.equals(list.get(0).getStatus())) {
//            Transformer rdmTransformer = entry.getValue();
//            Job job = rdmTransformer.getContext().getJob();
//            jobs.add(job);
//          }
//        } catch (Exception e) {
//          logger.error("Check job {} exists in db failed {}", e.getMessage());
//        }
//
//        //jobs是不完全的，我需要把JOB_MAP（内存中）Stopping的，但中间库不是Stopping的记录添加进来；同时修改中间库任务状态
//        Job job = entry.getValue().getContext().getJob();
//        String jobStatus = job.getStatus();
//        if (ConnectorConstant.STOPPING.equals(jobStatus)) {
//          jobs.add(job);
////					Map<String, Object> update = new HashMap<>();
////					update.put(ConnectorConstant.JOB_STATUS_FIELD, ConnectorConstant.STOPPING);
//          Update update = new Update().set(ConnectorConstant.JOB_STATUS_FIELD, ConnectorConstant.STOPPING);
//          clientMongoOperator.update(existsQuery, update, ConnectorConstant.JOB_COLLECTION);
//        }
//      }
//    }
//
//    if (CollectionUtils.isNotEmpty(jobs)) {
//      Iterator<Job> iterator = jobs.iterator();
//      while (iterator.hasNext()) {
//        Job job = iterator.next();
//        if (stopJobMap.containsKey(job.getId()) && isForceStop == stopJobMap.get(job.getId()).isForce()) {
//          iterator.remove();
//        }
//      }
//    }
//
//    return jobs;
//  }
//
//  private void stoppedJob(Job job) {
//    if (job != null) {
//      Map<String, Object> params = new HashMap<>();
//      String jobId = job.getId();
//      params.put("_id", jobId);
//      Map<String, Object> update = new HashMap<>();
//
//      if (job.getStatus().equals(ConnectorConstant.ERROR)) {
//        update.put(ConnectorConstant.JOB_STATUS_FIELD, ConnectorConstant.ERROR);
//      }
//      update.put(ConnectorConstant.JOB_TRANSFORMER_STOPPED_FIELD, true);
//      update.put(ConnectorConstant.JOB_PING_TIME_FIELD, null);
//      synchronized (messageDao) {
//        UpdateResult updateResult = clientMongoOperator.updateAndParam(params, update, ConnectorConstant.JOB_COLLECTION);
//        if (updateResult.getModifiedCount() > 0) {
//          Query query = new Query(where("_id").is(jobId).and(ConnectorConstant.JOB_TRANSFORMER_STOPPED_FIELD).is(true));
//          List<Job> jobs = clientMongoOperator.find(query, ConnectorConstant.JOB_COLLECTION, Job.class);
//          if (CollectionUtils.isNotEmpty(jobs)) {
//            job = jobs.get(0);
//          }
//
//          logger.info(
//            "Stop transformer success, current job {} status {}, transformerStopped {}, connectorStopped {}",
//            job.getName(),
//            job.getStatus(),
//            job.getTransformerStopped(),
//            job.getConnectorStopped()
//          );
//        }
//      }
//
//      if (job != null) {
//        if (job.getConnectorStopped() != null && job.getConnectorStopped() && !job.getStatus().equals(ConnectorConstant.ERROR)) {
//          update.clear();
//          update.put(ConnectorConstant.JOB_STATUS_FIELD, ConnectorConstant.PAUSED);
//
//          UpdateResult updateResult = clientMongoOperator.updateAndParam(params, update, ConnectorConstant.JOB_COLLECTION);
//          if (updateResult.getModifiedCount() > 0) {
//            logger.info(
//              "Stop job {} success, transformerStopped {}, connectorStopped {}",
//              job.getName(),
//              job.getTransformerStopped(),
//              job.getConnectorStopped()
//            );
//          } else {
//            logger.info(
//              "Waiting job {}'s connector stop, transformerStopped {}, connectorStopped {}",
//              job.getName(),
//              job.getTransformerStopped(),
//              job.getConnectorStopped()
//            );
//          }
//        }
//      }
//    }
//  }
//
//  /**
//   * only build profile is cloud return current user criteria
//   *
//   * @return
//   */
//  private Criteria currentUserCriteria() {
//    Criteria criteria = new Criteria();
//    String userId = (String) configCenter.getConfig(ConfigurationCenter.USER_ID);
//    Setting buildProfile = settingService.getSetting("buildProfile");
//    if (buildProfile != null) {
//      String value = buildProfile.getValue();
//      if (StringUtils.isBlank(value) || value.equals("CLOUD")) {
//        criteria = where("user_id").is(userId);
//      }
//    }
//    return criteria;
//  }
//
	private boolean isAdminRole() {
		User user = (User) configCenter.getConfig(ConfigurationCenter.USER_INFO);
		return User.ADMIN_ROLE == user.getRole();
	}
//
//  private void startedJob(Job job, boolean jobTransformerStoppedField) {
//    if (job != null) {
//      String jobId = job.getId();
//      Map<String, Object> params = new HashMap<>();
//      Map<String, Object> update = new HashMap<>();
//      if (StringUtils.isNotBlank(jobId)) {
//        params.put("_id", jobId);
//        update.put(ConnectorConstant.JOB_TRANSFORMER_STOPPED_FIELD, jobTransformerStoppedField);
//
//        clientMongoOperator.findAndModify(params, update, Job.class, ConnectorConstant.JOB_COLLECTION);
//        job.setLastStatsTimestamp(System.currentTimeMillis());
//      }
//    }
//  }
//
//  private void removeJobMapIfNeed(String jobId) {
//    if (StringUtils.isNotBlank(jobId)) {
//      Optional.ofNullable(JOB_MAP).ifPresent(jobMap -> jobMap.remove(jobId));
//      Optional.ofNullable(JOB_STATS).ifPresent(jobStats -> jobStats.remove(jobId));
//      Optional.ofNullable(JOB_STAGE_STATS).ifPresent(jobStats -> jobStats.remove(jobId));
//      Optional.ofNullable(RUNNING_JOB_THREAD_FUTURE_MAP).ifPresent(futures -> {
//        if (futures.containsKey(jobId)) {
//          futures.get(jobId).cancel(false);
//        }
//        futures.remove(jobId);
//      });
//      Optional.ofNullable(RUNNING_JOB_THREAD_POOL_MAP).ifPresent(executors -> {
//        if (executors.containsKey(jobId)) {
//          ExecutorService service = executors.get(jobId);
//          new ExecutorUtil().shutdown(service, () -> {
//            service.shutdownNow();
//            return true;
//          }, 60L, TimeUnit.MILLISECONDS);
//        }
//        executors.remove(jobId);
//      });
//    }
//  }
//
//  private void jobReset(Job job, Connections sourceConn, Connections targetConn, ClientMongoOperator clientMongoOperator) throws SQLException {
//    if (job == null || !job.isReset()) {
//      return;
//    }
//
//    // clear offset data
//    if (targetConn != null) {
//      DatabaseTypeEnum databaseTypeEnum = DatabaseTypeEnum.fromString(targetConn.getDatabase_type());
//      switch (databaseTypeEnum) {
//        case ORACLE:
//          try (
//            Connection connection = OracleUtil.createConnection(targetConn);
//            PreparedStatement deleteOffsetPstmt = connection.prepareStatement(String.format(JdbcConstant.ORACLE_DELETE_OFFSET_BY_JOBID,
//              targetConn.getDatabase_owner(),
//              JdbcConstant.ORACLE_OFFSET_TABLE_NAME))
//          ) {
//            deleteOffsetPstmt.setString(1, job.getId());
//            int deleteCount = deleteOffsetPstmt.executeUpdate();
//            connection.commit();
//
//            Map<String, Object> params = new HashMap<>();
//            params.put("_id", job.getId());
//            Map<String, Object> update = new HashMap<>();
//            update.put("reset", false);
//            clientMongoOperator.updateAndParam(params, update, ConnectorConstant.JOB_COLLECTION);
//
//            logger.info("Job reset, delete offset from oracle database, job id: {}, delete count: {}", job.getId(), deleteCount);
//          }
//
//          break;
//      }
//    }
//  }
//
//  private void stopJob(Job job, boolean force) {
//    if (job == null || StringUtils.isAnyBlank(job.getId())) {
//      return;
//    }
//
//    if (stopJobMap.containsKey(job.getId()) && force == stopJobMap.get(job.getId()).isForce()) {
//      return;
//    }
//
//    String threadName = String.format(stopJobThreadName, job.getName(), job.getId());
//    TransformerStopJob transformerStopJob = new TransformerStopJob(job, force, threadName);
//    Future future = stopJobThreadPool.submit(transformerStopJob);
//    transformerStopJob.setFuture(future);
//    stopJobMap.put(job.getId(), transformerStopJob);
//  }
//
//  private class TransformerStopJob implements Runnable {
//
//    private Job job;
//    private String threadName;
//    private boolean force;
//    private Future future;
//
//    public TransformerStopJob(Job job, boolean force, String threadName) {
//      this.job = job;
//      this.threadName = threadName;
//      this.force = force;
//    }
//
//    @Override
//    public void run() {
//      try {
//        Thread.currentThread().setName(threadName);
//        Log4jUtil.setThreadContext(job);
//        logger.info(TapLog.JOB_LOG_0006.getMsg(), job.getName());
//        Optional.ofNullable(JOB_MAP.get(job.getId())).ifPresent(transformer -> {
//          if (force) {
//            transformer.forceStop();
//          } else {
//            transformer.getContext().getJob().setStatus(ConnectorConstant.STOPPING);
//            transformer.stop();
//
//            logger.info(TapLog.JOB_LOG_0002.getMsg());
//          }
//        });
//
//
//      } finally {
//        ThreadContext.clearAll();
//      }
//    }
//
//    public Job getJob() {
//      return job;
//    }
//
//    public String getThreadName() {
//      return threadName;
//    }
//
//    public boolean isForce() {
//      return force;
//    }
//
//    public Future getFuture() {
//      return future;
//    }
//
//    public void setFuture(Future future) {
//      this.future = future;
//    }
//  }
}
