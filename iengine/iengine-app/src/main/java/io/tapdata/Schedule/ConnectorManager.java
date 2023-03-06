package io.tapdata.Schedule;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.constant.AgentUtil;
import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.DateUtil;
import com.tapdata.constant.JSONUtil;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.constant.MapUtil;
import com.tapdata.constant.MongodbUtil;
import com.tapdata.constant.SSLUtil;
import com.tapdata.constant.SystemUtil;
import com.tapdata.constant.UUIDGenerator;
import com.tapdata.constant.VersionCheck;
import com.tapdata.entity.AppType;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.Job;
import com.tapdata.entity.JobConnection;
import com.tapdata.entity.LoginResp;
import com.tapdata.entity.ProgressRateStatsMap;
import com.tapdata.entity.RelateDataBaseTable;
import com.tapdata.entity.Schema;
import com.tapdata.entity.Setting;
import com.tapdata.entity.Stats;
import com.tapdata.entity.TapLog;
import com.tapdata.entity.User;
import com.tapdata.entity.Worker;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.mongo.RestTemplateOperator;
import com.tapdata.tm.commons.ping.PingDto;
import com.tapdata.tm.commons.ping.PingType;
import com.tapdata.tm.sdk.util.CloudSignUtil;
import com.tapdata.tm.worker.WorkerSingletonException;
import com.tapdata.tm.worker.WorkerSingletonLock;
import com.tapdata.validator.ConnectionValidateResult;
import com.tapdata.validator.ConnectionValidator;
import com.tapdata.validator.ValidatorConstant;
import io.tapdata.Runnable.LoadSchemaRunner;
import io.tapdata.TapInterface;
import io.tapdata.aspect.LoginSuccessfullyAspect;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.common.ClassScanner;
import io.tapdata.common.Connector;
import io.tapdata.common.ConverterUtil;
import io.tapdata.common.JetExceptionFilter;
import io.tapdata.common.LoadBalancing;
import io.tapdata.common.LogUtil;
import io.tapdata.common.SettingService;
import io.tapdata.common.SupportUtil;
import io.tapdata.common.TapInterfaceUtil;
import io.tapdata.common.TapdataLog4jFilter;
import io.tapdata.common.WarningMaker;
import io.tapdata.dao.MessageDao;
import io.tapdata.entity.BaseConnectionValidateResult;
import io.tapdata.entity.BaseConnectionValidateResultDetail;
import io.tapdata.entity.ConnectionsType;
import io.tapdata.entity.Converter;
import io.tapdata.entity.Lib;
import io.tapdata.entity.LibSupported;
import io.tapdata.flow.engine.V2.entity.GlobalConstant;
import io.tapdata.metric.MetricManager;
import io.tapdata.schema.SchemaProxy;
import io.tapdata.task.TapdataTaskScheduler;
import io.tapdata.websocket.ManagementWebsocketHandler;
import io.tapdata.websocket.WebSocketEvent;
import io.tapdata.websocket.handler.PongHandler;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.filter.BurstFilter;
import org.apache.logging.log4j.core.layout.JsonLayout;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Primary;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.springframework.data.mongodb.core.query.Criteria.where;


/**
 * @author jackin
 * @date 2017/5/16
 */
@Component("connectorManager")
public class ConnectorManager {

	private final static long LONG_TIME_EXECUTED_CAPACITY = 1000L;

	private static final Logger logger = LogManager.getLogger(ConnectorManager.class);

	private static final ConcurrentHashMap<String, Job> JOB_MAP = new ConcurrentHashMap<>();

	private static final Set<String> JOB_SET = new HashSet<>(); // 进入调度的Job

	private static final ConcurrentHashMap<String, Job> ERR_JOB_MAP = new ConcurrentHashMap<>();

	private static final ConcurrentHashMap<String, Connector> JOB_THREADS = new ConcurrentHashMap<>();

	private static final ConcurrentHashMap<String, Map<String, Long>> JOB_STATS = new ConcurrentHashMap<>();

	private static final String DEFAULT_TAPDATA_MONGO_URI = "mongodb://mongo:27017/tapdata";

	private static final List<String> DEFAULT_BASE_URLS = Arrays.asList("http://127.0.0.1:3000/api/");

	private static final String CONNECTOR = "Connector";

	private String mongoURI;

	private boolean ssl;

	private String sslCA;

	private String sslPEM;

	private String mongodbConnParams;

	private List<String> baseURLs;

	private String accessCode;

	private Integer restRetryTime;

	private String mode;

	@Autowired
	private ConfigurationCenter configCenter;

	private String instanceNo = "tapdata-agent-connector";

	@Autowired
	private ClientMongoOperator clientMongoOperator;

	@Autowired
	private ClientMongoOperator pingClientMongoOperator;

	@Autowired
	private RestTemplateOperator restTemplateOperator;


	private WarningMaker warningMaker;

	@Autowired
	private SettingService settingService;

	@Autowired
	private MessageDao messageDao;

	private LoadBalancing loadBalancing;

	private String version;

	private List<Lib> libs;
	private List<DatabaseTypeEnum.DatabaseType> databaseTypes;
	private List<LibSupported> supporteds;
	private List<Converter> converters;

	private TapdataTaskScheduler scriptTaskScheduler;

	private ClassScanner classScanner;

	private ExecutorService loadSchemaThreadPool;

	private int availableProcessors;

	private ExecutorService schemaUpdatePool;

	private ExecutorService stopJobThreadPool;
	private final String stopJobThreadName = "Stop Connector runner Thread-%s-[%s]";
	private ConcurrentHashMap<String, ConnectorStopJob> stopJobMap = new ConcurrentHashMap<>();

	private AppType appType;

	@Autowired
	private MetricManager metricManager;

	private final ExecutorService scheduleJobExecutorService = Executors.newFixedThreadPool(Math.max(Runtime.getRuntime().availableProcessors() * 2, 8));

	private String tapdataWorkDir;

	/**
	 * 云版可用区
	 */
	private String jobTags;
	private String region;
	private String zone;

	@Autowired
	private SchemaProxy schemaProxy;

	@PostConstruct
	public void init() throws Exception {
		availableProcessors = Runtime.getRuntime().availableProcessors();
		logger.info("Available processors number: {}", availableProcessors);

		version = VersionCheck.getVersion();
		logger.info("Java class path: " + System.getProperty("java.class.path") + ".\n"
				+ " Agent version: " + version);

		if (!checkBaseURLs()) {
			String err = "Tapdata agent start error, tapdata.cloud.baseURLs is empty. Please check application.yml";
			throw new RuntimeException(err);
		}

		login();

		settingService.loadSettings();

		Map<String, Object> params = new HashMap<>();
		params.put("process_id", instanceNo);
		params.put("worker_type", ConnectorConstant.WORKER_TYPE_CONNECTOR);

		// 限制云版一个用户只能启动一个flow engine
		// 因为设计有缺陷，严重影响重启和升级流程，所以暂时屏蔽，后续完善后恢复
/*String checkCloudOneAgentResult = checkCloudOneAgent();
    if (StringUtils.isNotBlank(checkCloudOneAgentResult)) {
      throw new RuntimeException(checkCloudOneAgentResult);
    }*/

		WorkerSingletonLock.check(tapdataWorkDir, (singletonLock) -> {
			String newSingletonLock = UUID.randomUUID().toString();
			String status = clientMongoOperator.upsert(new HashMap<String, Object>() {{
				put("process_id", instanceNo);
				put("worker_type", ConnectorConstant.WORKER_TYPE_CONNECTOR);
				put("singletonLock", singletonLock);
			}}, new HashMap<String, Object>() {{
				put("singletonLock", newSingletonLock);
			}}, ConnectorConstant.WORKER_COLLECTION + "/singleton-lock", String.class);
			if (!"ok".equals(status)) {
				throw new RuntimeException(String.format("Singleton check in remote failed: '%s'", status));
			}
			return newSingletonLock;
		});

		List<Worker> workers = clientMongoOperator.find(params, ConnectorConstant.WORKER_COLLECTION, Worker.class);

		if (CollectionUtils.isNotEmpty(workers)) {
			params.clear();
			for (Worker worker : workers) {
				String id = worker.getId();
				params.put("id", id);

				Map<String, Object> updateData = new HashMap<>();
				updateData.put("createTime", new Date());
				updateData.put("stopping", false);

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
		} else if (CollectionUtils.isEmpty(workers) && appType.isDrs()) {
			String err = "Not found worker with params: " + params + ", will exit progress";
			throw new RuntimeException(err);
		}

		warningMaker = new WarningMaker(clientMongoOperator);

		addHTTPAppender();

		loadBalancing = new LoadBalancing(mode, instanceNo, ConnectorConstant.WORKER_TYPE_CONNECTOR, clientMongoOperator);

		// init lib(s)
		Setting buildProfileSetting = settingService.getSetting("buildProfile");
		String buildProfileSettingValue = buildProfileSetting.getValue();
		classScanner = new ClassScanner();
		classScanner.setBuildProfile(buildProfileSettingValue);
		libs = classScanner.initLibs();
		converters = classScanner.loadConverters();

		if (!appType.isCloud()) {
			// init database type(s)
//			initDatabaseTypes();

			// init supported list
//			initSupportedList();

			// init script task schedule
			initScriptTaskSchedule(clientMongoOperator);

			// init type mappings
      /*try {
        long startTs = System.currentTimeMillis();
        logger.info("Starting init job engine type mapping");
        TypeMappingUtil.initJobEngineTypeMappings(clientMongoOperator);
        logger.info("Init job engine type mapping complete, spend: " + (System.currentTimeMillis() - startTs) + " ms");
      } catch (Exception e) {
        String msg = "Init job engine type mapping failed, cause: " + e.getMessage();
        throw new Exception(msg, e);
      }*/
		}
		configCenter.putConfig(ConfigurationCenter.BASR_URLS, baseURLs);
		configCenter.putConfig(ConfigurationCenter.RETRY_TIME, restRetryTime);
		configCenter.putConfig(ConfigurationCenter.AGENT_ID, instanceNo);
		configCenter.putConfig(ConfigurationCenter.APPTYPE, null == appType ? AppType.DAAS : appType);
		configCenter.putConfig(ConfigurationCenter.IS_CLOUD, appType.isCloud());
		configCenter.putConfig(ConfigurationCenter.WORK_DIR, null == tapdataWorkDir ? "" : tapdataWorkDir);
		Optional.ofNullable(jobTags).ifPresent(j -> configCenter.putConfig(ConfigurationCenter.JOB_TAGS, jobTags));
		Optional.ofNullable(region).ifPresent(j -> configCenter.putConfig(ConfigurationCenter.REGION, region));
		Optional.ofNullable(zone).ifPresent(j -> configCenter.putConfig(ConfigurationCenter.ZONE, zone));

		loadSchemaThreadPool = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<>());

		schemaUpdatePool = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
				0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>());

		stopJobThreadPool = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
				0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>());

		SchemaProxy.schemaProxy = schemaProxy;

		ConnectorConstant.clientMongoOperator = clientMongoOperator;

		GlobalConstant.getInstance().configurationCenter(configCenter);
	}

	@PreDestroy
	public void destroy() {
		scheduleJobExecutorService.shutdownNow();
	}

	private String checkCloudOneAgent() {
		if (appType.isDfs()) {
			String userId = (String) configCenter.getConfig(ConfigurationCenter.USER_ID);
			if (StringUtils.isNotBlank(userId)) {
				Query query = new Query(Criteria.where("ping_time")
						.is(new Document("gte", "$serverDate").append("gte_offset", 60000))
						.and("user_id").regex("^" + userId + "$")
						.and("worker_type").is(ConnectorConstant.WORKER_TYPE_CONNECTOR));

				List<Worker> workers = clientMongoOperator.find(query, ConnectorConstant.WORKER_COLLECTION, Worker.class);
				int count = workers.size();
				if (count > 0) {
					// progress exit
					Worker worker = workers.get(0);
					String err = TapLog.ERROR_0007.getMsg() + "\n" + "Active agent id: "
							+ worker.getId() + ", ip: " + worker.getWorker_ip() + ", host name: "
							+ worker.getHostname() + ", process id: " + worker.getProcess_id() + ", ping time: " + worker.getPing_time();
					return err;
				}
			}
		}
		return "";
	}

	private void initScriptTaskSchedule(ClientMongoOperator clientMongoOperator) {
		if (StringUtils.isNotBlank(mongoURI)) {
			scriptTaskScheduler = new TapdataTaskScheduler(clientMongoOperator, mongoURI, settingService, mongodbConnParams);
			scriptTaskScheduler.start();
		}
	}

	private boolean checkBaseURLs() {
		if (CollectionUtils.isNotEmpty(baseURLs)) {
			for (String baseURL : baseURLs) {
				if (StringUtils.isNotBlank(baseURL)) {
					return true;
				}
			}
		}

		return false;
	}

	private void initDatabaseTypes() throws IllegalAccessException {
		// delete unknown database type
		clientMongoOperator.delete(new Query(where("type").is(DatabaseTypeEnum.UNKNOWN.getType())), ConnectorConstant.DATABASE_TYPE_COLLECTION);

		databaseTypes = classScanner.getDatabaseTypes();
		if (CollectionUtils.isNotEmpty(databaseTypes)) {
			for (DatabaseTypeEnum.DatabaseType databaseType : databaseTypes) {
				if (databaseType.getType().equals(DatabaseTypeEnum.UNKNOWN.getType())) {
					continue;
				}
				Map<String, Object> params = new HashMap<>();
				params.put("type", databaseType.getType());

				clientMongoOperator.upsert(params, MapUtil.obj2Map(databaseType), ConnectorConstant.DATABASE_TYPE_COLLECTION);
			}
		}
	}

	private void initSupportedList() {
		Setting libSupported = settingService.getSetting("libSupported");
		String libSupportedStr = libSupported.getValue();
		SupportUtil supportUtil = new SupportUtil(libSupportedStr, libs, databaseTypes);
		clientMongoOperator.dropCollection(ConnectorConstant.LIB_SUPPORTEDS_COLLECTION);
		supporteds = supportUtil.getSupporteds();
		clientMongoOperator.insertList(supporteds, ConnectorConstant.LIB_SUPPORTEDS_COLLECTION);
	}

	private void login() throws InterruptedException {
		LoginResp loginResp = null;
		while (loginResp == null) {
			try {

				if (!appType.isCloud()) {

					MongoTemplate mongoTemplate = clientMongoOperator.getMongoTemplate();
					List<User> users = mongoTemplate.find(new Query(where("role").is(1)), User.class, "User");
					if (CollectionUtils.isNotEmpty(users)) {
						User user = users.get(0);
						String accesscode = user.getAccesscode();
						if (StringUtils.isNotBlank(accesscode)) {
							this.accessCode = accesscode;
						}
					} else {
						logger.warn("Cannot find admin user from mongodb {}.", mongoURI);
					}
				}

				configCenter.putConfig(ConfigurationCenter.ACCESS_CODE, accessCode);

				Map<String, Object> params = new HashMap<>();
				params.put("accesscode", accessCode);
				logger.info("Login params: accessCode={}, endpoint={}", accessCode, baseURLs);
				loginResp = restTemplateOperator.postOne(params, "users/generatetoken", LoginResp.class);
				if (loginResp != null) {
					Date date = (Date) DateUtil.parse(loginResp.getCreated());
					long expiredTimestamp = date.getTime() + (loginResp.getTtl() * 1000);
					loginResp.setExpiredTimestamp(expiredTimestamp);

					configCenter.putConfig(ConfigurationCenter.TOKEN, loginResp.getId());
					configCenter.putConfig(ConfigurationCenter.USER_ID, loginResp.getUserId());
					configCenter.putConfig(ConfigurationCenter.LOGIN_INFO, loginResp);


					StringBuilder sb = new StringBuilder("users").append("/").append(loginResp.getUserId());
					User user = clientMongoOperator.findOne(new Query(), sb.toString(), User.class);

					if (user != null) {
						configCenter.putConfig(ConfigurationCenter.USER_INFO, user);
						user.setRole(user.getRole() == null ? 0 : user.getRole());
					}

					AspectUtils.executeAspect(LoginSuccessfullyAspect.class, () -> new LoginSuccessfullyAspect()
							.configCenter(configCenter)
							.baseUrls(restTemplateOperator.getBaseURLs())
							.user(user));
				} else {
					logger.warn("Login fail response {}, waiting 60(s) retry.", loginResp);
					Thread.sleep(60000L);
				}
			} catch (Exception e) {
				logger.error("Login fail {}, waiting 60(s) retry.", e.getMessage(), e);
				Thread.sleep(60000L);
			}
		}

	}

	@Bean("restTemplateOperator")
	public RestTemplateOperator initRestTemplate() {
		initVariable();
		restTemplateOperator = new RestTemplateOperator(
				baseURLs,
				restRetryTime,
				() -> {
					long jobHeartTimeout = settingService.getLong("jobHeartTimeout", 60000L);
					return jobHeartTimeout * 0.8 > 30000 ? (long) (jobHeartTimeout * 0.8) : 30000L;
				}
		);

		return restTemplateOperator;
	}

	@Bean("configCenter")
	@DependsOn("restTemplateOperator")
	public ConfigurationCenter initConfiguration() {

		ConfigurationCenter configCenter = new ConfigurationCenter();
		return configCenter;
	}

	@Bean(value = "metricManager", initMethod = "init")
	@DependsOn("settingService")
	public MetricManager initMetricManager() {

		this.metricManager = new MetricManager(settingService);
		return metricManager;
	}

	@Bean(value = "performanceStatistics")
	@DependsOn("settingService")
	public PerformanceStatistics performanceStatistics() {
		return new PerformanceStatistics(settingService);
	}

	@Bean("settingService")
	@DependsOn("clientMongoOperator")
	public SettingService initSettingsService() {

		settingService = new SettingService(clientMongoOperator);
		return settingService;
	}

	@Bean("clientMongoOperator")
	@DependsOn({"restTemplateOperator", "configCenter"})
	@Primary
	public ClientMongoOperator initMongoOperator() {
		MongoTemplate mongoTemplate = null;
		MongoClient client = null;
		MongoClientURI uri = null;
		try {
			if (StringUtils.isNotBlank(mongoURI)) {
				MongoClientOptions.Builder builder = MongoClientOptions.builder();
				builder.codecRegistry(MongodbUtil.getForJavaCoedcRegistry());

				if (ssl) {
					List<String> trustCertificates = SSLUtil.retriveCertificates(sslCA);
					String privateKey = SSLUtil.retrivePrivateKey(sslPEM);
					List<String> certificates = SSLUtil.retriveCertificates(sslPEM);

					SSLContext sslContext = SSLUtil.createSSLContext(privateKey, certificates, trustCertificates, "tapdata");
					builder.sslContext(sslContext).sslEnabled(true).sslInvalidHostNameAllowed(true);
				}
				uri = new MongoClientURI(mongoURI, builder);
				client = new MongoClient(uri);
				mongoTemplate = new MongoTemplate(client, uri.getDatabase());
			}
			clientMongoOperator = new HttpClientMongoOperator(mongoTemplate, client, uri, restTemplateOperator, configCenter);
			clientMongoOperator.setCloudRegion(jobTags);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return clientMongoOperator;
	}

	@Bean("pingClientMongoOperator")
	@DependsOn({"restTemplateOperator", "configCenter"})
	public ClientMongoOperator initPingMongoOperator() {
		MongoTemplate mongoTemplate = null;
		MongoClient client = null;
		MongoClientURI uri = null;
		try {
			if (StringUtils.isNotBlank(mongoURI)) {
				MongoClientOptions.Builder builder = MongoClientOptions.builder();
				builder.codecRegistry(MongodbUtil.getForJavaCoedcRegistry());

				if (ssl) {
					List<String> trustCertificates = SSLUtil.retriveCertificates(sslCA);
					String privateKey = SSLUtil.retrivePrivateKey(sslPEM);
					List<String> certificates = SSLUtil.retriveCertificates(sslPEM);

					SSLContext sslContext = SSLUtil.createSSLContext(privateKey, certificates, trustCertificates, "tapdata");
					builder.sslContext(sslContext).sslEnabled(true).sslInvalidHostNameAllowed(true);
				}
				uri = new MongoClientURI(mongoURI, builder);
				client = new MongoClient(uri);
				mongoTemplate = new MongoTemplate(client, uri.getDatabase());
			}
			pingClientMongoOperator = new HttpClientMongoOperator(mongoTemplate, client, uri, new RestTemplateOperator(
					baseURLs,
					restRetryTime,
					() -> {
						return 2000L;
					}, 1000, 30000, 30000
			), configCenter);
			pingClientMongoOperator.setCloudRegion(jobTags);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return pingClientMongoOperator;
	}

	@Bean("targetProgressRateStats")
	public ProgressRateStatsMap initTargetProgressRateStats() {
		ProgressRateStatsMap progressRateStatsMap = new ProgressRateStatsMap();

		return progressRateStatsMap;
	}

	@Bean("schemaProxy")
	@DependsOn("clientMongoOperator")
	public SchemaProxy initSchemaProxy() {
		return SchemaProxy.SchemaProxyInstance.INSTANCE.getInstance(clientMongoOperator);
	}

	@Scheduled(fixedDelay = 2000L)
	public void refreshToken() throws InterruptedException {
		Thread.currentThread().setName(String.format(ConnectorConstant.REFREASH_TOKEN_THREAD, CONNECTOR, instanceNo.substring(instanceNo.length() - 6)));
		User user = (User) configCenter.getConfig(ConfigurationCenter.USER_INFO);
		StringBuilder sb = new StringBuilder("users").append("/").append(user.getId());

		User dbUser = null;
		try {
			dbUser = clientMongoOperator.findOne(new Query(), sb.toString(), User.class);
		} catch (Exception e) {
			logger.error("Check token status failed {} then to refresh token info.", e.getMessage(), e);
		}
		if (dbUser == null) {
			login();
		}

	}

//  @Scheduled(fixedDelay = 2000L)
//  public void scanJob() {
//    Thread.currentThread().setName(String.format(ConnectorConstant.START_JOB_THREAD, CONNECTOR, instanceNo.substring(instanceNo.length() - 6)));
//    try {
//      Job runningJob = runningJob();
//      if (runningJob == null) {
//        ThreadContext.clearAll();
//        return;
//      }
//      Log4jUtil.setThreadContext(runningJob);
//
//      // 防止缓存任务重复入队
//      if (JOB_SET.contains(runningJob.getId())) {
//        logger.info("job [{}|{}] is already scheduling", runningJob.getId(), runningJob.getName());
//        return;
//      }
//      JOB_SET.add(runningJob.getId());
//
//      if (JOB_MAP.containsKey(runningJob.getId())) {
//        logger.info("job [{}|{}] is already running, no need to scheduled", runningJob.getId(), runningJob.getName());
//        return;
//      }
//      logger.info("Found scheduled job {}", runningJob.getName());
//      final Runnable startJobRunnable = () -> {
//
//        Thread.currentThread().setName(String.format("Start job thread, job name %s , job id %s", runningJob.getName(), runningJob.getId()));
//
//        Log4jUtil.setThreadContext(runningJob);
//
//        JobCustomerLogger customerLogger = new JobCustomerLogger(runningJob.getDataFlowId(), runningJob.getName(), clientMongoOperator);
//        runningJob.setJobCustomerLogger(customerLogger);
//        runningJob.setClientMongoOperator(clientMongoOperator);
//
//        String[] splitName = ManagementFactory.getRuntimeMXBean().getName().split("@");
//        customerLogger.info(CustomerLogMessagesEnum.AGENT_CREATE_JOB, ImmutableMap.of(
//          "agentHost", splitName.length >= 2 ? splitName[1] : "",
//          "jobInfo", runningJob.getJobInfo()
//        ));
//
//        Setting sampleRate = settingService.getSetting("sampleRate");
//        String sampleRateValue = sampleRate != null ? sampleRate.getValue() : "1";
//        if (StringUtils.isBlank(sampleRateValue)) {
//          sampleRateValue = sampleRate != null ? sampleRate.getDefault_value() : "1";
//        }
//        MilestoneJobService milestoneJobService = null;
//        try {
//          Log4jUtil.setThreadContext(runningJob);
//          LoadBalancing.runningThreadNum++;
//          logger.debug(TapLog.D_CONN_LOG_0005.getMsg(), runningJob.toString());
//
//          customerLogger.info(CustomerLogMessagesEnum.AGENT_CHECK_AVAILABILITY);
//          // 获取源端和目标的数据库信息
//          Connections sourceConn = runningJob.getConn(true, clientMongoOperator, null);
//          if (CollectionUtils.isNotEmpty(runningJob.getStages()) &&
//            runningJob.getStages().stream().anyMatch(stage -> Stage.StageTypeEnum.LOG_COLLECT.getType().equals(stage.getType()))) {
//            sourceConn.setDatabase_type(DatabaseTypeEnum.LOG_COLLECT.getType());
//          }
//          // test connection for source
//          TestConnectionHandler.testConnectionWithRetry(customerLogger, sourceConn, "source");
//
//          Connections targetConn = runningJob.getConn(false, clientMongoOperator, sourceConn);
//          //test connection for target
//          TestConnectionHandler.testConnectionWithRetry(customerLogger, targetConn, "target");
//
//          // 重置任务offset，如果包含聚合节点
//          // JobUtil.resetJobOffset(runningJob, clientMongoOperator);
//
//          // 设置全局缓存
//          messageDao.getCacheJob(runningJob);
//          TapdataShareContext tapdataShareContext = messageDao.getCacheTapdataShareContext(runningJob);
//          LinkedBlockingQueue messageQueue = messageDao.createJobMessageQueue(runningJob);
//
//          // 通知transformer运行任务
//          messageDao.getRunningJobsQueue().put(runningJob.getId());
//
//          if (NumberUtils.isParsable(sampleRateValue)) {
//            runningJob.setSampleRate(Double.valueOf(sampleRateValue));
//          }
//
//          Stats stats = runningJob.getStats();
//          if (stats == null) {
//            stats = new Stats();
//            runningJob.setStats(stats);
//          }
//
//          JOB_MAP.put(runningJob.getId(), runningJob);
//
//          // 清理模型缓存
//          SchemaProxy.getSchemaProxy().clear(sourceConn.getId());
//          SchemaProxy.getSchemaProxy().clear(targetConn.getId());
//
//          // 初始化里程碑业务类
//          try {
//            milestoneJobService = MilestoneFactory.getJobMilestoneService(runningJob, clientMongoOperator);
//          } catch (Exception e) {
//            logger.warn("Init job milestone failed, id: {}, name: {}, err: {}", runningJob.getId(), runningJob.getName(), e.getMessage(), e);
//          }
//
//          // Milestone-INIT_CONNECTOR-RUNNING
//          MilestoneUtil.updateMilestone(milestoneJobService, MilestoneStage.INIT_CONNECTOR, MilestoneStatus.RUNNING);
//
//          // 设置文件字符集
//          setFileDefaultCharset(sourceConn);
//
//          stats.initInitialCount(runningJob, sourceConn, targetConn);
//
//          Map<String, List<DataRules>> dataRulesMap = MongodbUtil.getDataRules(clientMongoOperator, targetConn);
//          if (dataRulesMap == null) {
//            logger.error(TapLog.CONN_ERROR_0025.getMsg());
//          }
//
//          logger.info(TapLog.JOB_LOG_0001.getMsg(), runningJob.getName());
//          User user = (User) configCenter.getConfig(ConfigurationCenter.USER_INFO);
//
//          DebugContext debugContext = new DebugContext(runningJob, clientMongoOperator, sourceConn, targetConn);
//          DebugProcessor debugProcessor = new DebugProcessor(debugContext);
//
//          // 缓存注册
//          messageDao.registerCache(runningJob, clientMongoOperator);
//
//          Connector connector = ConnectorJobManager.prepare(runningJob, clientMongoOperator, sourceConn,
//            messageQueue, targetConn, baseURLs, (String) configCenter.getConfig(ConfigurationCenter.ACCESS_CODE),
//            dataRulesMap, restRetryTime, settingService, user.getId(), user.getRole(), debugProcessor,
//            messageDao.getCacheService(), tapdataShareContext, appType.isCloud(),
//            milestoneJobService, configCenter);
//          if (connector != null) {
//            // start connector threads
//            startedJob(runningJob);
//            JOB_THREADS.put(runningJob.getId(), connector);
//            JOB_STATS.put(runningJob.getId(), new HashMap<>(stats.getTotal()));
//            connector.start();
//
//            // 旧的架构
//            if (connector.getSource() == null) {
//              // Milestone-INIT_CONNECTOR-FINISH
//              MilestoneUtil.updateMilestone(milestoneJobService, MilestoneStage.INIT_CONNECTOR, MilestoneStatus.FINISH);
//            }
//          } else {
//            // Milestone-INIT_CONNECTOR-FINISH
//            MilestoneUtil.updateMilestone(milestoneJobService, MilestoneStage.INIT_CONNECTOR, MilestoneStatus.FINISH);
//            interruptJob(runningJob);
//          }
//        } catch (ManagementException e) {
//          logger.warn(TapLog.JOB_WARN_0004.getMsg(), runningJob.getName(), Log4jUtil.getStackString(e));
//        } catch (Exception e) {
//          String errMsg = String.format(TapLog.JOB_ERROR_0002.getMsg(), runningJob.getName(), e.getMessage());
//
//          // Milestone-INIT_CONNECTOR-ERROR
//          Optional.ofNullable(milestoneJobService).ifPresent(m -> m.updateMilestoneStatusByCode(MilestoneStage.INIT_CONNECTOR, MilestoneStatus.ERROR, errMsg));
//          runningJob.jobError(e, true, "", logger, ConnectorConstant.WORKER_TYPE_CONNECTOR,
//            JobCustomerLogger.CUSTOMER_ERROR_LOG_PREFIX + errMsg, null);
//          customerLogger.error(ErrorCodeEnum.FATAL_INIT_CONNECTOR_FAILED);
//
//          try {
//            // 停止前汇报一次任务状态
//            flushJobStats(runningJob);
//          } catch (Exception ignore) {
//          }
//          interruptJob(runningJob);
//        } finally {
//          ThreadContext.clearAll();
//        }
//
//        if (!JOB_MAP.containsKey(runningJob.getId())) {
//          logger.info("Found scheduled job {}, but cannot handle it, maybe has any error.", runningJob.getName());
//        }
//      };
//
//      this.scheduleJobExecutorService.execute(startJobRunnable);
//
//    } catch (Exception e) {
//      logger.error("scanJob happen exception:", e);
//    }
//  }

	//	@Scheduled(fixedDelay = 2000L)
	private void scanStopJob() {
		Thread.currentThread().setName(String.format(ConnectorConstant.STOP_JOB_THREAD, CONNECTOR, instanceNo.substring(instanceNo.length() - 6)));
		try {
			// stop the stopping job
			List<Job> stoppedJobs = stoppingJob(false);

			if (CollectionUtils.isNotEmpty(stoppedJobs)) {
				stoppedJobs.forEach(job -> {
					if (JOB_MAP.containsKey(job.getId())) {
						try {
							Log4jUtil.setThreadContext(job);
							logger.info(TapLog.CON_LOG_0015.getMsg());

							stopJob(job, false, false, false);
						} finally {
							ThreadContext.clearAll();
						}
					} else {
						logger.info("Found need to stopped job {}, but not in job map.", job.getName());

						stoppedJob(job);
					}
				});
			} else {
				if (MapUtils.isNotEmpty(ERR_JOB_MAP) && !appType.isCloud()) {
					logger.warn("Stopping all connectors, because of all rest api call failed");
					ERR_JOB_MAP.forEach((jobId, job) -> stopJob(job, true, true, false));
					ERR_JOB_MAP.clear();
					logger.warn("Finished stop connectors.");
				}
			}
		} catch (Exception e) {
			logger.error("Scan stopping job failed {}", e.getMessage(), e);
		}

		if (MapUtils.isNotEmpty(stopJobMap)) {
			Iterator<String> iterator = stopJobMap.keySet().iterator();
			while (iterator.hasNext()) {
				try {
					String jobId = iterator.next();
					ConnectorStopJob connectorStopJob = stopJobMap.get(jobId);
					Job job = connectorStopJob.getJob();
					Log4jUtil.setThreadContext(job);

					// if stop connector runner
					if (connectorStopJob.getFuture().isDone()) {
						try {
							flushJobStats(job);
						} catch (Exception e) {
							logger.warn(TapLog.W_JOG_LOG_0002.getMsg(), e.getMessage(), Log4jUtil.getStackString(e));
						}

						messageDao.removeJobMessageQueue(job.getId());
						messageDao.removeJobCache(job.getId());
						messageDao.removeTapdataShareContext(job.getId());
						removeMapIfNeed(job.getId());
						iterator.remove();
						logger.info(TapLog.JOB_LOG_0013.getMsg(), job.getName());

						if (!connectorStopJob.isInternalStop()) {
							stoppedJob(job);
						}
						LoadBalancing.runningThreadNum--;

						logger.info(TapLog.CON_LOG_0014.getMsg(), job.getName());
					}
				} finally {
					ThreadContext.clearAll();
				}
			}
		}
	}

	//	@Scheduled(fixedDelay = 2000L)
	private void scanErrorJob() {
		Thread.currentThread().setName(String.format(ConnectorConstant.ERROR_JOB_THREAD, CONNECTOR, instanceNo.substring(instanceNo.length() - 6)));
		// if the job happen runtime exception, remove the jobmap
		for (Iterator<Map.Entry<String, Job>> it = JOB_MAP.entrySet().iterator(); it.hasNext(); ) {
			try {
				Map.Entry<String, Job> entry = it.next();
				Job job = entry.getValue();
				if (ConnectorConstant.ERROR.equals(job.getStatus())) {
					Log4jUtil.setThreadContext(job);
					logger.info("Found the error job: {}[{}], will stop it", job.getName(), job.getId());
					flushJobStats(job);
					stopJob(job, true, false, true);
				}
			} catch (Exception e) {
				logger.error("Scan error jobs failed {}", e.getMessage(), e);
			} finally {
				ThreadContext.clearAll();
			}
		}
	}

	//	@Scheduled(fixedDelay = 2000L)
	public void scanForceStopJob() {
		Thread.currentThread().setName(String.format(ConnectorConstant.FORCE_STOP_JOB_THREAD, CONNECTOR, instanceNo.substring(instanceNo.length() - 6)));
		try {
			List<Job> stoppedJobs = stoppingJob(true);
			if (CollectionUtils.isNotEmpty(stoppedJobs)) {
				stoppedJobs.forEach(job -> {
					if (JOB_MAP.containsKey(job.getId())) {
						try {
							Log4jUtil.setThreadContext(job);
							logger.info(TapLog.CON_LOG_0016.getMsg());
							stopJob(job, true, false, false);
						} catch (Exception e) {
							logger.error(TapLog.CONN_ERROR_0030.getMsg(), job.getName(), e.getMessage(), e);
						} finally {
							ThreadContext.clearAll();
						}
					} else {
						logger.info("Found need to stopped job {}, but not in job map.", job.getName());

						stoppedJob(job);
					}
				});
			}
		} catch (Exception e) {
			logger.error("Scan force stop jobs failed {}", e.getMessage(), e);
		}
	}

	//	@Scheduled(fixedDelay = 5000L)
	public void perSecondFlushJobStats() {
		Thread.currentThread().setName(String.format(ConnectorConstant.STATS_JOB_THREAD, CONNECTOR, instanceNo.substring(instanceNo.length() - 6)));
		try {
			long startTs = System.currentTimeMillis();

			if (!pingAllJobs()) return;

			long pingJobsEndTs = System.currentTimeMillis();
			if ((pingJobsEndTs - startTs) > LONG_TIME_EXECUTED_CAPACITY) {
				logger.info("Ping all jobs spent {}ms.", (pingJobsEndTs - startTs));
			}

			JOB_MAP.forEach((jobId, job) -> {
				try {

					Log4jUtil.setThreadContext(job);

					flushJobStats(job);

				} catch (Exception e) {
					logger.warn(TapLog.W_JOG_LOG_0002.getMsg(), e.getMessage(), Log4jUtil.getStackString(e));
				} finally {
					ThreadContext.clearAll();
				}
			});

			long endTs = System.currentTimeMillis();
			if ((endTs - startTs) > LONG_TIME_EXECUTED_CAPACITY) {
				logger.info("Report all jobs' stats spent {}ms.", (endTs - startTs));
			}
		} catch (Exception e) {
			logger.error("Flush jobs' stats to db failed {}", e.getMessage(), e);
		}
	}

	private boolean pingAllJobs() {
		int i = 0;
		List<String> ids = new ArrayList<>();
		for (String jobId : JOB_MAP.keySet()) {
			ids.add(jobId);
		}

		try {
			pingClientMongoOperator.update(
					new Query(where("_id").is(new Document("inq", ids))),
					new Update()
							.set(ConnectorConstant.JOB_CONNECTOR_PING_TIME_FIELD, System.currentTimeMillis())
							.set(ConnectorConstant.JOB_PING_TIME_FIELD, System.currentTimeMillis()),
					ConnectorConstant.JOB_COLLECTION
			);
			return true;
		} catch (Throwable e) {
			logger.info("Update ping all job error: {}", e.getMessage());
			for (Job job : JOB_MAP.values()) {
				job.setConnector_ping_time(System.currentTimeMillis());
				job.setPing_time(System.currentTimeMillis());
			}
			return false;
		}
	}

	private void flushJobStats(Job job) throws Exception {
		Log4jUtil.setThreadContext(job);
		if (JOB_MAP.containsKey(job.getId())) {
			job = JOB_MAP.get(job.getId());
		}
		Stats stats = job.getStats();
		Map<String, LinkedList<Long>> perSecond = stats.getPer_second();

		Map<String, Long> total = stats.getTotal();
		Map<String, Long> previousTotal = JOB_STATS.get(job.getId());
		if (MapUtils.isEmpty(previousTotal)) {
			previousTotal = new HashMap<>();
			previousTotal.put("source_received", new Long(0));
		}
		Long sourceReceived = total.get("source_received");

		Long previousSourceReceived = previousTotal.getOrDefault("source_received", new Long(0));

		LinkedList<Long> perSourceReceied = perSecond.getOrDefault("source_received", new LinkedList<>());

		if (perSourceReceied.size() >= 20) {
			perSourceReceied.removeFirst();
		}

		long currentTimeMillis = System.currentTimeMillis();
		long lastStatsTimestamp = job.getLastStatsTimestamp();
		long intervalSecs = (currentTimeMillis - lastStatsTimestamp) / 1000;
		intervalSecs = intervalSecs <= 0 ? 1 : intervalSecs;
		perSourceReceied.addLast((long) Math.ceil((double) (sourceReceived - previousSourceReceived) / (double) intervalSecs));

		Map<String, Object> params = new HashMap<>();
		Document connectorStatusParams = new Document();
		connectorStatusParams.append("$ne", true);

		params.put("_id", job.getId());
		params.put("process_id", instanceNo);
		params.put("agentId", instanceNo);
		params.put("connectorStopped", connectorStatusParams);

		Map<String, Object> update = new HashMap<>();
		update.put("stats.per_second.source_received", perSourceReceied);
		update.put("stats.total.source_received", sourceReceived);
		update.put("connector_ping_time", System.currentTimeMillis());
		update.put("fullSyncSucc", job.getFullSyncSucc());

		Map<String, Long> validateStats = stats.getValidate_stats();
		if (validateStats != null && validateStats.size() > 0) {
			update.put("stats.validate_stats", validateStats);
			update.put("validate_offset", job.getValidate_offset());
		}
		List<Map<String, Object>> totalCount = stats.getTotalCount();
		if (CollectionUtils.isNotEmpty(totalCount)) {
			totalCount = totalCount.parallelStream().filter(Objects::nonNull).collect(Collectors.toList());
			update.put("stats.totalCount", totalCount);
		}

		update.put("connectorErrorEvents", job.getConnectorErrorEvents() == null ? new ArrayList<>() : job.getConnectorErrorEvents());
		update.put("connectorLastSyncStage", job.getConnectorLastSyncStage());

		job.setLastStatsTimestamp(currentTimeMillis);

		if (!job.isEditDebug()) {
			UpdateResult updateResult = clientMongoOperator.updateAndParam(params, update, ConnectorConstant.JOB_COLLECTION);
			long modifiedCount = updateResult.getModifiedCount();

			final JobConnection connections = job.getConnections();
			// 缓存任务需要在多个节点上运行
			if (modifiedCount <= 0 && job.isRunning() && !connections.getCacheTarget()) {
				logger.warn("Job owner changed, abort my current process, name {}.", job.getName());
				stopJob(job, true, true, false);
			}

		}
		JOB_STATS.put(job.getId(), new HashMap<>(total));
	}

	/**
	 * 测试连接方法
	 * 定时轮询状态为testing的连接
	 */
//	@Scheduled(fixedDelay = 2000L)
	public void testConnection() {
		String userId = (String) configCenter.getConfig(ConfigurationCenter.USER_ID);
		String workerTimeout = settingService.getString("lastHeartbeat", "60");
		if (!AgentUtil.isFirstWorker(
				clientMongoOperator,
				instanceNo,
				appType.isCloud() ? userId : null,
				Double.valueOf(workerTimeout)
		)
		) {
			return;
		}
		Thread.currentThread().setName(String.format(ConnectorConstant.TEST_CONNECTION_THREAD, CONNECTOR, instanceNo.substring(instanceNo.length() - 6)));
		Query query = new Query(new Criteria().andOperator(
				currentUserCriteria(),
				new Criteria().orOperator(
						where("status").is("testing"),
						where("status").is("invalid").and("response_body.next_retry").lte(System.currentTimeMillis())),
				Criteria.where("database_type").in(Arrays.asList("gridfs", "rest api", "tcp_udp", "bitsflow", "gbase-8s", "custom_connection"))
		));
		query.fields().exclude("schema");
		List<Connections> connections = MongodbUtil.getConnections(query, null, clientMongoOperator, true);
		if (CollectionUtils.isNotEmpty(connections)) {
			for (Connections connection : connections) {

				try {
					logger.info(TapLog.CON_LOG_0021.getMsg(), connection.getName());

					// workaround.
					setFileDefaultCharset(connection);

					ConnectionValidateResult validateResult = ConnectionValidator.initialValidate(connection);
					List validateResultDetails = validateResult.getValidateResultDetails();

					// get retry and next_try info
					Map<String, Object> responseBody = connection.getResponse_body();
					if (MapUtils.isNotEmpty(responseBody) && responseBody.containsKey("retry")) {
						Object retry = responseBody.get("retry");
						validateResult.setRetry(retry == null ? 0 : (Integer) retry);
					}

					TapInterface tapInterface = null;
					String databaseType = connection.getDatabase_type();
					if (CollectionUtils.isEmpty(validateResultDetails)) {
						tapInterface = TapInterfaceUtil.getTapInterface(databaseType, null);
						if (tapInterface != null) {
							validateResultDetails = tapInterface.connectionsInit(ConnectionsType.getConnectionType(connection.getConnection_type()));
						}
					}

					Update update = new Update();
					update.set("response_body.validate_details", validateResultDetails);

					Query updateQuery = new Query(where("_id").is(connection.getId()));
					clientMongoOperator.update(query, update, ConnectorConstant.CONNECTION_COLLECTION);

					connection.setSampleSize(settingService.getInt("connections.mongodbLoadSchemaSampleSize", 100));
					long startTs = System.currentTimeMillis();
					validateResult = ConnectionValidator.validate(connection, validateResult);

					Schema schema = null;

					if (validateResult != null) {

						List<RelateDataBaseTable> schemaTables = null;
						if (validateResult.getSchema() != null) {
							schemaTables = validateResult.getSchema().getTables();
						}

						ConverterUtil.schemaConvert(schemaTables, connection.getDatabase_type());

						update = getValidateResultUpdate(
								validateResult.getRetry(), validateResult.getNextRetry(), validateResult.getStatus(),
								validateResult.getDb_version(), schemaTables, validateResultDetails,
								validateResult.getDbFullVersion()
						);
						int schemaTablesSize = CollectionUtils.isNotEmpty(schemaTables) ? schemaTables.size() : 0;
						update.set(ConnectorConstant.LOAD_FIELDS, "loading")
								.set("loadCount", 0).set("tableCount", schemaTablesSize);
						long javaEndTs = System.currentTimeMillis();
						long javaSpend = javaEndTs - startTs;

						clientMongoOperator.update(updateQuery, update, ConnectorConstant.CONNECTION_COLLECTION);

						schema = validateResult.getSchema();

						long endTs = System.currentTimeMillis();
						long managementSpend = endTs - startTs - javaSpend;
						logger.info(TapLog.CON_LOG_0022.getMsg(), connection.getName(), validateResult.getStatus(),
								javaSpend, managementSpend, schemaTablesSize);
					} else if (tapInterface != null) {
						BaseConnectionValidateResult baseConnectionValidateResult = tapInterface.testConnections(connection);

						if (baseConnectionValidateResult != null) {

							String status = baseConnectionValidateResult.getStatus();
							validateResultDetails = baseConnectionValidateResult.getValidateResultDetails();
							if (BaseConnectionValidateResult.CONNECTION_STATUS_READY.equals(status)
									&& baseConnectionValidateResult != null
									&& baseConnectionValidateResult.getSchema() != null
									&& CollectionUtils.isNotEmpty(baseConnectionValidateResult.getSchema().getTables())) {
								schema = baseConnectionValidateResult.getSchema();
								ConverterUtil.schemaConvert(schema.getTables(), connection.getDatabase_type());
								String uuid = UUIDGenerator.uuid();
								schema.getTables().forEach(table -> table.setSchemaVersion(uuid));
							} else {
								schema = new Schema(new ArrayList<>());
							}

							update = getValidateResultUpdate(
									baseConnectionValidateResult.getRetry(), baseConnectionValidateResult.getNextRetry(), status,
									baseConnectionValidateResult.getDb_version(), schema.getTables(), validateResultDetails, null
							);
							update.set("loadFieldsStatus", "finished");
							long javaEndTs = System.currentTimeMillis();
							long javaSpend = javaEndTs - startTs;

							clientMongoOperator.update(updateQuery, update, ConnectorConstant.CONNECTION_COLLECTION);

							long endTs = System.currentTimeMillis();
							long managementSpend = endTs - startTs - javaSpend;
							logger.info(TapLog.CON_LOG_0022.getMsg(), connection.getName(), baseConnectionValidateResult.getStatus(),
									javaSpend, managementSpend, schema.getTables().size());
						}
					}

					if (schema != null && !schema.isIncludeFields()) {
						// need to load schema fields
						LoadSchemaRunner loadSchemaRunner = new LoadSchemaRunner(connection, clientMongoOperator, schema.getTables().size());
						loadSchemaThreadPool.submit(loadSchemaRunner);
					}
				} catch (Exception e) {
					logger.error("Cannot test connection {} {}", connection.getName(), e.getMessage(), e);
					BaseConnectionValidateResult baseConnectionValidateResult = new BaseConnectionValidateResult();
					baseConnectionValidateResult.setStatus(BaseConnectionValidateResult.CONNECTION_STATUS_INVALID);
					List<BaseConnectionValidateResultDetail> baseConnectionValidateResultDetails = new ArrayList<>();
					baseConnectionValidateResultDetails.add(new BaseConnectionValidateResultDetail() {{
						setShow_msg("Init test connections");
						setRequired(true);
						setFailedInfo("Runtime error: " + e.getMessage());
					}});
					baseConnectionValidateResult.setValidateResultDetails(baseConnectionValidateResultDetails);

					Update update = getValidateResultUpdate(
							0, 0L, BaseConnectionValidateResult.CONNECTION_STATUS_INVALID,
							null, null, baseConnectionValidateResultDetails, null
					);

					Query updateQuery = new Query(where("_id").is(connection.getId()));
					clientMongoOperator.update(updateQuery, update, ConnectorConstant.CONNECTION_COLLECTION);
				}

			}
		} else {
			if (logger.isDebugEnabled()) {
				logger.debug("Does not find any testing connection.");
			}
		}
	}

	private Update getValidateResultUpdate(int retry, Long nextRetry, String status, Integer db_version, List<RelateDataBaseTable> schemaTables, List validateResultDetails, String dbFullVersion) {
		Update update = new Update();

		update.set("response_body.retry", retry);
		if (nextRetry != null) {
			update.set("response_body.next_retry", nextRetry);
		}

		update.set("response_body.validate_details", validateResultDetails);
		update.set("status", status);

		if (CollectionUtils.isNotEmpty(schemaTables)) {
			update.set("schema.tables", schemaTables);
		}

		if (db_version != null) {
			update.set("db_version", db_version);
		}

		if (db_version != null) {
			update.set("db_version", db_version);
		}

		if (dbFullVersion != null) {
			update.set("dbFullVersion", dbFullVersion);
		}

		return update;
	}

	/**
	 * 定时获取最新的任务设置信息
	 */
	@Scheduled(fixedDelay = 30000L)
	public void jobListener() {
		Thread.currentThread().setName(String.format(ConnectorConstant.MERAGE_JOB_SETTING_THREAD, CONNECTOR, instanceNo.substring(instanceNo.length() - 6)));
		try {
			JOB_MAP.forEach((jobId, job) -> {
				Query query = new Query(where("_id").is(jobId));
				query.fields().exclude("editorData");
				List<Job> jobs = clientMongoOperator.find(query, ConnectorConstant.JOB_COLLECTION, Job.class);
				if (!CollectionUtils.isEmpty(jobs)) {
					Job dbJob = jobs.get(0);

					if (ConnectorConstant.RUNNING.equals(job.getStatus())) {
						job.setExecuteMode(dbJob.getExecuteMode());
						job.setTransformerConcurrency(dbJob.getTransformerConcurrency());

						Job cacheJob = messageDao.getCacheJob(job);
						if (cacheJob != null) {
							cacheJob.mergeWithoutRuntimeState(job);
						}
					}
				}
			});
		} catch (Exception e) {
			logger.error("Listing jobs' change failed {}", e.getMessage(), e);
		}
	}

	@Scheduled(fixedDelay = 60000L)
	public void loadSettings() {
		Thread.currentThread().setName(String.format(ConnectorConstant.LOAD_SETTINGS_THREAD, CONNECTOR, instanceNo.substring(instanceNo.length() - 6)));
		settingService.loadSettings();
    /*HazelcastInstance hazelcastInstance = HazelcastUtil.getInstance(configCenter);
    Optional.ofNullable(hazelcastInstance).ifPresent(instance -> ShareCdcUtil.initHazelcastPersistenceStorage(
      instance.getConfig(),
      settingService,
      clientMongoOperator));*/
	}

	/**
	 * worker heart beat
	 */
	@Scheduled(fixedDelay = 5000L)
	public void workerHeartBeat() {
		Thread.currentThread().setName(String.format(ConnectorConstant.WORKER_HEART_BEAT_THREAD, CONNECTOR, instanceNo.substring(instanceNo.length() - 6)));
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
			params.put("worker_type", ConnectorConstant.WORKER_TYPE_CONNECTOR);

			List<Worker> workers = pingClientMongoOperator.find(params, ConnectorConstant.WORKER_COLLECTION, Worker.class);
			Integer finalThreshold = threshold;
			checkAndExit(workers, isExit -> {
				Map<String, Object> value = new HashMap<>();
				value.put("total_thread", finalThreshold);
				value.put("process_id", instanceNo);
				value.put("user_id", userId);
				value.put("singletonLock", WorkerSingletonLock.getCurrentTag());
				value.put("version", version);
				value.put("hostname", hostname);
				value.put("cpuLoad", processCpuLoad);
				value.put("usedMemory", usedMemory);
				value.put("metricValues", this.metricManager.getValueMap());
				value.put("worker_type", ConnectorConstant.WORKER_TYPE_CONNECTOR);
				if (StringUtils.isNoneBlank(region, zone)) {
					Map<String, String> platformInfo = new HashMap<>();
					platformInfo.put("region", region);
					platformInfo.put("zone", zone);
					value.put("platformInfo", platformInfo);
				}

				final String version = (String) configCenter.getConfig("version");
				if (StringUtils.isNotBlank(version)) {
					value.put("version", version);
				}
				final String gitCommitId = (String) configCenter.getConfig("gitCommitId");
				if (StringUtils.isNotBlank(gitCommitId)) {
					value.put("gitCommitId", gitCommitId);
				}
				if (isExit) {
					// 更新ping_time为1，以便于其他端可以快速识别本实例已经停止，而不用等待超时
					value.put("ping_time", 1);
				}

				sendWorkerHeartbeat(
						value,
						v -> pingClientMongoOperator.insertOne(v, ConnectorConstant.WORKER_COLLECTION + "/health"));
			});
		} catch (Exception e) {
			logger.error("Worker heart beat failed {}.", e.getMessage(), e);
		}
	}

	public static void sendWorkerHeartbeat(Map<String, Object> value, Consumer<Map<String, Object>> executeWhenError) {
		try {
			PingDto pingDto = new PingDto();
			pingDto.setPingType(PingType.WORKER_PING);
			pingDto.setData(value);
			String pingId = UUIDGenerator.uuid();
			pingDto.setPingId(pingId);
			WebSocketEvent<PingDto> webSocketEvent = new WebSocketEvent<>();
			webSocketEvent.setType("ping");
			webSocketEvent.setData(pingDto);
			ManagementWebsocketHandler managementWebsocketHandler = BeanUtil.getBean(ManagementWebsocketHandler.class);
			if (null == managementWebsocketHandler) {
				return;
			}
			managementWebsocketHandler.sendMessage(new TextMessage(JSONUtil.obj2Json(webSocketEvent)));
			boolean handleResponse = PongHandler.handleResponse(
					pingId,
					cache -> {
						String pingResult = cache.get(PingDto.PING_RESULT).toString();
						if (PingDto.PingResult.FAIL.name().equals(pingResult)) {
							Object errorMessage = cache.getOrDefault(PingDto.ERR_MESSAGE, "unknown error");
							if (WorkerSingletonLock.STOP_AGENT.equals(errorMessage)) {
								RuntimeException stopError = new WorkerSingletonException("Stop by singleton lock");
								logger.info(stopError.getMessage());
								System.exit(0);
								throw stopError;
							}
							throw new RuntimeException("Failed to send worker heartbeat use websocket, will retry http, message: " + errorMessage);
						}
					});
			if (!handleResponse) {
				throw new RuntimeException("No response from worker heartbeat websocket, will retry http");
			}
		} catch (Exception e) {
			logger.warn(e.getMessage());
			executeWhenError.accept(value);
		}
	}

	/**
	 * 检查实例是否被停止或删除，目前只有dfs会检查
	 *
	 * @param workers
	 * @return true - 被停止或删除，实例将停止
	 * false: 没有被停止或删除，实例正常运行
	 */
	private void checkAndExit(List<Worker> workers, Consumer<Boolean> beforeExit) {
		if (CollectionUtils.isEmpty(workers)) {
			beforeExit.accept(false);
			return;
		}
		Worker worker = workers.get(0);
		if (!appType.isDfs()) {
			beforeExit.accept(false);
			return;
		}
		if (worker.isDeleted() || worker.isStopping()) {
			String exitInfo = "Flow engine will stop, cause: ";
			if (worker.isDeleted()) {
				exitInfo += "is deleted";
			} else if (worker.isStopping()) {
				exitInfo += "is stopped";
			}
			logger.info(exitInfo);
			beforeExit.accept(true);
			System.exit(1);
		} else {
			beforeExit.accept(false);
		}
	}

	/**
	 * 获取待运行的Job，一次只返回一个Job
	 *
	 * @return
	 * @throws InterruptedException
	 */
	public Job runningJob() throws InterruptedException {


		/*if (loadBalancing.balancing()) {
			logger.debug("Agent is balancing, do not handle any job.");
			return null;
		}*/

		// 缓存节点需要全局共享，因此需要再所有flow engine都运行缓存任务
		// 暂时只支持企业版，dfs、drs需要管理端调度配合

		if (appType.isDaas()) {
			List<String> jobIds = new ArrayList<>(JOB_MAP.keySet());
			Criteria need2RunCacheJob = where("_id").nin(jobIds)
					.and("stages.type").is(Stage.StageTypeEnum.MEM_CACHE.getType())
					.and("stages.cacheType").is("all")
					.and(ConnectorConstant.JOB_STATUS_FIELD).is(ConnectorConstant.RUNNING);
			final List<Job> jobs = clientMongoOperator.find(new Query(need2RunCacheJob), ConnectorConstant.JOB_COLLECTION, Job.class);
			if (CollectionUtils.isNotEmpty(jobs)) {
				return jobs.get(0);
			}
		}

		Query query = new Query(where("agentId").is(instanceNo)
				.and(ConnectorConstant.JOB_STATUS_FIELD).is(ConnectorConstant.SCHEDULED));
		Update update = new Update().set(ConnectorConstant.JOB_STATUS_FIELD, ConnectorConstant.RUNNING)
				.set("connector_ping_time", System.currentTimeMillis())
				.set("ping_time", System.currentTimeMillis())
				.set("process_id", instanceNo)
				.set("agentId", instanceNo)
				.set(ConnectorConstant.JOB_CONNECTOR_STOPPED_FIELD, false)
				.set(ConnectorConstant.JOB_TRANSFORMER_STOPPED_FIELD, false);

		return clientMongoOperator.findAndModify(query, update, Job.class, ConnectorConstant.JOB_COLLECTION, true);
	}

	private void setFileDefaultCharset(Connections connection) {

		Setting setting = settingService.getSetting("file.defaultCharset");
		if (setting != null && StringUtils.isBlank(connection.getFileDefaultCharset())) {
			connection.setFileDefaultCharset(setting.getValue());
		}
	}

	private void interruptJob(Job job) {
		String jobId = job.getId();
//		Map<String, Object> params = new HashMap<>();
//		params.put("_id", jobId);
		Query query = new Query(
				where("_id").is(jobId)
		);
		query.fields().include("_id");

//		Map<String, Object> update = new HashMap<>();
//		update.put(ConnectorConstant.JOB_STATUS_FIELD, job.getStatus());
		Update update = new Update();
		update.set(ConnectorConstant.JOB_STATUS_FIELD, job.getStatus());

		clientMongoOperator.findAndModify(query, update, Job.class, ConnectorConstant.JOB_COLLECTION);
	}

	private void stoppedJob(Job job) {
		if (job != null) {
			Map<String, Object> params = new HashMap<>();
			String jobId = job.getId();
			params.put("_id", jobId);
			Map<String, Object> update = new HashMap<>();

			if (job.getStatus().equals(ConnectorConstant.ERROR)) {
				update.put(ConnectorConstant.JOB_STATUS_FIELD, ConnectorConstant.ERROR);
			}
			update.put(ConnectorConstant.JOB_CONNECTOR_STOPPED_FIELD, true);
			update.put(ConnectorConstant.JOB_CONNECTOR_PING_TIME_FIELD, null);
			synchronized (messageDao) {
				UpdateResult updateResult = clientMongoOperator.updateAndParam(params, update, ConnectorConstant.JOB_COLLECTION);
				if (updateResult.getModifiedCount() > 0) {
					Query query = new Query(where("_id").is(jobId).and(ConnectorConstant.JOB_CONNECTOR_STOPPED_FIELD).is(true));
					List<Job> jobs = clientMongoOperator.find(query, ConnectorConstant.JOB_COLLECTION, Job.class);
					if (CollectionUtils.isNotEmpty(jobs)) {
						job = jobs.get(0);
					}

					logger.info(
							"Stop connector success, current job {} status {}, transformerStopped {}, connectorStopped {}",
							job.getName(),
							job.getStatus(),
							job.getTransformerStopped(),
							job.getConnectorStopped()
					);
				}
			}


			if (job != null) {
				if (job.getTransformerStopped() != null && job.getTransformerStopped() && !job.getStatus().equals(ConnectorConstant.ERROR)) {
					update.clear();
					update.put(ConnectorConstant.JOB_STATUS_FIELD, ConnectorConstant.PAUSED);

					UpdateResult updateResult = clientMongoOperator.updateAndParam(params, update, ConnectorConstant.JOB_COLLECTION);

					if (updateResult.getModifiedCount() > 0) {
						logger.info(
								"Stop job {} success, transformerStopped {}, connectorStopped {}",
								job.getName(),
								job.getTransformerStopped(),
								job.getConnectorStopped()
						);
					} else {
						logger.info(
								"Waiting job {}'s transformer stop, transformerStopped {}, connectorStopped {}",
								job.getName(),
								job.getTransformerStopped(),
								job.getConnectorStopped()
						);
					}
				}
			}
			// 缓存注销
			messageDao.destroyCache(job);
		}
	}

	private List<Job> getJobsNeedClearTriggerLog(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		int hour = calendar.get(Calendar.HOUR_OF_DAY);

		Criteria hourWhere = new Criteria().andOperator(
				where(Job.TRIGGER_START_HOUR_FIELD).exists(true),
				where(Job.TRIGGER_START_HOUR_FIELD).ne(null),
				where(Job.TRIGGER_START_HOUR_FIELD).is(Double.valueOf(String.valueOf(hour)))
		);

		// sync_type: cdc or initial sync+cdc
		Criteria syncWhere = new Criteria().orOperator(
				new Criteria(Job.SYNC_TYPE_FIELD).is(ConnectorConstant.SYNC_TYPE_CDC),
				new Criteria(Job.SYNC_TYPE_FIELD).is(ConnectorConstant.SYNC_TYPE_INITIAL_SYNC_CDC)
		);

		Query query = new Query(
				new Criteria().andOperator(
						currentUserCriteria(),
						new Criteria().andOperator(
								hourWhere,
								syncWhere
						)
				)
		);

		return clientMongoOperator.find(query, ConnectorConstant.JOB_COLLECTION, Job.class);
	}

	//	@Scheduled(fixedDelay = 10000L)
	private void stoppedJobIfNeed() {
		Thread.currentThread().setName(String.format(ConnectorConstant.STOP_JOB_THREAD, CONNECTOR, instanceNo.substring(instanceNo.length() - 6)));
		Map<String, Object> params = new HashMap<>();
		Map<String, Object> update = new HashMap<>();

		// build query
		// job status: stopping or force stopping
		// connectorStopped: true and connector_ping_time: null
		// transformerStopped: true and ping_time: null
		try {
			Criteria jobStatus = new Criteria().orOperator(where(ConnectorConstant.JOB_STATUS_FIELD).is(ConnectorConstant.STOPPING),
					where(ConnectorConstant.JOB_STATUS_FIELD).is(ConnectorConstant.FORCE_STOPPING));
			Criteria connectorStopped = new Criteria().andOperator(where(ConnectorConstant.JOB_CONNECTOR_STOPPED_FIELD).is(true),
					where(ConnectorConstant.JOB_CONNECTOR_PING_TIME_FIELD).is(null));
			Criteria transformerStopped = new Criteria().andOperator(where(ConnectorConstant.JOB_TRANSFORMER_STOPPED_FIELD).is(true),
					where(ConnectorConstant.JOB_PING_TIME_FIELD).is(null));

			Query query = new Query(new Criteria().andOperator(currentUserCriteria(), jobStatus, connectorStopped, transformerStopped));

			List<Job> jobs = clientMongoOperator.find(query, ConnectorConstant.JOB_COLLECTION, Job.class);

			Optional.ofNullable(jobs).ifPresent(jobList -> {
				for (Job job : jobList) {
					params.clear();
					update.clear();
					params.put("_id", job.getId());
					update.put(ConnectorConstant.JOB_STATUS_FIELD, ConnectorConstant.PAUSED);

					clientMongoOperator.findAndModify(params, update, Job.class, ConnectorConstant.JOB_COLLECTION);
				}
			});
		} catch (Exception e) {
			logger.error("Scan stop fail job error {}", e.getMessage(), e);
		}
	}

	//	@Scheduled(fixedDelay = 60000L)
	private void clearExpiredGridFSFile() {
		Thread.currentThread().setName(String.format(ConnectorConstant.CLEAR_GRIDFS_EXPIRED_FILE_THREAD, CONNECTOR, instanceNo.substring(instanceNo.length() - 6)));
		Query query = new Query(where("database_type").is(DatabaseTypeEnum.GRIDFS.getType())
				.and("connection_type").is(ConnectorConstant.CONNECTION_TYPE_TARGET)
				.and("status").is(ValidatorConstant.CONNECTION_STATUS_READY)
		);
		query.fields().exclude("schema").exclude("response_body");

		try {
			List<Connections> connections = clientMongoOperator.find(query, ConnectorConstant.CONNECTION_COLLECTION, Connections.class);
			if (CollectionUtils.isNotEmpty(connections)) {
				for (Connections connection : connections) {
					try {
						GridFSCleaner.ClearGridFSResult result = GridFSCleaner.startClean(connection);
						if (result.isSuccess()) {
							logger.info("Clear _id {} name {} gridfs connection files succeeded, {} ", connection.getId(), connection.getName(), result.toString());
						} else {
							logger.warn("Clear _id {} name {} gridfs connection files failed, reason {} errorMsg {}",
									connection.getId(),
									connection.getName(),
									result.getFailedReason(),
									result.getException() != null ? result.getException().getMessage() : ""
							);
						}
					} catch (Exception e) {
						logger.error("Clear _id {} name {} gridfs connection files happened unexpected error {} ", connection.getId(), connection.getName(), e.getMessage(), e);
					}
				}
			}
		} catch (Exception e) {
			logger.error("Clear expired gridfs failed failed {}", e.getMessage(), e);
		}
	}

	@Scheduled(fixedDelay = 1000 * 60 * 30)
	public void schemaAutoUpdate() {
		String userId = (String) configCenter.getConfig(ConfigurationCenter.USER_ID);
		String workerTimeout = settingService.getString("lastHeartbeat", "60");
		if (!AgentUtil.isFirstWorker(
				clientMongoOperator,
				instanceNo,
				appType.isCloud() ? userId : null,
				Double.valueOf(workerTimeout)
		)
		) {
			return;
		}
		Query query = new Query(where("schemaAutoUpdate").is(true));
		query.fields().exclude("schema");
		List<Connections> connections = clientMongoOperator.find(query, ConnectorConstant.CONNECTION_COLLECTION, Connections.class);
		if (CollectionUtils.isNotEmpty(connections)) {
			try {
				String schemaUpdateHour = settingService.getString("connection_schema_update_hour", "false");
				int connectionSchemaUpdateHour = -1;
				if (schemaUpdateHour.indexOf(":") > 0) {
					String[] hourSplit = schemaUpdateHour.split(":");
					connectionSchemaUpdateHour = Integer.valueOf(hourSplit[0]);
				}
				int connectionSchemaUpdateInterval = settingService.getInt("connection_schema_update_interval", -1);
				int hour = LocalTime.now().getHour();
				if (connectionSchemaUpdateHour < 0 || hour != connectionSchemaUpdateHour || connectionSchemaUpdateInterval < 0) {
					return;
				}
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
				String now = sdf.format(new Date());
				for (Connections connection : connections) {
					String lastTime = sdf.format(new Date(connection.getSchemaAutoUpdateLastTime()));
					long day = (sdf.parse(now).getTime() - sdf.parse(lastTime).getTime()) / (1000 * 60 * 60 * 24);
					if (connectionSchemaUpdateInterval <= day) {
						try {
							connection.decodeDatabasePassword();
							LoadSchemaRunner loadSchemaRunner = new LoadSchemaRunner(connection, clientMongoOperator, 0);
							schemaUpdatePool.submit(loadSchemaRunner);
							query = new Query(where("_id").is(connection.getId()));
							query.fields().exclude("schema");
							clientMongoOperator.update(query, new Update().set("schemaAutoUpdateLastTime", System.currentTimeMillis()), ConnectorConstant.CONNECTION_COLLECTION);
						} catch (Exception e) {
							logger.error("Load schema is failed,connection name: {},message: {}", connection.getName(), e.getMessage(), e);
						}
					}
				}
			} catch (Exception e) {
				logger.error("SchemaUpdate is failed,message: {}", e.getMessage(), e);
			}
		}
	}

	private List<Job> stoppingJob(boolean isForceStop) {


		Criteria stoppingWhere;
		Query query;
		if (isForceStop) {
			stoppingWhere = where(ConnectorConstant.JOB_STATUS_FIELD).is(ConnectorConstant.FORCE_STOPPING);
		} else {
			stoppingWhere = new Criteria().orOperator(where(ConnectorConstant.JOB_STATUS_FIELD).is(ConnectorConstant.STOPPING),
					where(ConnectorConstant.JOB_STATUS_FIELD).is(ConnectorConstant.ERROR));
		}
		Criteria connectorStopped = where(ConnectorConstant.JOB_CONNECTOR_STOPPED_FIELD).is(false);

		query = new Query(new Criteria().andOperator(currentUserCriteria(), stoppingWhere, connectorStopped));

		List<Job> jobs;
		try {
			jobs = clientMongoOperator.find(query, ConnectorConstant.JOB_COLLECTION, Job.class);
		} catch (Exception e) {
			failedCallApiStopAllJob();
			return null;
		}

		if (!isForceStop) {
			for (Map.Entry<String, Job> entry : JOB_MAP.entrySet()) {
				String jobId = entry.getKey();

				if (stopJobMap.containsKey(jobId)) {
					continue;
				}
//				Map<String, Object> params = new HashMap<>();

				Query existsQuery = new Query(where("_id").is(jobId));
				existsQuery.fields().include("status");
//				params.clear();
//				params.put("_id", jobId);

				try {
					List<Job> list = clientMongoOperator.find(existsQuery, ConnectorConstant.JOB_COLLECTION, Job.class);

					// 库中不存在停止job，请求TM失败保持任务
					if (CollectionUtils.isEmpty(list) || ConnectorConstant.PAUSED.equals(list.get(0).getStatus())) {
						jobs.add(entry.getValue());
					}
				} catch (Exception e) {
					logger.error("Check job {} exists in db failed {}", e.getMessage());
				}

				//jobs是不完全的，我需要把JOB_MAP（内存中）Stopping的，但中间库不是Stopping的记录添加进来；同时修改中间库任务状态
				Job job = entry.getValue();
				String jobStatus = job.getStatus();
				if (ConnectorConstant.STOPPING.equals(jobStatus)) {
					query.addCriteria(new Criteria().and(ConnectorConstant.JOB_STATUS_FIELD).is(ConnectorConstant.RUNNING));
//					params.put(ConnectorConstant.JOB_STATUS_FIELD, ConnectorConstant.RUNNING);
					jobs.add(job);
//					Map<String, Object> update = new HashMap<>();
//					update.put(ConnectorConstant.JOB_STATUS_FIELD, ConnectorConstant.STOPPING);
					Update update = new Update().set(ConnectorConstant.JOB_STATUS_FIELD, ConnectorConstant.STOPPING);
					clientMongoOperator.update(query, update, ConnectorConstant.JOB_COLLECTION);
				}

			}
		}

		if (CollectionUtils.isNotEmpty(jobs)) {
			Iterator<Job> iterator = jobs.iterator();
			while (iterator.hasNext()) {
				Job job = iterator.next();
				if (stopJobMap.containsKey(job.getId()) && isForceStop == stopJobMap.get(job.getId()).isForce()) {
					iterator.remove();
				}
			}
		}

		return jobs;
	}

	private void failedCallApiStopAllJob() {

		ERR_JOB_MAP.putAll(JOB_MAP);

	}

	private void startedJob(Job job) {
		if (job != null) {
			String jobId = job.getId();
			Map<String, Object> params = new HashMap<>();
			Map<String, Object> update = new HashMap<>();
			if (StringUtils.isNotBlank(jobId)) {
				params.put("_id", jobId);
				update.put(ConnectorConstant.JOB_CONNECTOR_STOPPED_FIELD, false);

				clientMongoOperator.updateAndParam(params, update, ConnectorConstant.JOB_COLLECTION);
			}
		}
	}

	private List<Job> findJobsWhenClearTriggerLog(String databaseType) {
		List<Job> jobs = null;
		if (StringUtils.isNotBlank(databaseType)) {
			Criteria dbType = where("sourceConnection.database_type").is(databaseType);

			jobs = clientMongoOperator.find(new Query(dbType), ConnectorConstant.JOB_COLLECTION, Job.class);
		}

		return jobs;
	}

	/**
	 * only build profile is cloud return current user criteria
	 *
	 * @return
	 */
	public Criteria currentUserCriteria() {
		Criteria criteria = new Criteria();
		String userId = (String) configCenter.getConfig(ConfigurationCenter.USER_ID);
		Setting buildProfile = settingService.getSetting("buildProfile");
		if (buildProfile != null) {
			String value = buildProfile.getValue();
			if (StringUtils.isBlank(value) || value.equals("CLOUD")) {
				criteria = where("user_id").is(userId);
			}
		}
		return criteria;
	}

	private void addHTTPAppender() {
		org.apache.logging.log4j.core.Logger rootLogger = (org.apache.logging.log4j.core.Logger) LogManager.getRootLogger();
		JsonLayout jsonLayout = JsonLayout.newBuilder().setProperties(true).build();

		LogUtil logUtil = new LogUtil(settingService);
		TapdataLog4jFilter filter = logUtil.buildFilter();
		BurstFilter burstFilter = logUtil.buildBurstFilter();

		CustomHttpAppender httpAppender = CustomHttpAppender.createAppender("httpAppender", null, jsonLayout, clientMongoOperator);
		httpAppender.addFilter(burstFilter);
		logger.info("Burst filter: {}", burstFilter.toString());
		httpAppender.addFilter(filter);
		logger.info("Tapdata filter: {}", filter.toString());
		JetExceptionFilter jetExceptionFilter = new JetExceptionFilter.TapLogBuilder().build();
		httpAppender.addFilter(jetExceptionFilter);
		rootLogger.addAppender(httpAppender);
		httpAppender.start();
	}

	private void removeMapIfNeed(String jobId) {
		if (StringUtils.isNotBlank(jobId)) {
			Optional.ofNullable(JOB_SET).ifPresent(jobSet -> jobSet.remove(jobId));
			Optional.ofNullable(JOB_MAP).ifPresent(jobMap -> jobMap.remove(jobId));
			Optional.ofNullable(JOB_STATS).ifPresent(jobStats -> jobStats.remove(jobId));
			Optional.ofNullable(JOB_THREADS).ifPresent(jobThreads -> jobThreads.remove(jobId));
		}
	}

	private void stopJob(Job job, boolean force, boolean internalStop, boolean isError) {
		if (job == null || StringUtils.isAnyBlank(job.getId())) {
			return;
		}

		if (stopJobMap.containsKey(job.getId())) {
			ConnectorStopJob connectorStopJob = stopJobMap.get(job.getId());
			if (force == connectorStopJob.isForce()) {
				return;
			}

			if (connectorStopJob.isError()) {
				return;
			}
		}

		String threadName = String.format(stopJobThreadName, job.getName(), job.getId());
		ConnectorStopJob connectorStopJob = new ConnectorStopJob(job, force, threadName, internalStop, isError);
		Future future = stopJobThreadPool.submit(connectorStopJob);
		connectorStopJob.setFuture(future);
		stopJobMap.put(job.getId(), connectorStopJob);
		if (internalStop) {
			try {
				messageDao.getStopJobs().put(job.getId());
			} catch (InterruptedException ignore) {

			}
		}
	}

	private void initVariable() {
		this.mongoURI = DEFAULT_TAPDATA_MONGO_URI;
		this.ssl = false;
		this.sslCA = "";
		this.sslPEM = "";
		this.mongodbConnParams = "";
		this.baseURLs = DEFAULT_BASE_URLS;
		this.accessCode = "";
		this.restRetryTime = 3;
		this.mode = "cluster";

		String isCloud = System.getenv("isCloud");
		if ("true".equals(isCloud)) {
			this.mongoURI = null;
		} else {
			String tapdataMongoUri = System.getenv("TAPDATA_MONGO_URI");
			if (StringUtils.isBlank(tapdataMongoUri)) {
				logger.info("TAPDATA_MONGO_URI env variable does not set, will use default {}", this.mongoURI);
			} else {
				this.mongoURI = tapdataMongoUri;
			}
		}

		String tapdataMongoConn = System.getenv("TAPDATA_MONGO_CONN");
		if (StringUtils.isEmpty(tapdataMongoConn)) {
			logger.info("TAPDATA_MONGO_CONN env variable does not set, will use default {}", this.mongodbConnParams);
		}

		String ssl = System.getenv("MONGO_SSL");
		if (StringUtils.isEmpty(ssl)) {
			logger.info("ssl env variable does not set, will use default {}", this.ssl);
		} else {
			this.ssl = Boolean.valueOf(ssl);
		}

		String sslCA = System.getenv("MONGO_SSL_CA");
		if (StringUtils.isNotBlank(sslCA)) {
			this.sslCA = sslCA;
		}
		String sslCertKey = System.getenv("MONGO_SSL_CERT_KEY");
		if (StringUtils.isNotBlank(sslCertKey)) {
			this.sslPEM = sslCertKey;
		}

		String cloud_accessCode = System.getenv("cloud_accessCode");
		if (StringUtils.isEmpty(cloud_accessCode)) {
			logger.info("cloud_accessCode env variable does not set, will use default \"{}\".", this.accessCode);
		} else {
			this.accessCode = cloud_accessCode;
		}

		String cloud_retryTime = System.getenv("cloud_retryTime");
		if (StringUtils.isEmpty(cloud_retryTime)) {
			logger.info("cloud_retryTime env variable does not set, will use default {}", this.restRetryTime);
		} else {
			if (!NumberUtils.isDigits(cloud_retryTime)) {
				logger.warn("Set invalid cloud retry time {}", cloud_retryTime);
				cloud_retryTime = "3";
			}
			this.restRetryTime = Integer.valueOf(cloud_retryTime);
		}

		String cloud_baseURLs = System.getenv("backend_url");
		if (StringUtils.isEmpty(cloud_baseURLs)) {
			logger.info("backend_url env variable does not set, will use default {}", this.baseURLs);
		} else {
			List<String> baseURLs = Arrays.asList(cloud_baseURLs.split(","));
			if (CloudSignUtil.isNeedSign()) {
				//需要走ak/sk的时候，手动替换掉http://test.cloud.tapdata.net/tm_fdsfdf/api/  tm的后缀，解决老用户升级不走aksk的问题
				List<String> newBaseURLs = baseURLs.stream().map(this::replaceBaseURL).collect(Collectors.toList());
				logger.info("replace baseURLs {} -> {}", baseURLs, newBaseURLs);
				baseURLs = newBaseURLs;
			}
			this.baseURLs = baseURLs;
		}

		String mode = System.getenv("mode");
		if (StringUtils.isEmpty(mode)) {
			logger.info("mode env variable does not set, will use default {}", this.mode);
		} else {
			this.mode = mode;
		}

		try {
			this.appType = AppType.init();
		} catch (Exception e) {
			logger.error("Please check app_type in env and try again, message: {}", e.getMessage(), e);
			System.exit(1);
			return;
		}

		this.tapdataWorkDir = System.getenv("TAPDATA_WORK_DIR");
		String processId = System.getenv("process_id");
		if (StringUtils.isBlank(processId)) {
			processId = AgentUtil.readAgentId(tapdataWorkDir);
			if (StringUtils.isBlank(processId)) {
				try {
					processId = AgentUtil.createAgentIdYaml(tapdataWorkDir);
				} catch (IOException e) {
					logger.error("Generate process id failed, will exit agent, please try to set process_id in env and try again, message: {}", e.getMessage(), e);
					System.exit(1);
				}
				logger.info("Generated process id in agent.yml, please don't modify it, process id: {}", processId);
			}
		}
		this.instanceNo = processId;
		ConfigurationCenter.processId = processId;

		this.jobTags = System.getenv("jobTags");
		if (appType.isDrs() && StringUtils.isNotBlank(jobTags)) {
			String[] jobTagsSplit = jobTags.split(",");
			if (jobTagsSplit.length < 2) {
				logger.error("Job tags is invalid: {}, after split by ',' length should be 2", jobTags);
				System.exit(1);
			}
			region = jobTagsSplit[0].trim();
			zone = jobTagsSplit[1].trim();
		}

		logger.info("\nInitialed variable\n - mongoURI: {}\n - ssl: {}\n - sslCA: {}\n - sslPEM: {}" +
						"\n - mongodbConnParams: {}\n - baseURLs: {}\n - accessCode: {}\n - restRetryTime: {}\n - mode: {}\n - app_type: {}" +
						"\n - process id: {}\n - job tags: {}\n - region: {}\n - zone: {}\n - worker dir: {}",
				MongodbUtil.maskUriPassword(this.mongoURI), this.ssl, this.sslCA, this.sslPEM, this.mongodbConnParams,
				this.baseURLs, this.accessCode, this.restRetryTime, this.mode, this.appType, this.instanceNo, this.jobTags, this.region, this.zone,
				this.tapdataWorkDir
		);
	}


	public String getInstanceNo() {
		return instanceNo;
	}

	public void setInstanceNo(String instanceNo) {
		this.instanceNo = instanceNo;
	}

	public void setClientMongoOpertor(ClientMongoOperator clientMongoOpertor) {
		this.clientMongoOperator = clientMongoOpertor;
	}

	public SettingService getSettingService() {
		return settingService;
	}

	public void setSettingService(SettingService settingService) {
		this.settingService = settingService;
	}

	public void setMongoURI(String mongoURI) {
		this.mongoURI = mongoURI;
	}

	public void setSsl(boolean ssl) {
		this.ssl = ssl;
	}

	public void setSslCA(String sslCA) {
		this.sslCA = sslCA;
	}

	public void setSslPEM(String sslPEM) {
		this.sslPEM = sslPEM;
	}

	/**
	 * 替换baseURL中/tm后的随机字符串
	 *
	 * @param baseURL
	 * @return
	 */
	private String replaceBaseURL(String baseURL) {
		String regex = "/tm";
		String[] split = baseURL.split(regex);
		if (split.length >= 2 && !split[1].startsWith("/")) {
			int beginIndex = baseURL.indexOf(regex);
			int endIndex = split[0].length() + split[1].indexOf("/") + 3;
			String repStr = baseURL.substring(beginIndex, endIndex);
			baseURL = baseURL.replace(repStr, regex);
		}
		return baseURL;
	}

	private class ConnectorStopJob implements Runnable {

		private Job job;
		private boolean force;
		private String threadName;
		private Future future;
		private boolean internalStop;
		private boolean isError;

		public ConnectorStopJob(Job job, boolean force, String threadName) {
			if (job == null || StringUtils.isAnyBlank(job.getId(), threadName)) {
				throw new IllegalArgumentException();
			}
			this.job = job;
			this.force = force;
			this.threadName = threadName;
		}

		public ConnectorStopJob(Job job, boolean force, String threadName, boolean internalStop, boolean isError) {
			if (job == null || StringUtils.isAnyBlank(job.getId(), threadName)) {
				throw new IllegalArgumentException();
			}
			this.job = job;
			this.force = force;
			this.threadName = threadName;
			this.internalStop = internalStop;
			this.isError = isError;
		}

		@Override
		public void run() {
			Thread.currentThread().setName(threadName);
			Log4jUtil.setThreadContext(job);
			String jobId = job.getId();

			if (JOB_MAP.containsKey(jobId)) {
				if (JOB_MAP.get(jobId).getStatus().equals(ConnectorConstant.RUNNING)) {
					JOB_MAP.get(jobId).setStatus(ConnectorConstant.STOPPING);
				}
			}

			Optional.ofNullable(JOB_THREADS.get(jobId)).ifPresent(connector -> {
				logger.info("Stopping connector runner, please wait");
//        try {
//          if (force) {
//            connector.forceStop();
//          } else {
//            // shutdown thread pool
//            connector.stop();
//          }
//
//        } catch (InterruptedException e) {
//          logger.error(TapLog.JOB_ERROR_0003.getMsg(), job.getName(), e.getMessage(), e);
//          if (JOB_THREADS.containsKey(jobId)) {
//            Optional.ofNullable(JOB_THREADS.get(jobId)).ifPresent(conn -> {
//              logger.warn(TapLog.W_CONN_LOG_0012.getMsg());
//              conn.forceStop();
//            });
//          }
//        }
			});

			logger.info(TapLog.JOB_LOG_0014.getMsg(), job.getName());
		}

		public Future getFuture() {
			return future;
		}

		public void setFuture(Future future) {
			this.future = future;
		}

		public Job getJob() {
			return job;
		}

		public boolean isForce() {
			return force;
		}

		public String getThreadName() {
			return threadName;
		}

		public boolean isInternalStop() {
			return internalStop;
		}

		public boolean isError() {
			return isError;
		}
	}
}
