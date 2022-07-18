package io.tapdata.manager;

/**
 * @author huangjq
 * @ClassName: ConnectorJobManager
 * @Description: Connector Job Manager
 * @date 2017/6/16 11:17
 * @since 1.0
 */
//public class ConnectorJobManager {
//
//  private static Logger logger = LogManager.getLogger(ConnectorJobManager.class);
//  private final static String THREAD_NAME_PREFIX = "Connector runner-%s-[%s]";
//  private static String threadName = "";
//
//  /**
//   * 初始化源端采集器
//   *
//   * @param job
//   * @param clientMongoOpertor
//   * @param connection
//   * @param messageQueue
//   * @param targetConn
//   * @param baseURLs
//   * @param accessCode
//   * @param dataRulesMap
//   * @param restRetryTime
//   * @param settingService
//   * @param userId
//   * @param roleId
//   * @param debugProcessor
//   * @return
//   * @throws Exception
//   */
//  public synchronized static Connector prepare(
//    Job job,
//    ClientMongoOperator clientMongoOpertor,
//    Connections connection,
//    LinkedBlockingQueue<List<MessageEntity>> messageQueue,
//    Connections targetConn,
//    List<String> baseURLs,
//    String accessCode,
//    Map<String, List<DataRules>> dataRulesMap,
//    int restRetryTime,
//    SettingService settingService,
//    String userId,
//    Integer roleId,
//    DebugProcessor debugProcessor,
//    ICacheService cacheService,
//    TapdataShareContext tapdataShareContext,
//    boolean isCloud,
//    MilestoneJobService milestoneJobService,
//    ConfigurationCenter configurationCenter
//  ) throws Exception {
//    Runnable runnable = null;
//    String type = connection.getDatabase_type();
//    DatabaseTypeEnum databaseType = DatabaseTypeEnum.fromString(type);
//    threadName = String.format(THREAD_NAME_PREFIX, job.getName(), job.getId());
//
//    // Cause API cannot correct pojo offset value, workaround for convert
//    new OffsetConvertUtil(job, connection).convert();
//
//    setSettingsValue(connection, settingService);
//
//    ConverterProvider converterProvider = null;
//    Class<?> converterByDatabaseType = ClassScanner.getConverterByDatabaseType(type);
//    if (converterByDatabaseType != null) {
//      try {
//        converterProvider = (ConverterProvider) converterByDatabaseType.newInstance();
//        if (converterProvider != null) {
//          converterProvider.init(new ConverterProvider.ConverterContext(connection, targetConn));
//        }
//      } catch (InstantiationException | IllegalAccessException e) {
//        logger.error("Init convert provider error, message: {}.", e.getMessage(), e);
//      }
//    }
//
//    List<JavaScriptFunctions> javaScriptFunctions = JobUtil.getJavaScriptFunctions(clientMongoOpertor);
//
//    initMappings(job, connection, targetConn);
//    Connector connector = new Connector(job, messageQueue, clientMongoOpertor, connection, converterProvider, cacheService, configurationCenter);
//    List<Runnable> runnables = connector.getRunnableList();
//
//    CdcEventHandler cdcEventHandler = new CdcEventHandler(
//      job, connection, targetConn, clientMongoOpertor
//    );
//
//    if (databaseType != null && !job.is_test_write()) {
//
//      List<Processor> processors = initProcessors(job, connection, targetConn, dataRulesMap, clientMongoOpertor);
//
//      // db timezone
//      String timezone = TimeZoneUtil.getZoneIdByDatabaseType(connection);
//      String sysTimezone = TimeZoneUtil.getSysZoneIdByDatabaseType(connection);
//      String sysDate = TimeZoneUtil.getDateByDatabaseType(connection, timezone);
//      try {
//        connection.setZoneId(ZoneId.of(timezone));
//        connection.setSysZoneId(ZoneId.of(sysTimezone));
//        connection.setDbCurrentTime(sysDate);
//      } catch (Exception e) {
//        logger.warn("Set {} time zone error: {}, use system default time zone: {}",
//          connection.getDatabase_type(), timezone, ZoneId.systemDefault());
//      }
//
//      // custom timezone
//      connection.initCustomTimeZone();
//      logger.info("Source connection time zone: " + connection.getCustomZoneId());
//
//      //target connect
//      String targetTimezone = TimeZoneUtil.getZoneIdByDatabaseType(targetConn);
//      String targetSysTimezone = TimeZoneUtil.getSysZoneIdByDatabaseType(targetConn);
//      String targetSysDate = TimeZoneUtil.getDateByDatabaseType(targetConn, targetTimezone);
//      try {
//        targetConn.setZoneId(ZoneId.of(targetTimezone));
//        targetConn.setSysZoneId(ZoneId.of(targetSysTimezone));
//        targetConn.setDbCurrentTime(targetSysDate);
//      } catch (Exception e) {
//        logger.warn("Set {} time zone error: {}, use system default time zone: {}",
//          targetConn.getDatabase_type(), targetTimezone, ZoneId.systemDefault());
//      }
//      targetConn.initCustomTimeZone();
//      logger.info("Target connection time zone: " + targetConn.getCustomZoneId());
//      processSourceDBSyncTime(job, timezone, sysDate, clientMongoOpertor);
//
//      try {
//        Class<?> sourceClazz = ClassScanner.getClazzByDatabaseType(databaseType.getType(), ClassScanner.SOURCE);
//        if (sourceClazz != null) {
//          Source source = (Source) sourceClazz.newInstance();
//          buildConnector(connector, source, targetConn, settingService, baseURLs, accessCode, restRetryTime, userId, roleId, javaScriptFunctions);
//        }
//      } catch (IllegalAccessException | InstantiationException e) {
//        logger.error(TapLog.CONN_ERROR_0027.getMsg(), e.getMessage(), e);
//        return null;
//      }
//
//      connector.setProcessors(processors);
//      connector.setDebugProcessor(debugProcessor);
//      connector.setCdcEventHandler(cdcEventHandler);
//      connector.setTapdataShareContext(tapdataShareContext);
//      connector.setMilestoneJobService(milestoneJobService);
//      connector.getTapdataShareContext().setInitialStats(job.getStats().getInitialStats());
//    } else if (job.is_test_write()) {
//      logger.info(TapLog.CON_LOG_0011.getMsg(), job.is_test_write(), job.getTestWrite().getRows(), job.getTestWrite().getCol_length());
//      runnable = createFakeDataEngine(job, messageQueue, connection, settingService, debugProcessor, targetConn, clientMongoOpertor, cdcEventHandler, tapdataShareContext, milestoneJobService, configurationCenter);
//    }
//
//    Optional.ofNullable(runnable).ifPresent(runner -> {
//      logger.info(TapLog.CON_LOG_0012.getMsg(), threadName);
//      runnables.add(runner);
//    });
//
//    return connector;
//  }
//
//  private static void buildConnector(Connector connector, Source source, Connections targetConn, SettingService settingService,
//                                     List<String> baseURLs, String accessCode, int restRetryTime, String userId, Integer roleId,
//                                     List<JavaScriptFunctions> javaScriptFunctions) {
//    if (source != null) {
//      connector.setSource(source);
//      connector.setTargetConn(targetConn);
//      connector.setSettingService(settingService);
//      connector.setThreadName(threadName);
//      connector.setBaseUrl(buildBaseURLs(baseURLs));
//      connector.setAccessCode(accessCode);
//      connector.setRestRetryTime(restRetryTime);
//      connector.setUserId(userId);
//      connector.setRoleId(roleId);
//      connector.setJavaScriptFunctions(javaScriptFunctions);
//    }
//  }
//
//  private static void setSettingsValue(Connections connection, SettingService settingService) {
//    Setting setting = settingService.getSetting("database.lobMaxSize");
//    if (setting != null) {
//      String value = setting.getValue();
//      if (NumberUtils.isDigits(value)) {
//        Integer lobMaxSize = null;
//        try {
//          lobMaxSize = Integer.valueOf(value);
//          connection.setLobMaxSize(lobMaxSize);
//        } catch (NumberFormatException e) {
//          logger.warn("Database lob size setting is illegal, use default size 8388608 (byte).");
//        }
//      }
//    }
//  }
//
//  public static void tableNoPKWarn(Connections connections, List<Mapping> mappings) {
//    // table without primary key(s) warning
//    String tableNames = connections.getTableNamesWithoutPK(mappings);
//    if (StringUtils.isNotBlank(tableNames)) {
//      logger.warn(TapLog.W_CONN_LOG_0009.getMsg(), tableNames);
//    }
//
//    if (CollectionUtils.isEmpty(mappings)) {
//      throw new SourceException("All tables do not have primary key, cannot execute cdc replicate.", true);
//    }
//  }
//
//  private static void initMappings(Job job, Connections connections, Connections targetConn) throws Exception {
//
//    if (CollectionUtils.isEmpty(job.getMappings())
//      && !StringUtils.equalsAny(targetConn.getDatabase_type(), DatabaseTypeEnum.GRIDFS.getType(), DatabaseTypeEnum.FILE.getType())) {
//      initClusterCloneMapping(job, connections);
//    }
//
//    if (!job.isEditDebug() && CollectionUtils.isEmpty(job.getStages())) {
//      OneManyUtil.addTableCloneWhenOneMany(job, false);
//    }
//  }
//
//  private static void initClusterCloneMapping(Job job, Connections connection) throws Exception {
//    // init cluster-clone mappings
//    String mappingTemplate = job.getMapping_template();
//    if (ConnectorConstant.MAPPING_TEMPLATE_CLUSTER_CLONE.equals(mappingTemplate)) {
//      generateClusterCloneMappings(job, connection);
//    }
//  }
//
//  public static void processSourceDBSyncTime(Job job, String timezone, String sysDate, ClientMongoOperator clientMongoOperator) throws ParseException {
//    Map<String, Object> deployment = job.getDeployment();
//    if (ConnectorConstant.SYNC_TYPE_CDC.equalsIgnoreCase(job.getSync_type())) {
//      TimeZoneUtil.processSyncTime(deployment, timezone, sysDate);
//      Query query = new Query(where("_id").is(job.getId()));
//      Update update = new Update().set("deployment", deployment);
//      clientMongoOperator.update(query, update, ConnectorConstant.JOB_COLLECTION);
//    }
//  }
//
//  public static List<Processor> initProcessors(Job job, Connections sourceConn, Connections targetConn, Map<String, List<DataRules>> dataRulesMap, ClientMongoOperator clientMongoOpertor) throws Exception {
//    List<Processor> processors = new ArrayList<>(0);
//    List<Mapping> mappings = job.getMappings();
//
//    if (CollectionUtils.isNotEmpty(mappings) && CollectionUtils.isEmpty(job.getStages())) {
//
//      for (Mapping mapping : mappings) {
//        if (StringUtils.isNotBlank(mapping.getCustom_sql())) {
//          processors.add(new CustomSqlProcessor(mappings, job));
//          break;
//        }
//      }
//
//      // init field processor
//      for (Mapping mapping : mappings) {
//        List<FieldProcess> filedsProcess = mapping.getFields_process();
//        if (CollectionUtils.isNotEmpty(filedsProcess)) {
//          processors.add(new FieldProcessor(job));
//          break;
//        }
//      }
//
//      List<JavaScriptFunctions> javaScriptFunctions = clientMongoOpertor.find(new Query(where("type").ne("system")).with(Sort.by(Sort.Order.asc("last_update"))), ConnectorConstant.JAVASCRIPT_FUNCTION_COLLECTION, JavaScriptFunctions.class);
//
//      // init script processor
//      for (Mapping mapping : mappings) {
//        String script = mapping.getScript();
//        if (StringUtils.isNotBlank(script) && StringUtils.isNotBlank(script.replaceAll(" ", ""))) {
//          processors.add(new ScriptProcessor(mappings, sourceConn, targetConn, javaScriptFunctions, clientMongoOpertor, job));
//          break;
//        }
//      }
//
//      // init data rules processor
//      if (MapUtils.isNotEmpty(dataRulesMap)) {
//        for (Mapping mapping : mappings) {
//          String toTable = mapping.getTo_table();
//          if (dataRulesMap.containsKey(toTable)) {
//            processors.add(new DataRulesProcessor(dataRulesMap, mappings));
//            break;
//          }
//        }
//      }
//    }
//
//    return processors;
//  }
//
//  private static Runnable createFakeDataEngine(Job job, LinkedBlockingQueue<List<MessageEntity>> messageQueue,
//                                               Connections sourceConn, SettingService settingService,
//                                               DebugProcessor debugProcessor, Connections targetConn,
//                                               ClientMongoOperator clientMongoOpertor, CdcEventHandler cdcEventHandler, TapdataShareContext tapdataShareContext,
//                                               MilestoneJobService milestoneJobService, ConfigurationCenter configurationCenter) {
//    String syncType = job.getSync_type();
//
//    if (ConnectorConstant.SYNC_TYPE_INITIAL_SYNC.equals(syncType) || ConnectorConstant.SYNC_TYPE_INITIAL_SYNC_CDC.equals(syncType)) {
//      Runnable runnable = () -> {
//        try {
//          Thread.currentThread().setName(threadName);
//          setThreadContext(job);
//
//          JdbcConnector jdbcConnector = JdbcConnector.init(job, clientMongoOpertor, sourceConn, targetConn, null, null, milestoneJobService, configurationCenter);
//
//          MemoryMessageConsumer messageConsumer = buildConsumer(jdbcConnector.getContext(), messageQueue, null, debugProcessor, settingService, cdcEventHandler, tapdataShareContext);
//
//          jdbcConnector.startConnect(messageConsumer::sourceDataHandler);
//
//        } catch (Exception e) {
//          logger.error(TapLog.TRAN_ERROR_0009.getMsg(), job.getId(), job.getName(), e.getMessage(), e);
//        } finally {
//          ThreadContext.clearAll();
//        }
//      };
//
//      return runnable;
//    } else {
//      return null;
//    }
//  }
//
//  public static void generateClusterCloneMappings(Job job, Connections connection) throws Exception {
//    if (connection != null) {
//
//      Map<String, List<RelateDataBaseTable>> schema = connection.getSchema();
//      if (schema != null) {
//        job.generateCloneMapping(schema, job.getStages());
//      }
//
//    }
//  }
//
//  private static void setThreadContext(Job job) {
//    ThreadContext.clearAll();
//
//    ThreadContext.put("userId", job.getUser_id());
//    ThreadContext.put("jobId", job.getId());
//    ThreadContext.put("jobName", job.getName());
//    ThreadContext.put("app", "connector");
//    if (StringUtils.isNotBlank(job.getDataFlowId())) {
//      ThreadContext.put(DebugConstant.SUB_DATAFLOW_ID, job.getDataFlowId());
//    }
//  }
//
//  private static String buildBaseURLs(List<String> baseURLs) {
//    String baseUrl = "";
//
//    if (CollectionUtils.isNotEmpty(baseURLs)) {
//      StringBuffer stringBuffer = new StringBuffer();
//      for (String baseURL : baseURLs) {
//        stringBuffer.append(baseURL).append(",");
//      }
//      baseUrl = stringBuffer.toString();
//    }
//
//    if (StringUtils.isNotBlank(baseUrl) && StringUtils.endsWith(baseUrl, ",")) {
//      baseUrl = StringUtils.removeEnd(baseUrl, ",");
//    }
//
//    return baseUrl;
//  }
//
//  public static MemoryMessageConsumer buildConsumer(ConnectorContext context,
//                                                    LinkedBlockingQueue<List<MessageEntity>> messageQueue,
//                                                    ConverterProvider converterProvider,
//                                                    DebugProcessor debugProcessor,
//                                                    SettingService settingService,
//                                                    CdcEventHandler cdcEventHandler,
//                                                    TapdataShareContext tapdataShareContext) {
//
//    MemoryMessageConsumer memoryMessageConsumer = new MemoryMessageConsumer(context,
//      messageQueue,
//      converterProvider,
//      debugProcessor,
//      settingService,
//      cdcEventHandler,
//      tapdataShareContext);
//
//    return memoryMessageConsumer;
//  }
//}
