package io.tapdata.manager;

/**
 * @author huangjq
 * @ClassName: JobManager
 * @Description: TODO
 * @date 2017/6/16 11:17
 * @since 1.0
 */
//public class TransformerJobManager {
//
//  private static Logger logger = LogManager.getLogger(TransformerJobManager.class);
//
//  private final static String THREAD_NAME_PREFIX = "Transformer runner-%s-[%s]";
//
//  private final static String TARGET_EXTENT_CLASS_NAME = "io.tapdata.TargetExtend";
//
//  private static String threadName;
//
//  public static Transformer prepare(Job job, Connections connection,
//                                    Connections targetConn,
//                                    LinkedBlockingQueue<List<MessageEntity>> messageQueue,
//                                    SettingService settingService,
//                                    ClientMongoOperator clientMongoOperator,
//                                    Map<String, List<DataRules>> dataRulesMap,
//                                    DebugProcessor debugProcessor, ICacheService cacheService, TapdataShareContext tapdataShareContext,
//                                    boolean isCloud, MilestoneJobService milestoneJobService) throws Exception {
//
//    logger.info(TapLog.TRAN_LOG_0032.getMsg());
//    Transformer transformer;
//    threadName = String.format(THREAD_NAME_PREFIX, job.getName(), job.getId());
//
//    Thread.currentThread().setName(threadName);
//
//    new OffsetConvertUtil(job, connection).convert();
//
//    String timezone = TimeZoneUtil.getZoneIdByDatabaseType(targetConn);
//    if (StringUtils.isNotBlank(timezone)) {
//      try {
//        targetConn.setZoneId(ZoneId.of(timezone));
//      } catch (Exception e) {
//        logger.warn("Set {} time zone error: {}, use system default time zone: {}",
//          targetConn.getDatabase_type(), timezone, ZoneId.systemDefault());
//      }
//    }
//
//    connection.initCustomTimeZone();
//    targetConn.initCustomTimeZone();
//    logger.info("Target connection time zone: " + targetConn.getCustomZoneId());
//
//    String targetDatabaseType = targetConn.getDatabase_type();
//    DatabaseTypeEnum targetDatabaseTypeEnum = DatabaseTypeEnum.fromString(targetDatabaseType);
//    String sourceDatabaseType = connection.getDatabase_type();
//    DatabaseTypeEnum sourceDatabaseTypeEnum = DatabaseTypeEnum.fromString(sourceDatabaseType);
//
//    ConverterProvider converterProvider = ConverterUtil.buildConverterProvider(
//      connection,
//      targetConn,
//      settingService,
//      targetDatabaseType
//    );
//
//    List<JavaScriptFunctions> javaScriptFunctions = JobUtil.getJavaScriptFunctions(clientMongoOperator);
//
//    TransformerContext transformerContext = new TransformerContext(job, connection,
//      targetConn, messageQueue, settingService,
//      threadName, converterProvider, debugProcessor, javaScriptFunctions,
//      cacheService, dataRulesMap, clientMongoOperator, tapdataShareContext, isCloud, milestoneJobService);
//    transformerContext.setTargetTypeMappings(getTypeMappings(targetConn, clientMongoOperator));
//
//    if (!transformerContext.isRunning()) {
//      return null;
//    }
//
//    logger.info("Initialize target connector");
//    if (defaultTransformer(sourceDatabaseTypeEnum, targetDatabaseTypeEnum)) {
//      transformer = initDefaultTransformer(transformerContext);
//    } else {
//      transformer = initSpecialTransformer(transformerContext);
//    }
//
//    if (transformer == null) {
//      throw new TargetException(true, "Cannot supported this database type " + targetDatabaseTypeEnum + " for target");
//    }
//
//    EmbeddedArrayUtil.cleanCacheCollection(transformerContext);
//    return transformer;
//  }
//
//  public static Future<?> startTransform(Transformer transformer, ExecutorService executorService) {
//    CountDownLatch countDownLatch = new CountDownLatch(1);
//    new Thread(() -> {
//      Thread.currentThread().setName("Init-Target-Runner-" + transformer.getContext().getJob().getName());
//      Log4jUtil.setThreadContext(transformer.getContext().getJob());
//      try {
//        if (transformer.getContext().getJob().needInitTargetDB()) {
//          try {
//            initTargetDB(transformer);
//          } catch (Exception e) {
//            throw new TargetException(true, e.getMessage(), e);
//          }
//        }
//
//        if (!transformer.getContext().isRunning()) {
//          return;
//        }
//
//        try {
//          List<Object> objectsFromTransformer = getObjectsFromTransfomer(transformer);
//          for (Object obj : objectsFromTransformer) {
//            ReflectUtil.invokeInterfaceMethod(obj, TARGET_EXTENT_CLASS_NAME, "afterCreateTargetTable");
//          }
//        } catch (InvocationTargetException e) {
//          throw new TargetException(true, e.getTargetException().getMessage(), e.getTargetException());
//        } catch (IllegalAccessException | RuntimeException e) {
//          throw new TargetException(true, e.getMessage(), e);
//        }
//      } catch (TargetException e) {
//        transformer.getContext().getJob().jobError(e, true, SyncStageEnum.UNKNOWN.getSyncStage(), logger, WorkerTypeEnum.TRANSFORMER.getType(),
//          e.getMessage(), null);
//      } finally {
//        countDownLatch.countDown();
//      }
//    }).start();
//    while (transformer.getContext().isRunning()) {
//      try {
//        if (countDownLatch.await(3, TimeUnit.SECONDS)) {
//          break;
//        }
//      } catch (InterruptedException e) {
//        break;
//      }
//    }
//    return executorService.submit(transformer);
//  }
//
//  public static List<TypeMapping> getTypeMappings(Connections targetConn, ClientMongoOperator clientMongoOperator) {
//    Query query = new Query(Criteria.where("databaseType").is(targetConn.getDatabase_type()));
//    List<TypeMapping> targetTypeMappings = clientMongoOperator.find(query, ConnectorConstant.TYPE_MAPPINGS_COLLECTION, TypeMapping.class);
//    return targetTypeMappings;
//  }
//
//  private static void initTargetDB(Transformer transformer) throws Exception {
//    JobCustomerLogger customerLogger = transformer.getContext().getJob().getJobCustomerLogger();
//    String targetName = transformer.getContext().getJobTargetConn().getName();
//    // auto create target table by mappings
//    List<String> tables = new ArrayList<>();
//    for (Mapping mapping : transformer.getContext().getJob().getMappings()) {
//      tables.add(mapping.getTo_table());
//    }
//    customerLogger.info(CustomerLogMessagesEnum.AGENT_CREATE_TABLES_STARTED, ImmutableMap.of(
//      "targetName", targetName,
//      "tables", StringUtils.joinWith(", ", tables)
//    ));
//    // use the old version of creating tables
//    DatabaseTypeEnum databaseTypeEnum = DatabaseTypeEnum.fromString(transformer.getContext().getJobTargetConn().getDatabase_type());
//    if (!"v2".equalsIgnoreCase(transformer.getContext().getJob().getTransformModelVersion())
//      && TransformModelVersion.isSupportV1(databaseTypeEnum)) {
//      createTargetTableByMapping(transformer);
//    } else {
//      createTargetTableByMappingV2(transformer);
//    }
//    customerLogger.info(CustomerLogMessagesEnum.AGENT_CREATE_TABLES_COMPLETED, ImmutableMap.of(
//      "targetName", targetName
//    ));
//
//    if (transformer.getContext().isRunning() && transformer.getContext().getJob().needInitial()) {
//      try {
//        clearTargetTables(transformer);
//      } catch (Exception e) {
//        transformer.handleTargetLibException(e);
//        // should throw the exception after adding the customer log
//        throw e;
//      }
//    }
//
//    if (transformer.getContext().isRunning()) {
//      // create target indices
//      ImmutableMap<String, Object> params = ImmutableMap.of(
//        "targetName", targetName
//      );
//      customerLogger.info(CustomerLogMessagesEnum.AGENT_CREATE_INDEXES_STARTED, params);
//      createTargetIndicesForSync(transformer);
//      customerLogger.info(CustomerLogMessagesEnum.AGENT_CREATE_INDEXES_COMPLETED, params);
//    }
//
//    transformer.getContext().setInsertByFirstMapping();
//  }
//
//  /**
//   * auto create target table by mappings
//   *
//   * @param transformer
//   */
//  private static void createTargetTableByMapping(Transformer transformer) {
//    TransformerContext context = transformer.getContext();
//    MilestoneService milestoneService = context.getMilestoneService();
//    try {
//      Object supportCreateTargetTable = ReflectUtil.invokeInterfaceMethod(getObjectFromTransfomer(transformer), TARGET_EXTENT_CLASS_NAME, "supportCreateTargetTable");
//      // Milestone-CREATE_TARGET_TABLE-RUNNING
//      MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.CREATE_TARGET_TABLE, MilestoneStatus.RUNNING);
//      if (Boolean.valueOf(supportCreateTargetTable + "")) {
//        createTargetTable(transformer);
//      } else {
//        // 需要建表的目标库类型，没有实现自动建表，打印告警信息
//        if (DatabaseTypeEnum.fromString(transformer.getContext().getJobTargetConn().getDatabase_type()).isNeedCreateTargetTable()) {
//          logger.warn("Database {} sync to {}, auto create table not supported. Please create target table manually",
//            transformer.getContext().getJobSourceConn().getDatabase_type(),
//            transformer.getContext().getJobTargetConn().getDatabase_type());
//        }
//      }
//      // Milestone-CREATE_TARGET_TABLE-FINISH
//      MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.CREATE_TARGET_TABLE, MilestoneStatus.FINISH);
//    } catch (Exception e) {
//      String errMsg = String.format("Automatically create target table failed, job name: %s, err: %s, stacks: %s",
//        transformer.getContext().getJob().getName(), e.getMessage(), Log4jUtil.getStackString(e));
//
//      // Milestone-CREATE_TARGET_TABLE-ERROR
//      MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.CREATE_TARGET_TABLE, MilestoneStatus.ERROR, errMsg);
//
//      transformer.getContext().getJob().jobError(e, true, ConnectorConstant.SYNC_TYPE_INITIAL_SYNC, logger, ConnectorConstant.WORKER_TYPE_TRANSFORMER, errMsg, null);
//    }
//  }
//
//  private static void createTargetTableByMappingV2(Transformer transformer) {
//    logger.info("Start auto create target table(s)");
//    TransformerContext context = transformer.getContext();
//    MilestoneService milestoneService = context.getMilestoneService();
//
//    try {
//      Class<?> ddlMakerClazz;
//      try {
//        ddlMakerClazz = DDLProducer.getDDLMaker(DatabaseTypeEnum.fromString(context.getJobTargetConn().getDatabase_type()));
//      } catch (Exception e) {
//        String msg = "Find ddl maker failed, database type: " + context.getJobTargetConn().getDatabase_type() + ", cause: " + e.getMessage();
//        throw new Exception(msg, e);
//      }
//
//      DatabaseTypeEnum sourceDatabaseType = DatabaseTypeEnum.fromString(transformer.getContext().getJobSourceConn().getDatabase_type());
//      Class<TypeMappingProvider> sourceTypeMappingClazz = TypeMappingUtil.getJobEngineTypeMappingClazz(sourceDatabaseType);
//      DatabaseTypeEnum targetDatabaseType = DatabaseTypeEnum.fromString(transformer.getContext().getJobTargetConn().getDatabase_type());
//      Class<TypeMappingProvider> targetTypeMappingClazz = TypeMappingUtil.getJobEngineTypeMappingClazz(targetDatabaseType);
//
//      if (ddlMakerClazz != null && sourceTypeMappingClazz != null && targetTypeMappingClazz != null && transformer instanceof DefaultTransformer) {
//        // Milestone-CREATE_TARGET_TABLE-RUNNING
//        MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.CREATE_TARGET_TABLE, MilestoneStatus.RUNNING);
//        DDLMaker<?> ddlMaker = (DDLMaker<?>) ddlMakerClazz.newInstance();
//        createTargetTableV2((DefaultTransformer) transformer, ddlMaker);
//        // Milestone-CREATE_TARGET_TABLE-FINISH
//        MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.CREATE_TARGET_TABLE, MilestoneStatus.FINISH);
//      } else {
//        // Try to use the old version of automatic table creation
//        createTargetTableByMapping(transformer);
//      }
//    } catch (Exception e) {
//      String msg = "Failed to automatically create target table. Will stop job, please try to restart the task after creating the table manually, cause: "
//        + e.getMessage() + "\n  " + Log4jUtil.getStackString(e);
//      // Milestone-CREATE_TARGET_TABLE-ERROR
//      MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.CREATE_TARGET_TABLE, MilestoneStatus.ERROR, msg);
//      transformer.getContext().getJob().jobError(e, true, ConnectorConstant.SYNC_TYPE_INITIAL_SYNC, logger, ConnectorConstant.WORKER_TYPE_TRANSFORMER, msg, null);
//    }
//  }
//
//  private static void createTargetTable(Transformer transformer) {
//    Job job = transformer.getContext().getJob();
//    Connections jobTargetConn = transformer.getContext().getJobTargetConn();
//    Map<String, List<RelateDataBaseTable>> schema = jobTargetConn.getSchema();
//    if (MapUtils.isEmpty(schema)
//      || !schema.containsKey("tables")
//      || CollectionUtils.isEmpty(schema.get("tables"))) {
//      throw new TargetException(true, "There is no schema information for the target connection");
//    }
//
//    if (CollectionUtils.isEmpty(job.getMappings())) {
//      return;
//    }
//
//    Map<String, RelateDataBaseTable> schemaByTable = new HashMap<>();
//    schema.get("tables").forEach(table -> {
//      if (table == null) {
//        return;
//      }
//      schemaByTable.put(table.getTable_name(), table);
//    });
//
//    List<Mapping> mappings = job.getMappings();
//
//    if (job.isOnlyInitialAddMapping()) {
//      mappings = job.getAddInitialMapping();
//    }
//
////    mappings = mappings.stream().filter(mapping -> mapping.getRelationship().equals(ConnectorConstant.RELATIONSHIP_ONE_ONE)).collect(Collectors.toList());
//    ExecutorUtil executorUtil = new ExecutorUtil();
//    executorUtil.queueMultithreading(mappings, null,
//      mapping -> {
//        try {
//          String toTable = mapping.getTo_table();
//          if (!schemaByTable.containsKey(toTable)) {
//            throw new TargetException(true, "Target connection " + jobTargetConn.getName() + " does not contain " + toTable + "'s schema.");
//          }
//
//          RelateDataBaseTable relateDataBaseTable = schemaByTable.get(toTable);
//          relateDataBaseTable.addSourceType(transformer.getContext().getJobSourceConn().getDatabase_type()); // v1 推演： mssql 2 mssql 建表不创建主键的依据
//          ConverterProvider converterProvider = transformer.getContext().getConverterProvider();
//          for (RelateDatabaseField field : relateDataBaseTable.getFields()) {
//            converterProvider.javaTypeConverter(field);
//          }
//
//          try {
//            ReflectUtil.invokeInterfaceMethod(getObjectFromTransfomer(transformer),
//              TARGET_EXTENT_CLASS_NAME,
//              "createTargetTableBySchema", relateDataBaseTable);
//          } catch (InvocationTargetException e) {
//            throw new TargetException(true, e.getTargetException().getMessage() + "; stacks:\n  " + Log4jUtil.getStackString(e.getTargetException()), e.getTargetException());
//          } catch (IllegalAccessException e) {
//            throw new TargetException(true, e.getMessage(), e);
//          }
//        } catch (Exception e) {
//          if (transformer.getContext().isRunning()) {
//            transformer.getContext().getJob().jobError(e, true, SyncStageEnum.SNAPSHOT.getSyncStage(), logger, ConnectorConstant.WORKER_TYPE_TRANSFORMER,
//              e.getMessage(), null);
//          } else {
//            throw new ExecutorUtil.InterruptExecutorException();
//          }
//        }
//      }, "Auto create target table", transformer.getContext(), transformerContext -> !transformerContext.isRunning(), job);
//  }
//
//  private static void createTargetTableV2(DefaultTransformer transformer, DDLMaker<?> ddlMaker) throws Exception {
//    if (transformer == null) {
//      throw new NullPointerException("Transformer runner cannot be null");
//    }
//    Connections jobTargetConn = transformer.getContext().getJobTargetConn();
//    if (jobTargetConn == null) {
//      throw new NullPointerException("Target connection cannot be null");
//    }
//    Map<String, List<RelateDataBaseTable>> schema = jobTargetConn.getSchema();
//    Job job = transformer.getContext().getJob();
//    List<RelateDataBaseTable> tables = schema.get("tables");
//    List<Mapping> mappings = job.getMappings();
//
//    if (job.isOnlyInitialAddMapping()) {
//      mappings = job.getAddInitialMapping();
//    }
//
//    mappings = mappings.stream().filter(
//      mapping ->
//        mapping.getRelationship().equals(ConnectorConstant.RELATIONSHIP_ONE_ONE) ||
//          mapping.getRelationship().equals(ConnectorConstant.RELATIONSHIP_APPEND)
//    ).collect(Collectors.toList());
//    ExecutorUtil executorUtil = new ExecutorUtil();
//    executorUtil.queueMultithreading(job.getTransformerConcurrency(), mappings, null,
//      mapping -> {
//        try {
//          String toTable = mapping.getTo_table();
//          RelateDataBaseTable table = ((SchemaList<String, RelateDataBaseTable>) tables).get(toTable);
//
//          if (table == null) {
//            throw new Exception("Target connection " + jobTargetConn.getName() + " does not contain " + toTable + "'s schema.");
//          }
//
//          Target target = transformer.getTargets().get(0);
//          if (target instanceof JdbcTarget) {
//            jobTargetConn.setDbFullVersion(JdbcUtil.getDbFullVersion(jobTargetConn));
//          }
//          Object createTable = ddlMaker.createTable(jobTargetConn, table);
//          try {
//            target.createTable(createTable, toTable);
//          } catch (UnsupportedOperationException e) {
//            // Try to use the old version of automatic table creation
//            logger.warn(e.getMessage() + ", will retry; stacks:\n  " + Log4jUtil.getStackString(e));
//            createTargetTable(transformer);
//          }
//        } catch (Exception e) {
//          if (transformer.getContext().isRunning()) {
//            transformer.getContext().getJob().jobError(e, true, SyncStageEnum.SNAPSHOT.getSyncStage(), logger, ConnectorConstant.WORKER_TYPE_TRANSFORMER,
//              e.getMessage(), null);
//          } else {
//            throw new ExecutorUtil.InterruptExecutorException();
//          }
//        }
//      }, "Auto create target table", transformer.getContext(), transformerContext -> !transformerContext.isRunning(), job);
//  }
//
//  /**
//   * Clear target tables
//   * When job is running initial sync, job setting drop table is ture
//   *
//   * @param transformer
//   */
//  private static void clearTargetTables(Transformer transformer) {
//    logger.info("Start auto clear target table(s)");
//    MilestoneService milestoneService = transformer.getContext().getMilestoneService();
//    // Milestone-CLEAR_TARGET_DATA-RUNNING
//    MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.CLEAR_TARGET_DATA, MilestoneStatus.RUNNING);
//
//    if (transformer.getContext().needCleanTarget() && transformer.getContext().isRunning()) {
//      if (transformer != null) {
//        try {
//          Job job = transformer.getContext().getJob();
//          JobCustomerLogger customerLogger = job.getJobCustomerLogger();
//          List<Mapping> mappings = job.getMappings();
//          if (job.isOnlyInitialAddMapping()) {
//            mappings = job.getAddInitialMapping();
//          }
//          String toTableStr = mappings.stream().limit(10).map(Mapping::getTo_table).distinct().collect(Collectors.joining(", "));
//          if (mappings.size() > 10) {
//            toTableStr += "...";
//          }
//          String op = "truncate table(s)";
//          String databaseType = transformer.getContext().getJobTargetConn().getDatabase_type();
//          if (StringUtils.equalsAnyIgnoreCase(databaseType, DatabaseTypeEnum.MONGODB.getType(), DatabaseTypeEnum.ALIYUN_MONGODB.getType())) {
//            op = "drop collection(s)";
//          } else if (DatabaseTypeEnum.ELASTICSEARCH.getType().equals(databaseType)) {
//            op = "delete index(es)";
//          }
//
//          Object instance = getObjectFromTransfomer(transformer);
//          customerLogger.info(CustomerLogMessagesEnum.AGENT_CLEAR_DATA_STARTED, ImmutableMap.of(
//            "targetName", transformer.getContext().getJobTargetConn().getName(),
//            "tables", toTableStr
//          ));
//          try {
//            ReflectUtil.invokeInterfaceMethod(instance,
//              TARGET_EXTENT_CLASS_NAME,
//              "deleteTargetTables");
//          } catch (InvocationTargetException e) {
//            throw e;
//          }
//          customerLogger.info(CustomerLogMessagesEnum.AGENT_CLEAR_DATA_COMPLETED, ImmutableMap.of(
//            "targetName", transformer.getContext().getJobTargetConn().getName()
//          ));
//
//          if (StringUtils.isNotBlank(toTableStr)) {
//            logger.info("Finished {} in target {} database: {}", op, databaseType, toTableStr);
//          }
//
//          // Milestone-CLEAR_TARGET_DATA-FINISH
//          MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.CLEAR_TARGET_DATA, MilestoneStatus.FINISH);
//        } catch (Throwable e) {
//          String errMsg = String.format("Automatically clear target data failed, err: %s, stacks: %s", StringUtils.isBlank(e.getMessage()) ? e.getCause().getMessage() : e.getMessage(),
//            Log4jUtil.getStackString(e));
//
//          if (transformer.getContext().getJob().jobError(e, false, ConnectorConstant.SYNC_TYPE_INITIAL_SYNC, logger, ConnectorConstant.WORKER_TYPE_TRANSFORMER, errMsg, null)) {
//            // Milestone-CLEAR_TARGET_DATA-FINISH
//            MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.CLEAR_TARGET_DATA, MilestoneStatus.FINISH);
//          } else {
//            // Milestone-CLEAR_TARGET_DATA-ERROR
//            MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.CLEAR_TARGET_DATA, MilestoneStatus.ERROR, errMsg);
//          }
//        }
//      }
//    } else {
//      Job job = transformer.getContext().getJob();
//
//      logger.info("Found no need to clear target data, offset: " + job.getOffset() + ", sync type: " + job.getSync_type() + ", drop table: " + job.getDrop_target() + ", keep schema: " + job.getKeepSchema());
//      // Milestone-CLEAR_TARGET_DATA-FINISH
//      MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.CLEAR_TARGET_DATA, MilestoneStatus.FINISH);
//    }
//  }
//
//  /**
//   * Auto create indices on target database
//   *
//   * @param transformer
//   * @throws IllegalAccessException
//   */
//  private static void createTargetIndicesForSync(Transformer transformer) throws Exception {
//    if (
//      transformer == null
//        || !transformer.getContext().isRunning()
//        || !transformer.getContext().getJob().getNeedToCreateIndex()
//        || transformer.getContext().getJob().is_test_write()
//        || transformer.getContext().getJob().is_null_write()
//    ) {
//      return;
//    }
//
//    MilestoneService milestoneService = transformer.getContext().getMilestoneService();
//    try {
//      logger.info("Start auto create target table's index");
//      // Milestone-CREATE_TARGET_INDEX-RUNNING
//      MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.CREATE_TARGET_INDEX, MilestoneStatus.RUNNING);
//
//      Object instance = getObjectFromTransfomer(transformer);
//      Job job = transformer.getContext().getJob();
//      List<Mapping> mappings = job.getMappings();
//
//      if (mappings == null
//        || instance == null) {
//        return;
//      }
//
//      if (job.isOnlyInitialAddMapping()) {
//        mappings = job.getAddInitialMapping();
//      }
//
//      ExecutorUtil executorUtil = new ExecutorUtil();
//      executorUtil.queueMultithreading(job.getTransformerConcurrency(), mappings,
//        Objects::nonNull,
//        mapping -> {
//          try {
//            String toTable = mapping.getTo_table();
//            String relationship = mapping.getRelationship();
//            List<Map<String, String>> joinCondition = mapping.getJoin_condition();
//            List<Map<String, String>> matchCondition = mapping.getMatch_condition();
//
//            if (ConnectorConstant.RELATIONSHIP_ONE_ONE.equals(relationship)
//              && CollectionUtils.isNotEmpty(joinCondition) && !mapping.getNoPrimaryKey()) {
//
//              createIndex(transformer, toTable, joinCondition);
//
//            } else if (ConnectorConstant.RELATIONSHIP_ONE_MANY.equals(relationship)
//              && CollectionUtils.isNotEmpty(joinCondition)) {
//
//              createIndex(transformer, toTable, joinCondition);
//
//            } else if (ConnectorConstant.RELATIONSHIP_MANY_ONE.equals(relationship)
//              && CollectionUtils.isNotEmpty(joinCondition)
//              && CollectionUtils.isNotEmpty(matchCondition)) {
//
//              joinCondition.addAll(matchCondition);
//              createIndex(transformer, toTable, joinCondition);
//
//            }
//
//            if (transformer.getContext().getJobSourceConn().getDatabase_type().equals(DatabaseTypeEnum.LOG_COLLECT.getType())) {
//              // create log collect index
//              List<Map<String, String>> timestampCondition = buildIndexCondition(new HashSet<String>() {{
//                add("timestamp");
//              }});
//              createIndex(transformer, toTable, timestampCondition);
//
//              List<Map<String, String>> xidCondition = buildIndexCondition(new HashSet<String>() {{
//                add("data.XID");
//              }});
//              createIndex(transformer, toTable, xidCondition);
//            }
//          } catch (Exception e) {
//            if (job.isRunning()) {
//              job.jobError(e, true, SyncStageEnum.SNAPSHOT.getSyncStage(), logger, ConnectorConstant.WORKER_TYPE_TRANSFORMER,
//                "Create target index failed, connection name: " + transformer.getContext().getJobTargetConn().getName() + ", table name: " + mapping.getTo_table() + "; " + e.getMessage(), null);
//            } else {
//              throw new ExecutorUtil.InterruptExecutorException();
//            }
//          }
//        },
//        "auto create target index", transformer.getContext(), transformerContext -> !transformerContext.isRunning(), transformer.getContext().getJob()
//      );
//
//      if (StringUtils.equalsAnyIgnoreCase(transformer.getContext().getJobTargetConn().getDatabase_type(), DatabaseTypeEnum.MONGODB.getType(), DatabaseTypeEnum.ALIYUN_MONGODB.getType())) {
//        // create data quality tag indexes
//        Map<String, List<DataRules>> dataRulesMap = transformer.getDataRulesMap();
//        if (MapUtils.isNotEmpty(dataRulesMap)) {
//          transformer.getContext().createDataRulesIndex(mappings, dataRulesMap);
//        }
//
//        // create ttl index
//        transformer.getContext().createTtlIndex(mappings);
//      }
//
////			Object supportCopySourceIndex = ReflectUtil.invokeInterfaceMethod(getObjectFromTransfomer(transformer), TARGET_EXTENT_CLASS_NAME, "supportCopySourceIndex");
////			if (Boolean.valueOf(supportCopySourceIndex + "")
////				&& transformer.getContext().getJob().getNeedToCreateIndex()) {
////				// copy source indices to target
////				copySourceIndex2Target(transformer);
////			}
//
//      // Milestone-CREATE_TARGET_INDEX-FINISH
//      MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.CREATE_TARGET_INDEX, MilestoneStatus.FINISH);
//    } catch (Exception e) {
//      String errMsg = String.format("Automatically create target index(s) failed, job name: %s, err: %s, stacks: %s",
//        transformer.getContext().getJob().getName(), e.getMessage() == null ? e.getCause().getMessage() : e.getMessage(), Log4jUtil.getStackString(e));
//
//      // Milestone-CREATE_TARGET_INDEX-ERROR
//      Optional.ofNullable(milestoneService).ifPresent(m -> m.updateMilestoneStatusByCode(MilestoneStage.CREATE_TARGET_INDEX, MilestoneStatus.ERROR, errMsg));
//
//      transformer.getContext().getJob().jobError(e, true, ConnectorConstant.SYNC_TYPE_INITIAL_SYNC, logger, ConnectorConstant.WORKER_TYPE_TRANSFORMER, errMsg, null);
//    }
//  }
//
//  private static List<Map<String, String>> buildIndexCondition(Set<String> keys) {
//    List<Map<String, String>> condition = new ArrayList<>();
//    Map<String, String> keyMap = new HashMap<>();
//    condition.add(keyMap);
//
//    if (CollectionUtils.isEmpty(keys)) {
//      return null;
//    }
//
//    keys.forEach(key -> keyMap.put(key, key));
//
//    return condition;
//  }
//
//  private static Document getIndexDoc(List<Map<String, String>> list) {
//    Document doc = new Document();
//    list.forEach(map -> map.entrySet().stream().forEachOrdered(e -> doc.put(e.getKey(), 1)));
//    return doc;
//  }
//
//  private static void createIndex(Transformer transformer, String toTable, List<Map<String, String>> condition) throws IllegalAccessException, InvocationTargetException {
//    long startTs = System.currentTimeMillis();
//
//    Object needCreateIndex = ReflectUtil.invokeInterfaceMethod(getObjectFromTransfomer(transformer),
//      TARGET_EXTENT_CLASS_NAME, "needCreateIndex",
//      toTable, condition, transformer.getContext().getJobTargetConn());
//    if (!Boolean.valueOf(needCreateIndex + "")) {
//      return;
//    }
//
//    if (logger.isDebugEnabled()) {
//      logger.debug(TapLog.TRAN_LOG_0035.getMsg(), transformer.getContext().getJobTargetConn().getDatabase_type(), toTable, getIndexDoc(condition).toJson());
//    }
//    try {
//      ReflectUtil.invokeInterfaceMethod(
//        getObjectFromTransfomer(transformer), TARGET_EXTENT_CLASS_NAME, "createIndex", toTable, condition
//      );
//      if (logger.isDebugEnabled()) {
//        logger.debug(TapLog.TRAN_LOG_0036.getMsg(), System.currentTimeMillis() - startTs);
//      }
//    } catch (InvocationTargetException e) {
//      throw e;
//    }
//  }
//
//  private static Object getObjectFromTransfomer(Transformer transformer) {
//    Object instance = null;
//    if (transformer instanceof DefaultTransformer) {
//      if (CollectionUtils.isNotEmpty(((DefaultTransformer) transformer).getTargets())) {
//        instance = ((DefaultTransformer) transformer).getTargets().get(0);
//      }
//    } else {
//      instance = transformer;
//    }
//
//    return instance;
//  }
//
//  private static List<Object> getObjectsFromTransfomer(Transformer transformer) {
//    List<Object> instances = new ArrayList<>();
//    if (transformer instanceof DefaultTransformer) {
//      if (CollectionUtils.isNotEmpty(((DefaultTransformer) transformer).getTargets())) {
//        instances.addAll(((DefaultTransformer) transformer).getTargets());
//      }
//    } else {
//      instances.add(transformer);
//    }
//
//    return instances;
//  }
//
//  private static boolean defaultTransformer(DatabaseTypeEnum sourceDatabaseType, DatabaseTypeEnum targetDatabaseType) {
//
//    switch (sourceDatabaseType) {
//      case ORACLE:
//      case MYSQL:
//      case MARIADB:
//      case MYSQL_PXC:
//      case SYBASEASE:
//      case MSSQL:
//      case ALIYUN_MSSQL:
//      case MONGODB:
//      case ALIYUN_MONGODB:
//      case KUNDB:
//      case ADB_MYSQL:
//      case ALIYUN_MYSQL:
//      case ALIYUN_MARIADB:
//        if (targetDatabaseType.equals(DatabaseTypeEnum.MONGODB) || targetDatabaseType.equals(DatabaseTypeEnum.ALIYUN_MONGODB)) {
//          return false;
//        } else {
//          return true;
//        }
//      default:
//        return true;
//    }
//  }
//
//  private static Transformer initDefaultTransformer(TransformerContext transformerContext) throws Exception {
//    try {
//      initialJobStats(transformerContext.getJob());
//
//      initConfig(transformerContext.getJob(), transformerContext.getJobSourceConn(), transformerContext.getJobTargetConn());
//
//      return createDefaultTransformer(transformerContext);
//    } catch (Exception e) {
//      if (transformerContext != null) {
//        transformerContext.closeMongoCient();
//      }
//      throw e;
//    }
//  }
//
//  private static Transformer initSpecialTransformer(TransformerContext transformerContext) throws IOException {
//    Transformer transformer = null;
//    String type = transformerContext.getJobSourceConn().getDatabase_type();
//    DatabaseTypeEnum sourceDatabaseType = DatabaseTypeEnum.fromString(type);
//
//    initialJobStats(transformerContext.getJob());
//
//    // init config
//    initConfig(transformerContext.getJob(), transformerContext.getJobSourceConn(), transformerContext.getJobTargetConn());
//    try {
//
//      switch (sourceDatabaseType) {
//        case MONGODB:
//        case ALIYUN_MONGODB:
//
//          Boolean isOplog = MongodbUtil.checkOplogOrChangeStream(transformerContext.getJob(), transformerContext.getJobSourceConn());
//          if (isOplog == null) {
//            return null;
//          }
//          //if (isOplog) {
//          //transformer = createOpExpressionTransformer(transformerContext);
//          //} else if (!isOplog) {
//          transformer = createAfterTransformer(transformerContext);
//          //}
//          break;
//
//        default:
//
//          transformer = createAfterTransformer(transformerContext);
//          break;
//      }
//    } catch (Exception e) {
//      if (transformerContext != null) {
//        transformerContext.closeMongoCient();
//      }
//      throw e;
//    }
//    return transformer;
//  }
//
//  private static void initialJobStats(Job job) {
//    Stats allStats = job.getStats();
//    Map<String, Long> stats = new HashMap<>();
//    stats.put("target_inserted", new Long(0));
//    stats.put("target_updated", new Long(0));
//    stats.put("processed", new Long(0));
//    stats.put("source_received", new Long(0));
//    stats.put("total_updated", new Long(0));
//    stats.put("total_deleted", new Long(0));
//
//    if (allStats != null) {
//      Map<String, Long> total = allStats.getTotal();
//      if (!total.containsKey(Stats.TOTAL_UPDATED_FIELD_NAME)) {
//        total.put(Stats.TOTAL_UPDATED_FIELD_NAME, new Long(0));
//      }
//      if (!total.containsKey(Stats.TOTAL_DELETED_FIELD_NAME)) {
//        total.put(Stats.TOTAL_DELETED_FIELD_NAME, new Long(0));
//      }
//
//      if (!total.containsKey(Stats.TOTAL_DATA_QUAILTY_FIELD_NAME)) {
//        total.put(Stats.TOTAL_DATA_QUAILTY_FIELD_NAME, new Long(0));
//      }
//    } else {
//      allStats = new Stats();
//      allStats.setTotal(stats);
//      job.setStats(allStats);
//    }
//  }
//
//  private static Transformer createDefaultTransformer(TransformerContext context) throws Exception {
//
//    Transformer transformer = null;
//    String databaseType = context.getJobTargetConn().getDatabase_type();
//    Class<?> clazzByDatabaseType = ClassScanner.getClazzByDatabaseType(databaseType, ClassScanner.TARGET);
//    if (clazzByDatabaseType != null) {
//      TransformerMetrics metrics = new TransformerMetrics(context);
//      transformer = new DefaultTransformer(context, clazzByDatabaseType, metrics);
//    }
//
//    return transformer;
//  }
//
//  private static Transformer createAfterTransformer(TransformerContext context) {
//    TransformerMetrics metrics = new TransformerMetrics(context);
//    TransformerInitialSyncProcess relateInitialSyncProcess = new TransformerInitialSyncProcess(context, metrics);
//    TransformerCdcProcess relateCdcProcess;
//    final boolean cdcConcurrency = context.getJob().getCdcConcurrency();
//    if (cdcConcurrency) {
//      relateCdcProcess = new TransformerConcurrentCdcSyncProcess(context, metrics);
//    } else {
//      relateCdcProcess = new TransformerCdcProcess(context, metrics);
//    }
//    return new RdmTransformer(context, relateInitialSyncProcess, relateCdcProcess, metrics);
//  }
//
//  private static void initConfig(Job job, Connections sourceConn, Connections targetConn) {
//    String mappingTemplate = job.getMapping_template();
//
//    if (ConnectorConstant.MAPPING_TEMPLATE_CLUSTER_CLONE.equals(mappingTemplate)) {
//      Map<String, List<RelateDataBaseTable>> schema = sourceConn.getSchema();
//
//      if (MapUtils.isNotEmpty(schema)) {
//        if (CollectionUtils.isEmpty(job.getMappings())
//          && !StringUtils.equalsAny(targetConn.getDatabase_type(), DatabaseTypeEnum.GRIDFS.getType(), DatabaseTypeEnum.FILE.getType())) {
//          job.generateCloneMapping(schema, null);
//        }
//
//        // add shard key for mapping join condition
//        String databaseType = targetConn.getDatabase_type();
//        if (StringUtils.equalsAnyIgnoreCase(databaseType, DatabaseTypeEnum.MONGODB.getType(), DatabaseTypeEnum.ALIYUN_MONGODB.getType())) {
//          List<Mapping> mappings = job.getMappings();
//          Map<String, List<String>> shardKeysByMappings = MongodbUtil.getShardKeysByMappings(mappings, targetConn);
//          if (MapUtils.isNotEmpty(shardKeysByMappings)) {
//            for (Mapping mapping : mappings) {
//              String toTable = mapping.getTo_table();
//              if (!shardKeysByMappings.containsKey(toTable)) {
//                continue;
//              }
//              List<String> shardKeys = shardKeysByMappings.get(toTable);
//              List<Map<String, String>> joinConditions = mapping.getJoin_condition();
//              Set<String> targetFields = new HashSet<>();
//              for (Map<String, String> joinCondition : joinConditions) {
//                String targetField = joinCondition.get("target");
//                targetFields.add(targetField);
//              }
//
//              for (String shardKey : shardKeys) {
//                if (targetFields.contains(shardKey)) {
//                  continue;
//                }
//                Map<String, String> joinCondition = new HashMap<>();
//                joinCondition.put("source", shardKey);
//                joinCondition.put("target", shardKey);
//                joinConditions.add(joinCondition);
//                logger.info("Add shard key {} as join key for mapping {}", joinCondition, mapping);
//              }
//            }
//          }
//        }
//      }
//    }
//    List<Mapping> mappings = job.getMappings();
//    if (CollectionUtils.isNotEmpty(mappings)) {
//      for (Mapping mapping : mappings) {
//        List<Map<String, String>> join_condition = mapping.getJoin_condition();
//        if (CollectionUtils.isNotEmpty(join_condition)) {
//          List<Map<String, String>> join_conditions = Mapping.reverseConditionMapKeyValue(join_condition);
//          mapping.setJoin_condition(join_conditions);
//        }
//        List<Map<String, String>> match_condition = mapping.getMatch_condition();
//        if (CollectionUtils.isNotEmpty(match_condition)) {
//          List<Map<String, String>> match_conditions = Mapping.reverseConditionMapKeyValue(match_condition);
//          mapping.setMatch_condition(match_conditions);
//        }
//      }
//
//      Mapping.initMappingForFieldProcess(job.getMappings(), job.getMapping_template());
//    }
//
//    if (!job.isEditDebug() && CollectionUtils.isEmpty(job.getStages())) {
//      OneManyUtil.addTableCloneWhenOneMany(job, true);
//    }
//    Map<String, List<Mapping>> tableMappings = MappingUtil.adaptMappingsToTableMappings(job.getMappings());
//    job.setTableMappings(tableMappings);
//  }
//}
