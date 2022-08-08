package io.tapdata.Schedule;

/**
 * @author jackin
 */
//@Component("dataFlowScheduler")
//@DependsOn("connectorManager")
public class DataFlowScheduler {

//
//  @PostConstruct
//  public void init() {
//    instanceNo = configCenter.getConfig(ConfigurationCenter.AGENT_ID).toString();
//    appType = (AppType) configCenter.getConfig(ConfigurationCenter.APPTYPE);
//    logger.info("[Data flow scheduler] instance no: {}", instanceNo);
//  }
//
//  @Bean("dataFlowStatsManager")
//  public DataFlowStatsManager initDataFlowStatsManager() {
//
//    dataFlowStatsManager = new DataFlowStatsManager(dataFlowClientMap, clientMongoOperator, settingService);
//    return dataFlowStatsManager;
//  }
//
//  /**
//   * 调度编排任务方法
//   */
//  @Scheduled(fixedDelay = 1000L)
//  public void scheduledDataFlow() {
//    Thread.currentThread().setName(String.format(ConnectorConstant.START_DATAFLOW_THREAD, instanceNo));
//
//    try {
//      Query query = new Query(
//        new Criteria("agentId").is(instanceNo)
//          .and(DataFlow.STATUS_FIELD).is(DataFlow.STATUS_SCHEDULED)
//      );
//
//      Update update = new Update();
//      update.set(DataFlow.STATUS_FIELD, DataFlow.STATUS_RUNNING);
//      update.set(DataFlow.PING_TIME_FIELD, System.currentTimeMillis());
//      addAgentIdUpdate(update);
//      MilestoneFlowService flowMilestoneService = null;
//      List<Job> jobs = null;
//
//      DataFlow dataFlow = clientMongoOperator.findAndModify(query, update, DataFlow.class, ConnectorConstant.DATA_FLOW_COLLECTION, true);
//      if (dataFlow != null) {
//        try {
//          setThreadContext(dataFlow);
//          if (dataFlowClientMap.containsKey(dataFlow.getId())) {
//            logger.info("Data flow {} is already running", dataFlow.getName());
//          } else {
//            try {
//              setThreadContext(dataFlow);
//
//              if (dataFlow.getStats() == null || CollectionUtils.isEmpty(dataFlow.getStats().getStagesMetrics())) {
//                DataFlowStats dataFlowStats = new DataFlowStats();
//                dataFlowStats.initStageMetrics(dataFlow.getStages());
//                dataFlow.setStats(dataFlowStats);
//              }
//              String dataFlowId = dataFlow.getId();
//              if (dataFlow.getSetting() != null
//                && !dataFlow.getSetting().getIsSchedule()
//                && !dataFlow.getSetting().getIncrement()
//                && ConnectorConstant.SYNC_TYPE_INITIAL_SYNC.equals(dataFlow.getSetting().getSync_type())
//                && dataFlowCheckInitialized(dataFlow)) {
//                logger.warn("This initial sync job has already been completed before, if wanna re-execute this job, please click RESET and START bottom.");
//                pausedDataFlow(dataFlowId);
//                return;
//              }
//              FlowEngineVersion flowEngineVersion = FlowEngineVersion.fromVersion(dataFlow.getSetting().getFlowEngineVersion());
//
//              //判断负载，如果负载达到限度则拒绝执行任务
//              if (performanceStatistics.isOverLoad()) {
//                logger.error("The load is too high, the task will be put in an error state");
//                errorDataFlowWithErrMsg(dataFlowId, DataFlow.NOT_ENOUGH_RESOURCE);
//                return;
//              }
//
//              // 处理模型推演，版本兼容问题
//              handleTranModelVersionControl(jobs, dataFlow);
//
//              DataFlowClient dataFlowClient;
//              // Init data flow client
//              try {
//                switch (flowEngineVersion) {
//                  case V1:
//                    dataFlowClient = dataFlowManagerV1.submit(dataFlow);
//                    break;
//                  case V2_JET:
//                    dataFlowClient = jetDataFlowManager.submit(dataFlow);
//                    break;
//                  default:
//                    logger.warn("Unrecognized engine version: " + flowEngineVersion + ", will run with engine v1");
//                    dataFlowClient = dataFlowManagerV1.submit(dataFlow);
//                    break;
//                }
//              } catch (Exception e) {
//                String err = "Init data flow client failed, cause: " + e.getMessage();
//                throw new RuntimeException(err, e);
//              }
//
//              if (dataFlowClient != null) {
//                dataFlowClientMap.put(dataFlowId, dataFlowClient);
//              } else {
//                throw new RuntimeException("Start data flow engine failed, cause: cannot init client");
//              }
//
//            } catch (ManagementException e) {
//              logger.warn(TapLog.JOB_WARN_0005.getMsg(), dataFlow.getName(), Log4jUtil.getStackString(e));
//            } catch (Exception e) {
//              if (flowMilestoneService != null) {
//                String errMsg = String.format("Schedule data flow %s failed %s, stacks: %s",
//                  dataFlow.getName(), e.getMessage(), Log4jUtil.getStackString(e));
//                try {
//                  if (jobs != null) {
//                    flowMilestoneService.updateJobsMilestone(MilestoneStage.INIT_DATAFLOW, MilestoneStatus.ERROR, errMsg, jobs);
//                  }
//                  flowMilestoneService.updateList();
//                } catch (Exception ignore) {
//                }
//              }
//
//              errorDataFlow(dataFlow.getId());
//              logger.error("Schedule data flow {} failed {}", dataFlow.getName(), e.getMessage(), e);
//
//            }
//          }
//        } finally {
//          ThreadContext.clearAll();
//        }
//      }
//    } catch (Exception e) {
//      logger.error("Schedule data flows failed {}", e.getMessage(), e);
//    }
//  }
//
//  private void handleTranModelVersionControl(List<Job> jobs, DataFlow dataFlow) {
//    // only when the version is "v2", use the type mapping setting
//    if (!"v2".equalsIgnoreCase(dataFlow.getSetting().getTransformModelVersion())) {
//      return;
//    }
//    Map<String, Boolean> tranModelVersionControl = dataFlow.getTranModelVersionControl();
//    if (MapUtils.isNotEmpty(tranModelVersionControl)) {
//      for (String stageId : tranModelVersionControl.keySet()) {
//        Boolean versionControl = tranModelVersionControl.get(stageId);
//        // find stage by stage id
//        Stage findStage = dataFlow.getStages().stream().filter(stage -> stage.getId().equals(stageId)).findFirst().orElse(null);
//        if (findStage == null) {
//          continue;
//        }
//        // find mapping
//        for (Job job : jobs) {
//          List<Mapping> mappings = job.getMappings();
//          mappings.stream().filter(mapping -> sameMappingBySinkStage(mapping, findStage)).findFirst().ifPresent(findMapping -> findMapping.setTranModelVersionControl(versionControl));
//        }
//      }
//    }
//  }
//
//  /**
//   * 当发送ddl事件后，任务终止后重新启动，需要重新加载源和目标的schema
//   *
//   * @param jobs
//   */
//  private void loadSchema(List<Job> jobs) {
//    Map<String, Connections> needLoadSchemaMap = new HashMap<>();
//    for (Job job : jobs) {
//      RuntimeInfo runtimeInfo = job.getRuntimeInfo();
//      if (runtimeInfo != null && !runtimeInfo.getDdlConfirm()) {
//        runtimeInfo.getUnSupportedDDLS().forEach(item -> {
//          JobConnection jobConnection = job.getConnections();
//          setNeedLoadSchemaMap(needLoadSchemaMap, job, jobConnection.getSource(), true);
//          setNeedLoadSchemaMap(needLoadSchemaMap, job, jobConnection.getTarget(), false);
//        });
//      }
//    }
//    if (needLoadSchemaMap.size() > 0) {
//      needLoadSchemaMap.values().forEach(c -> {
//        String table_filter = c.getTable_filter();
//        logger.info("connections {}, load schema [{}]...", c.getName(), table_filter);
//        new LoadSchemaRunner(c, this.clientMongoOperator, 0).run();
//      });
//    }
//  }
//
//  private void setNeedLoadSchemaMap(Map<String, Connections> needLoadSchemaMap, Job job, String connectionId, boolean isSource) {
//    Connections connections = needLoadSchemaMap.get(connectionId);
//    if (connections == null) {
//      connections = job.getConn(isSource, this.clientMongoOperator);
//      needLoadSchemaMap.put(connectionId, connections);
//    }
//    //table_filter无法单独更新指定表模型，会清掉不符合table_filter的模型，所以不进行设置，还是全量更新
////    if (StringUtils.isEmpty(sourceConnections.getTable_filter())) {
////      sourceConnections.setTable_filter(tableMame);
////    } else {
////      sourceConnections.setTable_filter(sourceConnections.getTable_filter() + "," + tableMame);
////    }
//  }
//
//  /**
//   * 扫描状态为force stopping状态的编排任务，执行强制停止
//   */
//  @Scheduled(fixedDelay = 5000L)
//  public void stoppingDataFlow() {
//
//    try {
//      for (Map.Entry<String, DataFlowClient> entry : dataFlowClientMap.entrySet()) {
//        DataFlowClient dataFlowClient = entry.getValue();
//        DataFlow dataFlow = dataFlowClient.getDataFlow();
//        if (DataFlow.STATUS_FORCE_STOPPING.equals(dataFlow.getStatus())) {
//          stopFlow(dataFlowClient, true);
//        } else if (DataFlow.STATUS_STOPPING.equals(dataFlow.getStatus())) {
//          stopFlow(dataFlowClient, false);
//        }
//        cleanCacheIfNeed(dataFlowClient);
//        JobCustomerLogger.dataflowStopped(dataFlow.getId(), dataFlow.getName(), clientMongoOperator);
//      }
//
//      List<DataFlow> timeoutStoppingFlows = findTimeoutStoppingFlows();
//      for (DataFlow timeoutStoppingFlow : timeoutStoppingFlows) {
//        pausedDataFlow(timeoutStoppingFlow.getId());
//      }
//    } catch (Exception e) {
//      logger.error("Scan force stopping data flow failed {}", e.getMessage(), e);
//    }
//  }
//
//  @Scheduled(fixedDelay = 5000L)
//  public void errorDataFlow() {
//    for (Iterator<Map.Entry<String, DataFlowClient>> it = dataFlowClientMap.entrySet().iterator(); it.hasNext(); ) {
//      Map.Entry<String, DataFlowClient> entry = it.next();
//      DataFlowClient dataFlowClient = entry.getValue();
//      DataFlow dataFlow = dataFlowClient.getDataFlow();
//      String dataFlowId = dataFlow.getId();
//      try {
//        errorFlow(dataFlowClient);
//      } catch (Exception e) {
//        logger.error("Check data flow {}[{}] error status failed {}", dataFlow.getName(), dataFlowId, e.getMessage(), e);
//      }
//    }
//  }
//
//  private boolean sameMappingBySinkStage(Mapping mapping, Stage stage) {
//    if (mapping == null || stage == null || DataFlowStageUtil.isProcessorStage(stage.getType())
//      || StringUtils.isAnyBlank(mapping.getTo_table(), stage.getTableName())
//      || CollectionUtils.isEmpty(mapping.getStages())) {
//      return false;
//    }
//
//    return mapping.getTo_table().equals(stage.getTableName())
//      && mapping.getStages().get(mapping.getStages().size() - 1).getId().equals(stage.getId());
//  }
//
//  private List<Job> saveFlowJobs(String dataFlowId, List<Job> jobs, DataFlow dataFlow) throws Exception {
//    Map<String, Object> params = new HashMap<>();
//
//    for (Job job : jobs) {
//      /*
//       * 表数量过多，mappings过大，会导致无法存入中间库
//       * 这里拆分后的Job里面不存入mappings，当启动Jobs的时候，再根据stages进行解析
//       * 这里删除 mapping 会影响暂时还不支持类型映射的数据源，先加上判断，只在类型映射的情况下才删除 mapping
//       */
//      boolean removeMapping = true;
//      String transformModelVersion = dataFlow.getSetting().getTransformModelVersion();
//      if ("v2".equals(transformModelVersion)) {
//        job.setMappings(null);
//      } else {
//        for (Mapping mapping : job.getMappings()) {
//          // 既存在有类型映射的也有无类型映射的，遵循无类型映射的标准
//          if (!removeMapping) {
//            break;
//          }
//          removeMapping = mapping.isTranModelVersionControl();
//        }
//        if (removeMapping) {
//          job.setMappings(null);
//        }
//      }
//
//      if (Boolean.valueOf(configCenter.getConfig("is_cloud").toString())) {
//        params.put("dataFlowId", dataFlowId);
//        params.put("connections.source", job.getConnections().getSource());
//        params.put("connections.target", job.getConnections().getTarget());
//        if (job.getPartitionId() != null) {
//          params.put("partitionId", job.getPartitionId());
//        }
//        Map<String, Object> jobMap = MapUtil.obj2Map(job);
//        jobMap.remove("id");
//        jobMap.remove("logger");
//        jobMap.remove("serialVersionUID");
//        jobMap.put("agentId", instanceNo);
//        job = clientMongoOperator.upsert(params, jobMap, ConnectorConstant.JOB_COLLECTION, Job.class);
//        logger.info("Finished saved job {}", job.getName());
//      } else {
//        if (!job.isRunning()) {
//          params.put("dataFlowId", dataFlowId);
//          params.put("connections.source", job.getConnections().getSource());
//          params.put("connections.target", job.getConnections().getTarget());
//          if (job.getPartitionId() != null) {
//            params.put("partitionId", job.getPartitionId());
//          }
//          Map<String, Object> jobMap = MapUtil.obj2Map(job);
//          jobMap.remove("id");
//          jobMap.remove("logger");
//          jobMap.remove("serialVersionUID");
//          jobMap.put("agentId", instanceNo);
//          job = clientMongoOperator.upsert(params, jobMap, ConnectorConstant.JOB_COLLECTION, Job.class);
//          logger.info("Finished saved job {}", job.getName());
//        } else {
//          logger.info("Job {} is running, will not modified job config.", job.getName());
//        }
//      }
//    }
//
//    return jobs;
//  }
//
//  private void startJobsByDataFlowId(List<Job> jobs) {
//
//    for (Job job : jobs) {
//      String id = job.getId();
//      Criteria where = where("_id").is(id);
//
//      Query query = new Query(where);
//
//      Update update = new Update();
//
//      job.setTimeoutToStop(false);
//      job.setPing_time(System.currentTimeMillis());
//      job.setConnector_ping_time(System.currentTimeMillis());
//      String status = job.getStatus();
//      List<Stage> stages = job.getStages();
//      if (CollectionUtils.isNotEmpty(stages)) {
//        StringBuilder sb = new StringBuilder();
//        for (Stage stage : stages) {
//          String name = stage.getName();
//          sb.append(name).append(", ");
//        }
//
//        String stagesName = StringUtils.removeEnd(sb.toString(), ", ");
//        logger.info("Job {} include stages [{}]", job.getName(), stagesName);
//      }
//
//      update.set(ConnectorConstant.JOB_STATUS_FIELD, ConnectorConstant.SCHEDULED);
//      update.set(ConnectorConstant.JOB_CONNECTOR_STOPPED_FIELD, false);
//      update.set(ConnectorConstant.JOB_TRANSFORMER_STOPPED_FIELD, false);
//      update.set("timeoutToStop", false);
//      // 更新 ddlConfirm 为ture
//      RuntimeInfo runtimeInfo = job.getRuntimeInfo();
//      if (runtimeInfo != null) {
//        runtimeInfo.setDdlConfirm(true);
//        update.set("runtimeInfo", runtimeInfo);
//      }
//
//      if (Boolean.valueOf(configCenter.getConfig("is_cloud").toString())) {
//
//        // 云版启动子任务逻辑
//        job.setStatus(ConnectorConstant.SCHEDULED);
//        logger.info("Modified job {} status to scheduled.", job.getName());
//        clientMongoOperator.update(query, update, ConnectorConstant.JOB_COLLECTION);
//
//      } else {
//        // 企业版启动子任务逻辑
////				if (!ConnectorConstant.RUNNING.equals(status)) {
////
////					clientMongoOperator.update(query, update, ConnectorConstant.JOB_COLLECTION);
////					job.setStatus(ConnectorConstant.SCHEDULED);
////					logger.info("Modified job {} status to scheduled.", job.getName());
////
////				} else if (!job.getIsDistribute()) {
//
//        update.set("agentId", instanceNo);
//        update.set("process_id", instanceNo);
//        clientMongoOperator.update(query, update, ConnectorConstant.JOB_COLLECTION);
//        job.setStatus(ConnectorConstant.SCHEDULED);
//        logger.info("Modified job {} status to scheduled.", job.getName());
//
////				}
//      }
//    }
//
//  }
//
//  @Scheduled(fixedDelay = 15000L)
//  public void refreshCacheDataFlowJobs() {
//    Thread.currentThread().setName(String.format("Refresh Cache Dataflow Jobs Handler[%s]", instanceNo));
//    try {
//
//      for (Map.Entry<String, DataFlowClient> entry : dataFlowClientMap.entrySet()) {
//        DataFlowClient dataFlowClient = entry.getValue();
//        if (dataFlowClient instanceof DataFlowClientV1) {
//          dataFlowManagerV1.refreshCache(dataFlowClient);
//        } else if (dataFlowClient instanceof JetDataFlowClient) {
//          jetDataFlowManager.refreshCache(dataFlowClient);
//        }
//      }
//    } catch (Exception e) {
//      logger.error("Refresh data flows and jobs cache failed {}", e.getMessage(), e);
//    }
//  }
//
//  @Scheduled(fixedDelay = 5000L)
//  public void pingDataFlow() {
//    Thread.currentThread().setName(String.format("Ping Dataflow Handler[%s]", instanceNo));
//
//    try {
//      long startTs = System.currentTimeMillis();
//
//      pingAllDataFlows();
//
//      long pingFlowsEndTs = System.currentTimeMillis();
//      if ((pingFlowsEndTs - startTs) > LONG_TIME_EXECUTED_CAPACITY) {
//        logger.info("Ping all data flows spent {}ms.", (pingFlowsEndTs - startTs));
//      }
//
//      for (Map.Entry<String, DataFlowClient> entry : dataFlowClientMap.entrySet()) {
//        DataFlowClient dataFlowClient = entry.getValue();
//
//        if (dataFlowClient instanceof DataFlowClientV1) {
//          dataFlowManagerV1.ping(dataFlowClient);
//        } else if (dataFlowClient instanceof JetDataFlowClient) {
//          jetDataFlowManager.ping(dataFlowClient);
//        }
//      }
//    } catch (Exception e) {
//      logger.error("Ping all data flow failed {}", e.getMessage(), e);
//    }
//  }
//
//  @Scheduled(fixedDelay = 5000L)
//  public void dataFlowStats() {
//    Thread.currentThread().setName(String.format(ConnectorConstant.STATS_DATAFLOW_THREAD, instanceNo));
//    try {
//      long startTs = System.currentTimeMillis();
//      dataFlowStatsManager.stats();
//      long endTs = System.currentTimeMillis();
//      if ((endTs - startTs) > LONG_TIME_EXECUTED_CAPACITY) {
//        logger.info("Collect all data flow stats from jobs spent {}ms", endTs - startTs);
//      }
//    } catch (Exception e) {
//      logger.error("Collect all data flow stats from jobs failed {}", e.getMessage(), e);
//    }
//  }
//
//  private void pingAllDataFlows() {
//    List<String> ids = new ArrayList<>();
//    for (String flowId : dataFlowClientMap.keySet()) {
//      ids.add(flowId);
//    }
//
//    clientMongoOperator.update(
//      new Query(where("_id").is(new Document("inq", ids))),
//      new Update()
//        .set(DataFlow.PING_TIME_FIELD, System.currentTimeMillis()),
//      ConnectorConstant.DATA_FLOW_COLLECTION
//    );
//  }
//
//  private void stopFlow(DataFlowClient dataFlowClient, boolean force) {
//    if (dataFlowClient == null || dataFlowClient.getDataFlow() == null || StringUtils.isBlank(dataFlowClient.getDataFlow().getId())) {
//      return;
//    }
//    boolean stopped = false;
//    if (dataFlowClient instanceof DataFlowClientV1) {
//      stopped = dataFlowManagerV1.stop(dataFlowClient, force);
//    } else if (dataFlowClient instanceof JetDataFlowClient) {
//      stopped = jetDataFlowManager.stop(dataFlowClient, force);
//    }
//    if (stopped) {
//      cleanCacheIfNeed(dataFlowClient);
//      JobCustomerLogger.dataflowStopped(dataFlowClient.getDataFlow().getId(), dataFlowClient.getDataFlow().getName(), clientMongoOperator);
//    }
//  }
//
//  private void errorFlow(DataFlowClient dataFlowClient) {
//    if (dataFlowClient == null || dataFlowClient.getDataFlow() == null || StringUtils.isBlank(dataFlowClient.getDataFlow().getId())) {
//      return;
//    }
//
//    boolean stopped = false;
//    if (dataFlowClient instanceof DataFlowClientV1) {
//      stopped = dataFlowManagerV1.error(dataFlowClient);
//    } else if (dataFlowClient instanceof JetDataFlowClient) {
//      stopped = jetDataFlowManager.error(dataFlowClient);
//    }
//
//    if (stopped) {
//      cleanCacheIfNeed(dataFlowClient);
//      JobCustomerLogger.dataflowStopped(dataFlowClient.getDataFlow().getId(), dataFlowClient.getDataFlow().getName(), clientMongoOperator);
//    }
//  }
//
//  private synchronized void cleanCacheIfNeed(DataFlowClient dataFlowClient) {
//    if (dataFlowClient == null || dataFlowClient.getDataFlow() == null || StringUtils.isBlank(dataFlowClient.getDataFlow().getId())) {
//      return;
//    }
//    String dataFlowId = dataFlowClient.getDataFlow().getId();
//    String dataFlowStatus = dataFlowClient.getDataFlow().getStatus();
//    if (!StringUtils.equalsAny(dataFlowStatus, DataFlow.STATUS_RUNNING, DataFlow.STATUS_STOPPING, DataFlow.STATUS_FORCE_STOPPING)) {
//      return;
//    }
//    DataFlowStatus status = dataFlowClient.getStatus();
//    switch (status) {
//      case PAUSED:
//        pausedDataFlow(dataFlowId);
//        removeDataFlowIfNeed(dataFlowId);
//        break;
//      case INTERNAL_PAUSED:
//        removeDataFlowIfNeed(dataFlowId);
//        break;
//      case ERROR:
//        errorDataFlow(dataFlowId);
//        removeDataFlowIfNeed(dataFlowId);
//      default:
//        break;
//    }
//  }
//
//  /**
//   * 查找超时未停止的编排任务
//   *
//   * @return
//   */
//  private List<DataFlow> findTimeoutStoppingFlows() {
//    long jobHeartTimeout = getJobHeartTimeout();
//    long expiredTimeMillis = System.currentTimeMillis() - jobHeartTimeout;
//    Criteria statusCriteria = new Criteria().orOperator(
//      where(DataFlow.STATUS_FIELD).is(DataFlow.STATUS_STOPPING),
//      where(DataFlow.STATUS_FIELD).is(DataFlow.STATUS_FORCE_STOPPING)
//    );
//    Criteria timeoutCriteria = new Criteria()
//      .orOperator(
//        where(DataFlow.PING_TIME_FIELD).lt(Double.valueOf(String.valueOf(expiredTimeMillis))),
//        where(DataFlow.PING_TIME_FIELD).is(null),
//        where(DataFlow.PING_TIME_FIELD).exists(false)
//      );
//
//    Query query = new Query(new Criteria().andOperator(
//      statusCriteria,
//      timeoutCriteria
//    ));
//    query.fields().include("id").include("status");
//    return clientMongoOperator.find(query, ConnectorConstant.DATA_FLOW_COLLECTION, DataFlow.class);
//  }
//
//  private void removeConnetionsMap(DataFlow dataFlow) {
//    if (dataFlow == null) {
//      return;
//    }
//    try {
//      for (Stage stage : dataFlow.getStages()) {
//        if (stage.getConnectionId() == null) {
//          continue;
//        }
//        boolean isExistConnection = false;
//        ok:
//        for (Iterator<Map.Entry<String, DataFlowClient>> it = dataFlowClientMap.entrySet().iterator(); it.hasNext(); ) {
//          Map.Entry<String, DataFlowClient> entry = it.next();
//          DataFlow value = entry.getValue().getDataFlow();
//          if (value.getId().equals(dataFlow.getId())) {
//            continue;
//          }
//          for (Stage valueStage : value.getStages()) {
//            if (valueStage.getConnectionId() != null && valueStage.getConnectionId().equals(stage.getConnectionId())) {
//              isExistConnection = true;
//              break ok;
//            }
//          }
//        }
//        if (!isExistConnection && connectionsMap.containsKey(stage.getConnectionId())) {
//          connectionsMap.remove(stage.getConnectionId());
//        }
//      }
//    } catch (Exception e) {
//      logger.error("Remove Connetions Map error,message: {}", e.getMessage(), e);
//    }
//  }
//
//  private void pausedDataFlow(String dataFlowId) {
//    try {
//      dataFlowStatsManager.stats(dataFlowId);
//    } catch (Exception e) {
//    }
//    Criteria where = where("_id").is(dataFlowId);
//    clientMongoOperator.update(new Query(where), new Update().set(DataFlow.STATUS_FIELD, DataFlow.STATUS_PAUSED), ConnectorConstant.DATA_FLOW_COLLECTION);
//  }
//
//  private void errorDataFlow(String dataFlowId) {
//    try {
//      dataFlowStatsManager.stats(dataFlowId);
//    } catch (Exception ignore) {
//    }
//    Query query = new Query(where("_id").is(dataFlowId));
//    Update update = new Update().set(DataFlow.STATUS_FIELD, DataFlow.STATUS_ERROR);
//    clientMongoOperator.update(query, update, ConnectorConstant.DATA_FLOW_COLLECTION);
//    removeDataFlowIfNeed(dataFlowId);
//  }
//
//  private void errorDataFlowWithErrMsg(String dataFlowId, String errMsg) {
//    try {
//      dataFlowStatsManager.stats(dataFlowId);
//    } catch (Exception ignore) {
//    }
//    Query query = new Query(where("_id").is(dataFlowId));
//    Update update = new Update().set(DataFlow.STATUS_FIELD, DataFlow.STATUS_ERROR)
//      .set(DataFlow.ERROR_MSG, errMsg);
//    clientMongoOperator.update(query, update, ConnectorConstant.DATA_FLOW_COLLECTION);
//    removeDataFlowIfNeed(dataFlowId);
//  }
//
//  private void removeDataFlowIfNeed(String dataFlowId) {
//    if (dataFlowId == null) {
//      return;
//    }
//    Optional.ofNullable(dataFlowClientMap).ifPresent(
//      dataFlowClientMap -> dataFlowClientMap.remove(dataFlowId)
//    );
//    Optional.ofNullable(dataFlowClientMap).ifPresent(
//      dataFlowClientMap -> Optional.ofNullable(dataFlowClientMap.get(dataFlowId)).ifPresent(
//        dataFlowClient -> removeConnetionsMap(dataFlowClient.getDataFlow())
//      )
//    );
//
//    // 删除缓存中数据，回收资源
//    messageDao.getCacheService().removeCacheStageRuntimeStats(dataFlowId, null);
//    logger.info("Removed cache StageRuntimeStats.");
//
//  }
//
//  private void addAgentIdCriteria(Criteria where) {
//    where.and("agentId").is(instanceNo);
//  }
//
//  private void addAgentIdUpdate(Update update) {
//    update.set("agentId", instanceNo);
//  }
//
//  /**
//   * check all dataflow stage is initialized
//   **/
//  private boolean dataFlowCheckInitialized(DataFlow dataFlow) {
//    boolean flag = false;
//    if (dataFlow.getStats() != null && CollectionUtils.isNotEmpty(dataFlow.getStats().getStagesMetrics())) {
//      List<StageRuntimeStats> stagesMetrics = dataFlow.getStats().getStagesMetrics();
//      if (CollectionUtils.isNotEmpty(dataFlow.getStages())) {
//        for (Stage stage : dataFlow.getStages()) {
//          if (CollectionUtils.isNotEmpty(stage.getOutputLanes())) {
//            flag = true;
//            Optional<StageRuntimeStats> optional = stagesMetrics.stream().filter(s -> s.getStageId().equals(stage.getId()) && ConnectorConstant.STATS_STATUS_INITIALIZED.equals(s.getStatus())).findFirst();
//            if (!optional.isPresent()) {
//              return false;
//            }
//          }
//        }
//      }
//    }
//    return flag;
//  }
//
//  private boolean sameFlowJob(Job job, Job other) {
//    String source = job.getConnections().getSource();
//    String target = job.getConnections().getTarget();
//    String dataFlowId = job.getDataFlowId();
//
//    String otherSource = other.getConnections().getSource();
//    String otherTarget = other.getConnections().getTarget();
//    String otherDataFlowId = other.getDataFlowId();
//
//    return StringUtils.equals(source, otherSource) &&
//      StringUtils.equals(target, otherTarget) &&
//      StringUtils.equals(dataFlowId, otherDataFlowId);
//  }
//
//  private void setThreadContext(DataFlow dataFlow) {
//    ThreadContext.clearAll();
//
//    ThreadContext.put("userId", dataFlow.getUser_id());
//    ThreadContext.put(DebugConstant.SUB_DATAFLOW_ID, dataFlow.getId());
//    ThreadContext.put("jobName", dataFlow.getName());
//    ThreadContext.put("app", ConnectorConstant.WORKER_TYPE_CONNECTOR);
//  }
//
//  private long getJobHeartTimeout() {
//    return settingService.getLong("jobHeartTimeout", 60000L);
//  }
}
