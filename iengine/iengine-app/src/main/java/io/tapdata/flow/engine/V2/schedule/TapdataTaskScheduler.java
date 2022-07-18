package io.tapdata.flow.engine.V2.schedule;

import com.tapdata.constant.CollectionUtil;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.entity.AppType;
import com.tapdata.entity.TapLog;
import com.tapdata.entity.dataflow.DataFlow;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.SubTaskDto;
import io.tapdata.common.SettingService;
import io.tapdata.dao.MessageDao;
import io.tapdata.debug.DebugConstant;
import io.tapdata.exception.ManagementException;
import io.tapdata.flow.engine.V2.task.TaskClient;
import io.tapdata.flow.engine.V2.task.TaskService;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.springframework.data.mongodb.core.query.Criteria.where;


/**
 * @author jackin
 */
@Component
@DependsOn("connectorManager")
public class TapdataTaskScheduler {

  private final static long LONG_TIME_EXECUTED_CAPACITY = 1000L;

  private Logger logger = LogManager.getLogger(TapdataTaskScheduler.class);

  private Map<String, TaskClient<SubTaskDto>> taskClientMap = new ConcurrentHashMap<>();

  private String instanceNo;

  @Autowired
  private ClientMongoOperator clientMongoOperator;

  @Autowired
  private ConfigurationCenter configCenter;

  @Autowired
  private SettingService settingService;

  @Autowired
  private TaskService<SubTaskDto> hazelcastTaskService;

  @Autowired
  private MessageDao messageDao;

  private AppType appType;


  @PostConstruct
  public void init() {
//    instanceNo = configCenter.getConfig(ConfigurationCenter.AGENT_ID).toString();
//    appType = (AppType) configCenter.getConfig(ConfigurationCenter.APPTYPE);
    instanceNo = (String) configCenter.getConfig(ConfigurationCenter.AGENT_ID);
    logger.info("[Task scheduler] instance no: {}", instanceNo);
  }

  /**
   * 调度编排任务方法
   */
  @Scheduled(fixedDelay = 1000L)
  public void scheduledTask() {
    Thread.currentThread().setName(String.format(ConnectorConstant.START_DATAFLOW_THREAD, instanceNo));

    try {
      Query query = new Query(
        new Criteria("agentId").is(instanceNo)
          .and(DataFlow.STATUS_FIELD).is(SubTaskDto.STATUS_WAIT_RUN)
      );

      Update update = new Update();
//      update.set(DataFlow.STATUS_FIELD, DataFlow.STATUS_RUNNING);
      update.set(DataFlow.PING_TIME_FIELD, System.currentTimeMillis());
//      update.set(DataFlow.START_TIME_FIELD, new Date());
      addAgentIdUpdate(update);

      SubTaskDto subTaskDto = clientMongoOperator.findAndModify(query, update, SubTaskDto.class, ConnectorConstant.SUB_TASK_COLLECTION, true);
      if (subTaskDto != null) {
        try {
          try {
            final String taskId = subTaskDto.getId().toHexString();
            clientMongoOperator.updateById(new Update(), ConnectorConstant.SUB_TASK_COLLECTION + "/running", taskId, SubTaskDto.class);
            Log4jUtil.setThreadContext(subTaskDto);
            final TaskClient<SubTaskDto> subTaskDtoTaskClient = hazelcastTaskService.startTask(subTaskDto);
            taskClientMap.put(subTaskDtoTaskClient.getTask().getId().toHexString(), subTaskDtoTaskClient);
          } catch (ManagementException e) {
            logger.warn(TapLog.JOB_WARN_0005.getMsg(), subTaskDto.getName(), Log4jUtil.getStackString(e));
          } catch (Exception e) {
            logger.error("Schedule task {} failed {}", subTaskDto.getName(), e.getMessage(), e);
            clientMongoOperator.updateById(new Update(), ConnectorConstant.SUB_TASK_COLLECTION + "/runError", subTaskDto.getId().toHexString(), SubTaskDto.class);
          }
        } finally {
          ThreadContext.clearAll();
        }
      }
    } catch (Exception e) {
      logger.error("Schedule task failed {}", e.getMessage(), e);
    }
  }

  /**
   * 扫描状态为force stopping状态的编排任务，执行强制停止
   */
  @Scheduled(fixedDelay = 5000L)
  public void forceStoppingTask() {

    try {
      for (Iterator<Map.Entry<String, TaskClient<SubTaskDto>>> it = taskClientMap.entrySet().iterator(); it.hasNext(); ) {
        Map.Entry<String, TaskClient<SubTaskDto>> entry = it.next();
        TaskClient<SubTaskDto> subTaskDtoTaskClient = entry.getValue();
        final SubTaskDto subTask = subTaskDtoTaskClient.getTask();
        final SubTaskDto stopTask = findStopTask(subTask.getId().toHexString());
        if (stopTask != null) {
          stopTask(subTaskDtoTaskClient);
        }
//        else if (DataFlow.STATUS_STOPPING.equals(subTask.getStatus())) {
//          stopTask(dataFlowClient, false);
//        }
      }

      List<SubTaskDto> timeoutStoppingTasks = findStoppingTasks();
      for (SubTaskDto timeoutStoppingTask : timeoutStoppingTasks) {
        final String taskId = timeoutStoppingTask.getId().toHexString();
        clientMongoOperator.updateById(new Update(), ConnectorConstant.SUB_TASK_COLLECTION + "/stopped", taskId, SubTaskDto.class);
        ;
      }
    } catch (Exception e) {
      logger.error("Scan force stopping data flow failed {}", e.getMessage(), e);
    }
  }

  /**
   * 检查出错的编排任务，执行强制停止
   */
  @Scheduled(fixedDelay = 5000L)
  public void errorOrStopTask() {

    try {
      for (Iterator<Map.Entry<String, TaskClient<SubTaskDto>>> it = taskClientMap.entrySet().iterator(); it.hasNext(); ) {

        Map.Entry<String, TaskClient<SubTaskDto>> entry = it.next();
        TaskClient<SubTaskDto> subTaskDtoTaskClient = entry.getValue();
        final String status = subTaskDtoTaskClient.getStatus();
        if (SubTaskDto.STATUS_ERROR.equals(status)) {
          errorTask(subTaskDtoTaskClient);
        } else if (SubTaskDto.STATUS_STOP.equals(status)) {
          stopTask(subTaskDtoTaskClient);
        } else if (SubTaskDto.STATUS_COMPLETE.equals(status)) {
          completeTask(subTaskDtoTaskClient);
        }
      }
    } catch (Exception e) {
      logger.error("Scan force stopping data flow failed {}", e.getMessage(), e);
    }
  }

  private void destroyCache(TaskClient<SubTaskDto> subTaskDtoTaskClient) {
    String cacheName = subTaskDtoTaskClient.getCacheName();
    if (StringUtils.isNotEmpty(cacheName)) {
      messageDao.updateCacheStatus(cacheName, subTaskDtoTaskClient.getStatus());
      messageDao.destroyCache(subTaskDtoTaskClient.getTask(), cacheName);
    }
  }

  private void errorTask(TaskClient<SubTaskDto> subTaskDtoTaskClient) {
    if (subTaskDtoTaskClient == null || subTaskDtoTaskClient.getTask() == null || StringUtils.isBlank(subTaskDtoTaskClient.getTask().getId().toHexString())) {
      return;
    }
    final String taskId = subTaskDtoTaskClient.getTask().getId().toHexString();
    clientMongoOperator.updateById(new Update(), ConnectorConstant.SUB_TASK_COLLECTION + "/runError", taskId, SubTaskDto.class);
    taskClientMap.remove(taskId);
    destroyCache(subTaskDtoTaskClient);
  }

  /**
   * 查找超时未停止的编排任务
   *
   * @return
   */
  private List<SubTaskDto> findStoppingTasks() {
    long jobHeartTimeout = getJobHeartTimeout();
    long expiredTimeMillis = System.currentTimeMillis() - jobHeartTimeout;
    Criteria timeoutCriteria = where("status").is(SubTaskDto.STATUS_STOPPING)
//      .and("agentId").is(instanceNo)
      .orOperator(
        where("pingTime").lt(Double.valueOf(String.valueOf(expiredTimeMillis))),
        where("pingTime").is(null),
        where("pingTime").exists(false));

    Query query = new Query(timeoutCriteria);
    query.fields().include("id").include("status");
    return clientMongoOperator.find(query, ConnectorConstant.SUB_TASK_COLLECTION, SubTaskDto.class);
  }

  /**
   * 查找停止的编排任务
   *
   * @param taskId
   * @return
   */
  private SubTaskDto findStopTask(String taskId) {
    Criteria timeoutCriteria = where("_id").is(taskId)
      .orOperator(
        where("status").is(SubTaskDto.STATUS_STOPPING),
        where("status").is(SubTaskDto.STATUS_STOP)
      );

    Query query = new Query(timeoutCriteria);
    query.fields().include("id").include("status");
    final List<SubTaskDto> subTaskDtos = clientMongoOperator.find(query, ConnectorConstant.SUB_TASK_COLLECTION, SubTaskDto.class);
    return CollectionUtil.isNotEmpty(subTaskDtos) ? subTaskDtos.get(0) : null;
  }

  private void stopTask(TaskClient<SubTaskDto> subTaskDtoTaskClient) {
    if (subTaskDtoTaskClient == null || subTaskDtoTaskClient.getTask() == null || StringUtils.isBlank(subTaskDtoTaskClient.getTask().getId().toHexString())) {
      return;
    }
    final boolean stop = subTaskDtoTaskClient.stop();
    if (stop) {
      final String taskId = subTaskDtoTaskClient.getTask().getId().toHexString();
      clientMongoOperator.updateById(new Update(), ConnectorConstant.SUB_TASK_COLLECTION + "/stopped", taskId, SubTaskDto.class);
      taskClientMap.remove(taskId);
      destroyCache(subTaskDtoTaskClient);
    }
  }

  private void completeTask(TaskClient<SubTaskDto> subTaskDtoTaskClient) {
    if (subTaskDtoTaskClient == null || subTaskDtoTaskClient.getTask() == null || StringUtils.isBlank(subTaskDtoTaskClient.getTask().getId().toHexString())) {
      return;
    }
    final String taskId = subTaskDtoTaskClient.getTask().getId().toHexString();
    clientMongoOperator.updateById(new Update(), ConnectorConstant.SUB_TASK_COLLECTION + "/complete", taskId, SubTaskDto.class);
    taskClientMap.remove(taskId);
    destroyCache(subTaskDtoTaskClient);
  }

  private void addAgentIdUpdate(Update update) {
    update.set("agentId", instanceNo);
  }

  private void setThreadContext(DataFlow dataFlow) {
    ThreadContext.clearAll();

    ThreadContext.put("userId", dataFlow.getUser_id());
    ThreadContext.put(DebugConstant.SUB_DATAFLOW_ID, dataFlow.getId());
    ThreadContext.put("jobName", dataFlow.getName());
    ThreadContext.put("app", ConnectorConstant.WORKER_TYPE_CONNECTOR);
  }

  private long getJobHeartTimeout() {
    return settingService.getLong("jobHeartTimeout", 60000L);
  }
}
