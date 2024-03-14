package com.tapdata.tm.task.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.base.service.IBaseService;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.TransformerWsMessageDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.TaskRunHistoryDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dataflowinsight.dto.DataFlowInsightStatisticsDto;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.monitor.param.IdParam;
import com.tapdata.tm.task.bean.*;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.param.LogSettingParam;
import com.tapdata.tm.task.param.SaveShareCacheParam;
import com.tapdata.tm.task.repository.TaskRepository;
import com.tapdata.tm.task.vo.ShareCacheDetailVo;
import com.tapdata.tm.task.vo.ShareCacheVo;
import com.tapdata.tm.task.vo.TaskDetailVo;
import com.tapdata.tm.task.vo.TaskStatsDto;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public interface TaskService extends IBaseService<TaskDto, TaskEntity, ObjectId, TaskRepository> {
    static String printInfos(DAG dag) {
        try {
            StringBuilder sb = new StringBuilder();
            List<Edge> edges = dag.getEdges();
            for (Edge edge : edges) {
                sb.append(dag.getNode(edge.getSource()).getName()).append(" -> ").append(dag.getNode(edge.getTarget()).getName()).append("\r\n");
            }
            return sb.toString();
        } catch (Exception e) {
            return "";
        }
    }

    Supplier<TaskDto> dataPermissionFindById(ObjectId taskId, Field fields);

    //@Transactional
    TaskDto create(TaskDto taskDto, UserDetail user);

    <T> AggregationResults<T> aggregate(org.springframework.data.mongodb.core.aggregation.Aggregation aggregation, Class<T> outputType);

    //@Transactional
    TaskDto updateById(TaskDto taskDto, UserDetail user);

    TaskDto updateAfter(TaskDto taskDto, UserDetail user);

    TaskDto updateShareCacheTask(String id, SaveShareCacheParam saveShareCacheParam, UserDetail user);

    void checkTaskName(String newName, UserDetail user, ObjectId id);

    boolean checkTaskNameNotError(String newName, UserDetail user, ObjectId id);

    TaskDto confirmStart(TaskDto taskDto, UserDetail user, boolean confirm);

    TaskDto confirmById(TaskDto taskDto, UserDetail user, boolean confirm);

    void checkTaskInspectFlag(TaskDto taskDto);

    TaskDto confirmById(TaskDto taskDto, UserDetail user, boolean confirm, boolean importTask);

    void checkDagAgentConflict(TaskDto taskDto, boolean showListMsg);

    TaskDto remove(ObjectId id, UserDetail user);

    void afterRemove(TaskDto taskDto, UserDetail user);

    void deleteShareCache(ObjectId id, UserDetail user);

    TaskDto copy(ObjectId id, UserDetail user);

    Page<TaskRunHistoryDto> queryTaskRunHistory(Filter filter, UserDetail user);

    void renew(ObjectId id, UserDetail user);

    void renew(ObjectId id, UserDetail user, boolean system);

    void afterRenew(TaskDto taskDto, UserDetail user);

    TaskDto checkExistById(ObjectId id, UserDetail user);

    TaskDto checkExistById(ObjectId id, UserDetail user, String... fields);

    TaskDto checkExistById(ObjectId id, String... fields);

    List<MutiResponseMessage> batchStart(List<ObjectId> taskIds, UserDetail user,
                                         HttpServletRequest request, HttpServletResponse response);

    int subCronOrPlanNum(TaskDto task, int runningNum);

    boolean checkIsCronOrPlanTask(TaskDto task);

    List<MutiResponseMessage> batchStop(List<ObjectId> taskIds, UserDetail user,
                                        HttpServletRequest request,
                                        HttpServletResponse response);

    List<MutiResponseMessage> batchDelete(List<ObjectId> taskIds, UserDetail user,
                                          HttpServletRequest request,
                                          HttpServletResponse response);

    List<MutiResponseMessage> batchRenew(List<ObjectId> taskIds, UserDetail user,
                                         HttpServletRequest request, HttpServletResponse response);

    Page<TaskDto> scanTask(Filter filter, UserDetail userDetail);

    Page<TaskDto> find(Filter filter, UserDetail userDetail);

    Page<TaskDto> superFind(Filter filter, UserDetail userDetail);

    void deleteNotifyEnumData(List<TaskDto> taskDtoList);

    LogCollectorResult searchLogCollector(String key);

    TaskDto createShareCacheTask(SaveShareCacheParam saveShareCacheParam, UserDetail user,
                                 HttpServletRequest request,
                                 HttpServletResponse response);

    Page<ShareCacheVo> findShareCache(Filter filter, UserDetail userDetail);

    ShareCacheDetailVo findShareCacheById(String id);

    Map<String, Object> chart(UserDetail user);

    Map<String, Integer> inspectChart(UserDetail user);

    List<TaskEntity> findByIds(List<ObjectId> idList);

    TaskDetailVo findTaskDetailById(String id, Field field, UserDetail userDetail);

    Boolean checkRun(String taskId, UserDetail user);

    TransformerWsMessageDto findTransformParam(String taskId, UserDetail user);

    TransformerWsMessageDto findTransformAllParam(String taskId, UserDetail user);

    TaskDto findByTaskId(ObjectId id, String... fields);

    void rename(String taskId, String newName, UserDetail user);

    TaskStatsDto stats(UserDetail userDetail);

    DataFlowInsightStatisticsDto statsTransport(UserDetail userDetail);

    DataFlowInsightStatisticsDto statsTransport(UserDetail userDetail, List<LocalDate> localDates);

    Map<String, List<TaskDto>> getByConIdOfTargetNode(List<String> connectionIds, String status, String position, UserDetail user, int page, int pageSize);

    long countTaskNumber(UserDetail user);

    List<SampleTaskVo> findByConId(String sourceConnectionId, String targetConnectionId, String syncType, String status, Where where, UserDetail user);

    void batchLoadTask(HttpServletResponse response, List<String> taskIds, UserDetail user);

    void importRmProject(MultipartFile multipartFile, UserDetail user, boolean cover, List<String> tags, String source, String sink) throws IOException;

    void checkJsProcessorTestRun(UserDetail user, List<TaskDto> tpTasks);

    void batchUpTask(MultipartFile multipartFile, UserDetail user, boolean cover, List<String> tags);

    void batchImport(List<TaskDto> taskDtos, UserDetail user, boolean cover, List<String> tags, Map<String, DataSourceConnectionDto> conMap, Map<String, MetadataInstancesDto> metaMap);

    Criteria parseOrToCriteria(Where where);

    void getTableDDL(TaskDto taskDto);

    List<TaskDto> findAllTasksByIds(List<String> list);

    void renewAgentMeasurement(String taskId);

    UpdateResult renewNotSendMq(TaskDto taskDto, UserDetail user);

    boolean findAgent(TaskDto taskDto, UserDetail user);

    void start(ObjectId id, UserDetail user);

    void start(ObjectId id, UserDetail user, boolean system);

    void start(TaskDto taskDto, UserDetail user, String startFlag, boolean system);

    void start(TaskDto taskDto, UserDetail user, String startFlag);

    void run(TaskDto taskDto, UserDetail user);

    void updateTaskRecordStatus(TaskDto dto, String status, UserDetail userDetail);

    void pause(ObjectId id, UserDetail user, boolean force);

    void pause(TaskDto TaskDto, UserDetail user, boolean force);

    //@Transactional
    void pause(ObjectId id, UserDetail user, boolean force, boolean restart);

    void pause(ObjectId id, UserDetail user, boolean force, boolean restart, boolean system);

    void pause(TaskDto taskDto, UserDetail user, boolean force, boolean restart);

    void sendStoppingMsg(String taskId, String agentId, UserDetail user, boolean force);

    String running(ObjectId id, UserDetail user);

    String runError(ObjectId id, UserDetail user, String errMsg, String errStack);

    String complete(ObjectId id, UserDetail user);

    String stopped(ObjectId id, UserDetail user);

    RunTimeInfo runtimeInfo(ObjectId id, Long endTime, UserDetail user);

    void updateNode(ObjectId objectId, String nodeId, Document param, UserDetail user);

    void updateSyncProgress(ObjectId taskId, Document document);

    void increaseClear(ObjectId taskId, String srcNode, String tgtNode, UserDetail user);

    void increaseBacktracking(ObjectId taskId, String srcNode, String tgtNode, TaskDto.SyncPoint point, UserDetail user);

    boolean checkPdkTask(TaskDto taskDto, UserDetail user);

    void startPlanMigrateDagTask();

    void startPlanCronTask();

    TaskDto findByCacheName(String cacheName, UserDetail user);

    void updateDag(TaskDto taskDto, UserDetail user, boolean saveHistory);

    TaskDto findByVersionTime(String id, Long time);

    void clean(String taskId, Long time);

    Map<String, Object> totalAutoInspectResultsDiffTables(IdParam param);

    void updateTaskLogSetting(String taskId, LogSettingParam logSettingParam, UserDetail userDetail);

    Chart6Vo chart6(UserDetail user);

    void stopTaskIfNeedByAgentId(String agentId, UserDetail userDetail);

    List<TaskDto> getTaskStatsByTableNameOrConnectionId(String connectionId, String tableName, UserDetail userDetail);

    TableStatusInfoDto getTableStatus(String connectionId, String tableName, UserDetail userDetail);

    boolean judgeTargetInspect(String connectionId, String tableName, UserDetail userDetail);

    boolean judgeTargetNode(TaskDto taskDto, String tableName);

    List<TaskDto> findHeartbeatByConnectionId(String connectionId, String... includeFields);

    TaskDto findHeartbeatByTaskId(String taskId, String... includeFields);

    int deleteHeartbeatByConnId(UserDetail user, String connId);

    int runningTaskNum(String processId, UserDetail userDetail);

    TaskEntity convertToEntity(Class entityClass, BaseDto dto, String... ignoreProperties);

    <T extends BaseDto> T convertToDto(TaskEntity entity, Class<T> dtoClass, String... ignoreProperties);

    int findRunningTasksByAgentId(String processId);

    int runningTaskNum(UserDetail userDetail);

    boolean checkCloudTaskLimit(ObjectId taskId, UserDetail user, boolean checkCurrentTask);

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Char1Group {
        private String _id;
        private long count;
    }

    @Data
    public static class Char2Group {
        private long totalInput = 0;
        private long totalOutput = 0;
        private long totalInputDataSize = 0;
        private long totalOutputDataSize = 0;
        private long totalInsert = 0;
        private long totalInsertSize = 0;
        private long totalUpdate = 0;
        private long totalUpdateSize = 0;
        private long totalDelete = 0;
        private long totalDeleteSize = 0;
    }
}