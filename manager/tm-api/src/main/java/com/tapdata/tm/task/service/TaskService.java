package com.tapdata.tm.task.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.base.service.BaseService;
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
import lombok.NonNull;
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

public abstract class TaskService extends BaseService<TaskDto, TaskEntity, ObjectId, TaskRepository> {
    public TaskService(@NonNull TaskRepository repository) {
        super(repository, TaskDto.class, TaskEntity.class);
    }
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

    public abstract Supplier<TaskDto> dataPermissionFindById(ObjectId taskId, Field fields);

    //@Transactional
    public abstract TaskDto create(TaskDto taskDto, UserDetail user);

    public abstract <T> AggregationResults<T> aggregate(org.springframework.data.mongodb.core.aggregation.Aggregation aggregation, Class<T> outputType);

    //@Transactional
    public abstract TaskDto updateById(TaskDto taskDto, UserDetail user);

    public abstract TaskDto updateAfter(TaskDto taskDto, UserDetail user);

    public abstract TaskDto updateShareCacheTask(String id, SaveShareCacheParam saveShareCacheParam, UserDetail user);

    public abstract void checkTaskName(String newName, UserDetail user, ObjectId id);

    public abstract boolean checkTaskNameNotError(String newName, UserDetail user, ObjectId id);

    public abstract TaskDto confirmStart(TaskDto taskDto, UserDetail user, boolean confirm);

    public abstract TaskDto confirmById(TaskDto taskDto, UserDetail user, boolean confirm);

    public abstract void checkTaskInspectFlag(TaskDto taskDto);

    public abstract TaskDto confirmById(TaskDto taskDto, UserDetail user, boolean confirm, boolean importTask);

    public abstract void checkDagAgentConflict(TaskDto taskDto, UserDetail user, boolean showListMsg);

    public abstract TaskDto remove(ObjectId id, UserDetail user);

    public abstract void afterRemove(TaskDto taskDto, UserDetail user);

    public abstract void deleteShareCache(ObjectId id, UserDetail user);

    public abstract TaskDto copy(ObjectId id, UserDetail user);

    public abstract Page<TaskRunHistoryDto> queryTaskRunHistory(Filter filter, UserDetail user);

    public abstract void renew(ObjectId id, UserDetail user);

    public abstract void renew(ObjectId id, UserDetail user, boolean system);

    public abstract void afterRenew(TaskDto taskDto, UserDetail user);

    public abstract TaskDto checkExistById(ObjectId id, UserDetail user);

    public abstract TaskDto checkExistById(ObjectId id, UserDetail user, String... fields);

    public abstract TaskDto checkExistById(ObjectId id, String... fields);

    public abstract List<MutiResponseMessage> batchStart(List<ObjectId> taskIds, UserDetail user,
                                         HttpServletRequest request, HttpServletResponse response);

    public abstract int subCronOrPlanNum(TaskDto task, int runningNum);

    public abstract boolean checkIsCronOrPlanTask(TaskDto task);

    public abstract List<MutiResponseMessage> batchStop(List<ObjectId> taskIds, UserDetail user,
                                        HttpServletRequest request,
                                        HttpServletResponse response);

    public abstract List<MutiResponseMessage> batchDelete(List<ObjectId> taskIds, UserDetail user,
                                          HttpServletRequest request,
                                          HttpServletResponse response);

    public abstract List<MutiResponseMessage> batchRenew(List<ObjectId> taskIds, UserDetail user,
                                         HttpServletRequest request, HttpServletResponse response);

    public abstract Page<TaskDto> scanTask(Filter filter, UserDetail userDetail);

    public Page<TaskDto> find(Filter filter, UserDetail userDetail){
        return super.find(filter, userDetail);
    }

    public abstract Page<TaskDto> superFind(Filter filter, UserDetail userDetail);

    public abstract void deleteNotifyEnumData(List<TaskDto> taskDtoList);

    public abstract LogCollectorResult searchLogCollector(String key);

    public abstract TaskDto createShareCacheTask(SaveShareCacheParam saveShareCacheParam, UserDetail user,
                                 HttpServletRequest request,
                                 HttpServletResponse response);

    public abstract Page<ShareCacheVo> findShareCache(Filter filter, UserDetail userDetail);

    public abstract ShareCacheDetailVo findShareCacheById(String id);

    public abstract Map<String, Object> chart(UserDetail user);

    public abstract Map<String, Integer> inspectChart(UserDetail user);

    public abstract List<TaskEntity> findByIds(List<ObjectId> idList);

    public abstract TaskDetailVo findTaskDetailById(String id, Field field, UserDetail userDetail);

    public abstract Boolean checkRun(String taskId, UserDetail user);

    public abstract TransformerWsMessageDto findTransformParam(String taskId, UserDetail user);

    public abstract TransformerWsMessageDto findTransformAllParam(String taskId, UserDetail user);

    public abstract TaskDto findByTaskId(ObjectId id, String... fields);

    public abstract void rename(String taskId, String newName, UserDetail user);

    public abstract TaskStatsDto stats(UserDetail userDetail);

    public abstract DataFlowInsightStatisticsDto statsTransport(UserDetail userDetail);

    public abstract DataFlowInsightStatisticsDto statsTransport(UserDetail userDetail, List<LocalDate> localDates);

    public abstract Map<String, List<TaskDto>> getByConIdOfTargetNode(List<String> connectionIds, String status, String position, UserDetail user, int page, int pageSize);

    public abstract long countTaskNumber(UserDetail user);

    public abstract List<SampleTaskVo> findByConId(String sourceConnectionId, String targetConnectionId, String syncType, String status, Where where, UserDetail user);

    public abstract void batchLoadTask(HttpServletResponse response, List<String> taskIds, UserDetail user);

    public abstract void importRmProject(MultipartFile multipartFile, UserDetail user, boolean cover, List<String> tags, String source, String sink) throws IOException;

    public abstract void checkJsProcessorTestRun(UserDetail user, List<TaskDto> tpTasks);

    public abstract void batchUpTask(MultipartFile multipartFile, UserDetail user, boolean cover, List<String> tags);

    public abstract void batchImport(List<TaskDto> taskDtos, UserDetail user, boolean cover, List<String> tags, Map<String, DataSourceConnectionDto> conMap, Map<String, MetadataInstancesDto> metaMap);

    public abstract Criteria parseOrToCriteria(Where where);

    public abstract void getTableDDL(TaskDto taskDto);

    public abstract List<TaskDto> findAllTasksByIds(List<String> list);

    public abstract void renewAgentMeasurement(String taskId);

    public abstract UpdateResult renewNotSendMq(TaskDto taskDto, UserDetail user);

    public abstract boolean findAgent(TaskDto taskDto, UserDetail user);

    public abstract void start(ObjectId id, UserDetail user);

    public abstract void start(ObjectId id, UserDetail user, boolean system);

    public abstract void start(TaskDto taskDto, UserDetail user, String startFlag, boolean system);

    public abstract void start(TaskDto taskDto, UserDetail user, String startFlag);

    public abstract void run(TaskDto taskDto, UserDetail user);

    public abstract void updateTaskRecordStatus(TaskDto dto, String status, UserDetail userDetail);

    public abstract void pause(ObjectId id, UserDetail user, boolean force);

    public abstract void pause(TaskDto TaskDto, UserDetail user, boolean force);

    //@Transactional
    public abstract void pause(ObjectId id, UserDetail user, boolean force, boolean restart);

    public abstract void pause(ObjectId id, UserDetail user, boolean force, boolean restart, boolean system);

    public abstract void pause(TaskDto taskDto, UserDetail user, boolean force, boolean restart);

    public abstract void sendStoppingMsg(String taskId, String agentId, UserDetail user, boolean force);

    public abstract String running(ObjectId id, UserDetail user);

    public abstract String runError(ObjectId id, UserDetail user, String errMsg, String errStack);

    public abstract String complete(ObjectId id, UserDetail user);

    public abstract String stopped(ObjectId id, UserDetail user);

    public abstract RunTimeInfo runtimeInfo(ObjectId id, Long endTime, UserDetail user);

    public abstract void updateNode(ObjectId objectId, String nodeId, Document param, UserDetail user);

    public abstract void updateSyncProgress(ObjectId taskId, Document document);

    public abstract void increaseClear(ObjectId taskId, String srcNode, String tgtNode, UserDetail user);

    public abstract void increaseBacktracking(ObjectId taskId, String srcNode, String tgtNode, TaskDto.SyncPoint point, UserDetail user);


    public abstract void startPlanMigrateDagTask();

    public abstract void startPlanCronTask();

    public abstract TaskDto findByCacheName(String cacheName, UserDetail user);

    public abstract void updateDag(TaskDto taskDto, UserDetail user, boolean saveHistory);

    public abstract TaskDto findByVersionTime(String id, Long time);

    public abstract void clean(String taskId, Long time);

    public abstract Map<String, Object> totalAutoInspectResultsDiffTables(IdParam param);

    public abstract void updateTaskLogSetting(String taskId, LogSettingParam logSettingParam, UserDetail userDetail);

    public abstract Chart6Vo chart6(UserDetail user);

    public abstract void stopTaskIfNeedByAgentId(String agentId, UserDetail userDetail);

    public abstract List<TaskDto> getTaskStatsByTableNameOrConnectionId(String connectionId, String tableName, UserDetail userDetail);

    public abstract TableStatusInfoDto getTableStatus(String connectionId, String tableName, UserDetail userDetail);

    public abstract boolean judgeTargetInspect(String connectionId, String tableName, UserDetail userDetail);

    public abstract boolean judgeTargetNode(TaskDto taskDto, String tableName);

    public abstract List<TaskDto> findHeartbeatByConnectionId(String connectionId, String... includeFields);

    public abstract TaskDto findHeartbeatByTaskId(String taskId, String... includeFields);

    public abstract int deleteHeartbeatByConnId(UserDetail user, String connId);

    public abstract int runningTaskNum(String processId, UserDetail userDetail);

    public abstract TaskEntity convertToEntity(Class entityClass, BaseDto dto, String... ignoreProperties);

    public <T extends BaseDto> T convertToDto(TaskEntity entity, Class<T> dtoClass, String... ignoreProperties){
        return super.convertToDto(entity, dtoClass, ignoreProperties);
    }

    public abstract int findRunningTasksByAgentId(String processId);

    public abstract int runningTaskNum(UserDetail userDetail);

    public abstract boolean checkCloudTaskLimit(ObjectId taskId, UserDetail user, boolean checkCurrentTask);


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