package com.tapdata.tm.worker.service;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.IBaseService;
import com.tapdata.tm.commons.base.dto.SchedulableDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dataflow.dto.DataFlowDto;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.dto.WorkerExpireDto;
import com.tapdata.tm.worker.dto.WorkerProcessInfoDto;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.repository.WorkerRepository;
import com.tapdata.tm.worker.vo.ApiWorkerStatusVo;
import com.tapdata.tm.worker.vo.CalculationEngineVo;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.List;
import java.util.Map;

public interface WorkerService extends IBaseService<WorkerDto, Worker, ObjectId, WorkerRepository> {
    List<Worker> findAvailableAgent(UserDetail userDetail);

    List<Worker> findAllAgent(UserDetail userDetail);

    boolean isAgentTimeout(Long pingTime);

    List<Worker> findAvailableAgentBySystem(UserDetail userDetail);

    List<Worker> findAvailableAgentBySystem(List<String> processIdList);

    List<Worker> findAvailableAgentByAccessNode(UserDetail userDetail, List<String> processIdList);

    Page<WorkerDto> find(Filter filter);

    WorkerDto createWorker(WorkerDto workerDto, UserDetail loginUser);

    Map<String, WorkerProcessInfoDto> getProcessInfo(List<String> ids, UserDetail userDetail);

    CalculationEngineVo scheduleTaskToEngine(SchedulableDto entity, UserDetail userDetail, String type, String name) throws BizException;

    CalculationEngineVo calculationEngine(SchedulableDto entity, UserDetail userDetail, String type);

    void scheduleTaskToEngine(InspectDto inspectDto, UserDetail userDetail) throws BizException;

    void scheduleTaskToEngine(DataFlowDto dataFlowDto, UserDetail userDetail) throws BizException;

    @Deprecated
    void scheduleTaskToEngine(SchedulableDto schedulableDto, UserDetail userDetail) throws BizException;

    void cleanWorkers();

    void updateMsg(Map map);

    Page<ApiWorkerStatusVo> findApiWorkerStatus(UserDetail userDetail);

    void updateAll(Query query, Update update);

    WorkerDto health(WorkerDto worker, UserDetail loginUser);

    TaskDto setHostName(TaskDto dto);

    String checkTaskUsedAgent(String taskId, UserDetail user);

    String checkUsedAgent(String processId, UserDetail user);

    void sendStopWorkWs(String processId, UserDetail userDetail);

    WorkerDto findByProcessId(String processId, UserDetail userDetail, String... fields);

    void createShareWorker(WorkerExpireDto workerExpireDto, UserDetail loginUser);

    WorkerExpireDto getShareWorker(UserDetail userDetail);

    void checkWorkerExpire();

    int getLimitTaskNum(WorkerDto workerDto, UserDetail user);

    void deleteShareWorker(UserDetail loginUser);

    WorkerDto queryWorkerByProcessId(String processId);

    List<Worker> queryAllBindWorker();

    boolean bindByProcessId(WorkerDto workerDto, String processId, UserDetail userDetail);

    boolean unbindByProcessId(String processId);

    Long getAvailableAgentCount();

    Long getLastCheckAvailableAgentCount();
}
