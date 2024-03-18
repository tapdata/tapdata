package com.tapdata.tm.worker.service;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
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
import lombok.NonNull;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.List;
import java.util.Map;

public abstract class WorkerService extends BaseService<WorkerDto, Worker, ObjectId, WorkerRepository> {
    public WorkerService(@NonNull WorkerRepository repository) {
        super(repository, WorkerDto.class, Worker.class);
    }
    public abstract List<Worker> findAvailableAgent(UserDetail userDetail);

    public abstract List<Worker> findAllAgent(UserDetail userDetail);

    public abstract boolean isAgentTimeout(Long pingTime);

    public abstract List<Worker> findAvailableAgentBySystem(UserDetail userDetail);

    public abstract List<Worker> findAvailableAgentBySystem(List<String> processIdList);

    public abstract List<Worker> findAvailableAgentByAccessNode(UserDetail userDetail, List<String> processIdList);

    public Page<WorkerDto> find(Filter filter){
        return super.find(filter);
    }

    public abstract WorkerDto createWorker(WorkerDto workerDto, UserDetail loginUser);

    public abstract Map<String, WorkerProcessInfoDto> getProcessInfo(List<String> ids, UserDetail userDetail);

    public abstract CalculationEngineVo scheduleTaskToEngine(SchedulableDto entity, UserDetail userDetail, String type, String name) throws BizException;

    public abstract CalculationEngineVo calculationEngine(SchedulableDto entity, UserDetail userDetail, String type);

    public abstract void scheduleTaskToEngine(InspectDto inspectDto, UserDetail userDetail) throws BizException;

    public abstract void scheduleTaskToEngine(DataFlowDto dataFlowDto, UserDetail userDetail) throws BizException;

    @Deprecated
    public abstract void scheduleTaskToEngine(SchedulableDto schedulableDto, UserDetail userDetail) throws BizException;

    public abstract void cleanWorkers();

    public abstract void updateMsg(Map map);

    public abstract Page<ApiWorkerStatusVo> findApiWorkerStatus(UserDetail userDetail);

    public abstract void updateAll(Query query, Update update);

    public abstract WorkerDto health(WorkerDto worker, UserDetail loginUser);

    public abstract TaskDto setHostName(TaskDto dto);

    public abstract String checkTaskUsedAgent(String taskId, UserDetail user);

    public abstract String checkUsedAgent(String processId, UserDetail user);

    public abstract void sendStopWorkWs(String processId, UserDetail userDetail);

    public abstract WorkerDto findByProcessId(String processId, UserDetail userDetail, String... fields);

    public abstract void createShareWorker(WorkerExpireDto workerExpireDto, UserDetail loginUser);

    public abstract WorkerExpireDto getShareWorker(UserDetail userDetail);

    public abstract void checkWorkerExpire();

    public abstract int getLimitTaskNum(WorkerDto workerDto, UserDetail user);

    public abstract void deleteShareWorker(UserDetail loginUser);

    public abstract WorkerDto queryWorkerByProcessId(String processId);

    public abstract List<Worker> queryAllBindWorker();

    public abstract boolean bindByProcessId(WorkerDto workerDto, String processId, UserDetail userDetail);

    public abstract boolean unbindByProcessId(String processId);

    public abstract Long getAvailableAgentCount();

    public abstract Long getLastCheckAvailableAgentCount();
}
