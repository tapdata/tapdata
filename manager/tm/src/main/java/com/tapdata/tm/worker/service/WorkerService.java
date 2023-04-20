package com.tapdata.tm.worker.service;

import cn.hutool.core.map.MapUtil;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.constant.SettingsEnum;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.cluster.dto.ClusterStateDto;
import com.tapdata.tm.cluster.dto.SystemInfo;
import com.tapdata.tm.cluster.dto.UpdataStatusRequest;
import com.tapdata.tm.cluster.service.ClusterStateService;
import com.tapdata.tm.commons.base.dto.SchedulableDto;
import com.tapdata.tm.commons.task.dto.ParentTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dataflow.dto.DataFlowDto;
import com.tapdata.tm.dataflow.service.DataFlowService;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.scheduleTasks.dto.ScheduleTasksDto;
import com.tapdata.tm.scheduleTasks.service.ScheduleTasksService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.constant.Operation;
import com.tapdata.tm.userLog.service.UserLogService;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.worker.WorkerSingletonLock;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.dto.WorkerProcessInfoDto;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.repository.WorkerRepository;
import com.tapdata.tm.worker.vo.ApiWorkerStatusVo;
import com.tapdata.tm.worker.vo.CalculationEngineVo;
import io.firedome.MultiTaggedCounter;
import io.micrometer.core.instrument.Metrics;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.BsonValue;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/9/10 下午4:30
 * @description
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class WorkerService extends BaseService<WorkerDto, Worker, ObjectId, WorkerRepository> {

    private DataFlowService dataFlowService;
    @Autowired
    private ClusterStateService clusterStateService;
    @Autowired
    private UserLogService userLogService;
    @Autowired
    private SettingsService settingsService;
    @Autowired
    private ScheduleTasksService scheduleTasksService;

    @Autowired
    private TaskService taskService;

    private final MultiTaggedCounter workerPing;



    public WorkerService(@NonNull WorkerRepository repository) {
        super(repository, WorkerDto.class, Worker.class);

        workerPing = new MultiTaggedCounter("worker_ping", Metrics.globalRegistry, "worker_type", "version");
    }

    @Override
    protected void beforeSave(WorkerDto dto, UserDetail userDetail) {
        if (dto.getIsDeleted() != null && dto.getIsDeleted()) {
            dto.setPingTime(0L);
        } else if (dto.getStopping() != null && dto.getStopping()) {
            dto.setPingTime(System.currentTimeMillis() - 1000 * 60 * 5);
        }
    }

    public List<Worker> findAvailableAgent(UserDetail userDetail) {
        if (Objects.isNull(userDetail)) {
            return null;
        }
        // 引擎定时任务是5秒
        Query query = getAvailableAgentQuery();
        return repository.findAll(query, userDetail);
    }

    public List<Worker> findAllAgent(UserDetail userDetail) {
        if (Objects.isNull(userDetail)) {
            return null;
        }
        Criteria criteria = Criteria.where("worker_type").is("connector")
                .and("isDeleted").ne(true);
        return repository.findAll(Query.query(criteria), userDetail);
    }

    @NotNull
    private Query getAvailableAgentQuery() {
        return Query.query(getAvailableAgentCriteria());
    }

    public boolean isAgentTimeout(Long pingTime) {
        if (null != pingTime) {
            int overTime = SettingsEnum.WORKER_HEART_OVERTIME.getIntValue(30);
            return pingTime <= System.currentTimeMillis() - (overTime * 1000L);
        }
        return true;
    }

    private Criteria getAvailableAgentCriteria() {
        int overTime = SettingsEnum.WORKER_HEART_OVERTIME.getIntValue(30);
        Criteria criteria = Criteria.where("worker_type").is("connector")
                .and("ping_time").gte(System.currentTimeMillis() - (overTime * 1000L))
                .and("isDeleted").ne(true).and("stopping").ne(true)
                .and("agentTags").ne("disabledScheduleTask");
        return criteria;
    }

    public List<Worker> findAvailableAgentBySystem(UserDetail user) {
        Query query = getAvailableAgentQuery();
        return repository.findAll(query, user);
    }

    public List<Worker> findAvailableAgentBySystem(List<String> processIdList) {
        Query query = getAvailableAgentQuery();
        if (CollectionUtils.isNotEmpty(processIdList)) {
            query.addCriteria(Criteria.where("process_id").in(processIdList));
        }
        return repository.findAll(query);
    }

    public List<Worker> findAvailableAgentByAccessNode(UserDetail userDetail, List<String> processIdList) {
        if (Objects.isNull(userDetail)) {
            return null;
        }
        Query query = getAvailableAgentQuery();
        if (CollectionUtils.isNotEmpty(processIdList)) {
            query.addCriteria(Criteria.where("process_id").in(processIdList));
        }
        return repository.findAll(query, userDetail);
    }

    @Override
    public Page<WorkerDto> find(Filter filter) {

        Page<WorkerDto> page = super.find(filter);
        long now = new Date().getTime();
        List<ObjectId> runningJobs = new ArrayList<>();
        page.getItems().forEach(workerDto -> {
            workerDto.setServerDate(now);
            if (workerDto.getRunningJobs() != null && workerDto.getRunningJobs().size() > 0) {
                workerDto.getRunningJobs().forEach(id -> runningJobs.add(new ObjectId(id)));
            }
        });
        Query query = Query.query(Criteria.where("id").in(runningJobs));
        query.fields().include("id", "name");
        List<DataFlowDto> list = dataFlowService.findAll(query);

        page.getItems().forEach(workerDto -> {
            List<DataFlowDto> selfRunningJobs = new ArrayList<>();
            for (int i = 0; i < list.size(); i++) {
                if (workerDto.getRunningJobs().contains(list.get(0).getId().toHexString())) {
                    selfRunningJobs.add(list.remove(i));
                    i--;
                }
            }
            //TODO这里还是dataflow, 需要兼容Task格式的
            workerDto.setJobs(selfRunningJobs);
        });

        return page;
    }

    public WorkerDto createWorker(WorkerDto workerDto, UserDetail loginUser) {

        if (StringUtils.isBlank(workerDto.getProcessId())) {
            workerDto.setProcessId(generatorProcessId());
        }

        workerDto.setExternalUserId(workerDto.getUserId());
        workerDto.setUserId(loginUser.getUserId());
        workerDto.setAccessCode(loginUser.getAccessCode());
        workerDto.setWorkerType("connector");

        WorkerDto transformer = new WorkerDto();
        BeanUtils.copyProperties(workerDto, transformer);

        transformer.setWorkerType("transformer");

        save(workerDto, loginUser);
        save(transformer, loginUser);

        workerDto.setTmUserId(loginUser.getUserId());

        try {
            String agentName = "";
            if (workerDto.getTcmInfo() != null && StringUtils.isNotBlank(workerDto.getTcmInfo().getAgentName())) {
                agentName = workerDto.getTcmInfo().getAgentName();
            }
            userLogService.addUserLog(Modular.AGENT, Operation.CREATE, loginUser, agentName);
        } catch (Exception e) {
            // Ignore record agent operation log error
            log.error("Record create worker operation fail", e);
        }

        return workerDto;
    }

    private String generatorProcessId() {
        return new ObjectId().toHexString() + '-' + BigInteger.valueOf(new Date().getTime()).toString(32);
    }

    public Map<String, WorkerProcessInfoDto> getProcessInfo(List<String> ids) {

        Map<String, WorkerProcessInfoDto> result = new HashMap<>();
        List<WorkerDto> workers = findAll(Query.query(Criteria.where("workerType").is("connector").and("process_id").in(ids)));

        workers.forEach(worker -> {

            Query query = Query.query(Criteria.where("agentId").is(worker.getProcessId()).and("status").is(TaskDto.STATUS_RUNNING));
            query.fields().include("id", "name", "syncType");
            //List<DataFlowDto> dataFlows = dataFlowService.findAll(query);
            List<TaskDto> tasks = taskService.findAll(query);

            query = Query.query(Criteria.where("systemInfo.process_id").is(worker.getProcessId()));
            query.with(Sort.by(Sort.Order.desc("lastUpdAt"))).limit(1);
            List<ClusterStateDto> clusterState = clusterStateService.findAll(query);
            SystemInfo systemInfo = clusterState != null && clusterState.size() > 0 ? clusterState.get(0).getSystemInfo() : null;

            WorkerProcessInfoDto workerProcessInfo = new WorkerProcessInfoDto();
            workerProcessInfo.setRunningNum(tasks.size());
            Map<String, Long> groupBySyncType = tasks.stream().collect(Collectors.groupingBy(ParentTaskDto::getSyncType, Collectors.counting()));
            workerProcessInfo.setRunningTaskNum(groupBySyncType);
            workerProcessInfo.setDataFlows(tasks.stream().map(task -> {
                DataFlowDto dataFlow = new DataFlowDto();
                dataFlow.setId(task.getId());
                dataFlow.setName(task.getName());
                return dataFlow;
            }).collect(Collectors.toList()));
            workerProcessInfo.setSystemInfo(systemInfo);

            result.put(worker.getProcessId(), workerProcessInfo);
        });

        return result;
    }

    /**
     * 调度任务到合适的实例上
     * <p>
     * 用户可以启动多个实例，这些实例会运行在不同地域，并且会有不同的特性支持
     * 需要在实例上执行任务时，需要通过此方法筛选出合适的实例执行任务。
     * <p>
     * 支持DRS场景（地域/可用区、有无互联网访问、单向/双向过滤）、DFS场景（托管实例、本地自建实例）
     * 支持资源池同地域多个实例的负载调度
     *
     */
    public CalculationEngineVo scheduleTaskToEngine(SchedulableDto entity, UserDetail userDetail, String type, String name) throws BizException {

        CalculationEngineVo calculationEngineVo = calculationEngine(entity, userDetail, type);
        String processId = calculationEngineVo.getProcessId();
        String filter = calculationEngineVo.getFilter();
        ArrayList<BasicDBObject> threadLog = calculationEngineVo.getThreadLog();

        ScheduleTasksDto scheduleTasksDto = new ScheduleTasksDto();
        scheduleTasksDto.setTask_name("TM_SCHEDULE");
        scheduleTasksDto.setType(type);
        scheduleTasksDto.setPeriod(0L);
        scheduleTasksDto.setStatus("done");
        scheduleTasksDto.setTask_name(name);
        scheduleTasksDto.setTask_profile("DEFAULT");
        scheduleTasksDto.setAgent_id(processId);
        scheduleTasksDto.setLast_updated(new Date());
        scheduleTasksDto.setPing_time(System.currentTimeMillis());
        scheduleTasksDto.setFilter(filter);
        scheduleTasksDto.setThread(threadLog);
        scheduleTasksService.save(scheduleTasksDto, userDetail);

        return calculationEngineVo;
    }

    private CalculationEngineVo calculationEngine(SchedulableDto entity, UserDetail userDetail, String type) {
        CalculationEngineVo calculationEngineVo = new CalculationEngineVo();
        String filter;
        int availableNum;

        Object jobHeartTimeout = settingsService.getByCategoryAndKey(CategoryEnum.WORKER, KeyEnum.WORKER_HEART_TIMEOUT).getValue();
        Object buildProfile = settingsService.getByCategoryAndKey(CategoryEnum.SYSTEM, KeyEnum.BUILD_PROFILE).getValue();
        boolean isCloud = buildProfile.equals("CLOUD") || buildProfile.equals("DRS") || buildProfile.equals("DFS");
        if ((userDetail.getUserId() == null || userDetail.getUserId().equals("")) && isCloud) {
            throw new BizException("NotFoundUserId");
        }
        Long findTime = 0L;

        // 53迭代Task上增加了指定Flow Engine的功能 --start
        String agentId = entity.getAgentId();
        if (StringUtils.isNotBlank(agentId)) {
            Criteria where = Criteria.where("worker_type").is("connector")
                    .and("ping_time").gte(findTime)
                    .and("isDeleted").ne(true)
                    .and("stopping").ne(true)
                    .and("process_id").is(agentId);
            WorkerDto worker = findOne(Query.query(where));

            int num = taskService.runningTaskNum(agentId, userDetail);
            if (Objects.nonNull(worker)) {
                if (worker.getLimitTaskNum() > num || !type.equals("task")) {
                    calculationEngineVo.setProcessId(agentId);
                    calculationEngineVo.setManually(true);

                    calculationEngineVo.setFilter(where.toString());
                    ArrayList<BasicDBObject> threadLog = new ArrayList<>();
                    threadLog.add(new BasicDBObject()
                            .append("process_id", worker.getProcessId())
                            .append("weight", worker.getWeight())
                            .append("running_thread", worker.getRunningThread())
                    );
                    calculationEngineVo.setThreadLog(threadLog);

                    entity.setAgentId(calculationEngineVo.getProcessId());
                    entity.setScheduleTime(System.currentTimeMillis());
                    return calculationEngineVo;
                }
            }
        }
        // 53迭代Task上增加了指定Flow Engine的功能 --end
        BasicDBObject thread = new BasicDBObject();
        //优先调度到用户自己的agent上。用于内部测试使用。不要修改此逻辑！！！
        //和steven确认了 之前指定agent的逻辑是 work中process_id存的是entity.getUserId()
        Criteria whereSelf = Criteria.where("worker_type").is("connector")
                .and("ping_time").gte(findTime)
                .and("isDeleted").ne(true)
                .and("stopping").ne(true)
                .and("agentTags").ne("disabledScheduleTask")
                .and("process_id").is(entity.getUserId());
        Worker selfWorkers = repository.findOne(Query.query(whereSelf)).orElse(null);
        ArrayList<BasicDBObject> threadLog = new ArrayList<>();
        if(selfWorkers != null){
            filter = whereSelf.toString();
            availableNum = 1;

            thread.append("process_id",selfWorkers.getProcessId());
            threadLog.add(new BasicDBObject()
                    .append("process_id", selfWorkers.getProcessId())
                    .append("weight", selfWorkers.getWeight())
                    .append("running_thread", selfWorkers.getRunningThread())
            );
        } else {
            Criteria where = Criteria.where("worker_type").is("connector")
                    .and("ping_time").gte(findTime)
                    .and("isDeleted").ne(true)
                    .and("stopping").ne(true);
            if (isCloud) {
                where.and("user_id").is(userDetail.getUserId());
            }

            if (isCloud && entity.getAgentTags() != null && entity.getAgentTags().size() > 0) {
                List<String> agentTags = new ArrayList<>();
                for (int i = 0; i < entity.getAgentTags().size(); i++) {
                    String s = entity.getAgentTags().get(i);
                    if (!s.equals("unidirectional")) {
                        agentTags.add(s);
                    }
                }
                if (CollectionUtils.isNotEmpty(agentTags)) {
                    where.and("agentTags").all(agentTags);
                } else {
                    where.and("agentTags").ne("disabledScheduleTask");
                }
            } else {
                where.and("agentTags").ne("disabledScheduleTask");
            }

            Query query = Query.query(where);
            List<WorkerDto> workers = findAllDto(query, userDetail);
            int workerNum = 0;
            for (int i = 0; i < workers.size(); i++) {
                WorkerDto worker = workers.get(i);
                if (worker.getWeight() == null) {
                    worker.setWeight(1);
                }
                long num = taskService.runningTaskNum(worker.getProcessId(), userDetail);
                if (worker.getLimitTaskNum() > num || !type.equals("task")) {
                    workerNum++;

                    worker.setRunningThread((int) num);
                    threadLog.add(new BasicDBObject()
                            .append("process_id", worker.getProcessId())
                            .append("weight", worker.getWeight())
                            .append("running_thread", worker.getRunningThread())
                    );
                    float load = (float) (worker.getRunningThread() / worker.getWeight());
                    if (i == 0 && thread.get("load") == null) {
                        thread.append("load", load);
                        thread.append("process_id", worker.getProcessId());
                    } else if (thread.get("load") != null && load < (float) thread.get("load")) {
                        thread.append("load", load);
                        thread.append("process_id", worker.getProcessId());
                    }
                }
            }

            if (StringUtils.isBlank((String) thread.get("process_id"))) {
                if (workerNum < workers.size()) {
                    calculationEngineVo.setTaskAvailable(workerNum);
                }
            }

            filter = where.toString();
            availableNum = workers.size();
        }


        String processId = (String) thread.get("process_id");

        entity.setAgentId(processId);
        entity.setScheduleTime(System.currentTimeMillis());

        calculationEngineVo.setProcessId(processId);
        calculationEngineVo.setFilter(filter);
        calculationEngineVo.setThreadLog(threadLog);
        calculationEngineVo.setAvailable(availableNum);
        calculationEngineVo.setManually(false);

        return calculationEngineVo;
    }

    public void scheduleTaskToEngine(InspectDto inspectDto, UserDetail userDetail) throws BizException {
        scheduleTaskToEngine(inspectDto, userDetail, "Inspect", inspectDto.getName());
    }


    public void scheduleTaskToEngine(DataFlowDto dataFlowDto, UserDetail userDetail) throws BizException {
        scheduleTaskToEngine(dataFlowDto, userDetail, "Inspect", dataFlowDto.getName());
    }

    @Deprecated
    public void scheduleTaskToEngine(SchedulableDto schedulableDto, UserDetail userDetail) throws BizException {
        List<Worker> list = findAvailableAgent(userDetail);

        if (list != null && list.size() > 0) {
            Worker worker = list.get(0);

            // 设置调度结果
            schedulableDto.setAgentId(worker.getProcessId());
            schedulableDto.setScheduleTime(new Date().getTime());
        } else {
            throw new BizException("NotFoundAvailableEngine", "xxxx");
        }
    }

    public void cleanWorkers() {
        Long jobHeartTimeout = 120000L;
        Long nowMillSeconds = new Date().getTime();
        List<String> statusList = Arrays.asList("preparing", "downloading", "upgrading");

        Query query = Query.query(Criteria.where("updatePingTime").lt((nowMillSeconds - jobHeartTimeout)));
        query.addCriteria(Criteria.where("updateStatus").in(statusList));
        Update update = new Update();
        update.set("updateStatus", "fail");
        update.set("updateMsg", "time out");
        UpdateResult updateResult = update(query, update);

        BsonValue upsertedId = updateResult.getUpsertedId();
        log.info("clean worker :{}", updateResult.getModifiedCount());
    }


    /**
     * tapdata engine 调用该方法，更新worker版本信息
     *
     * @param map
     */
    public void updateMsg(Map map) {
        log.info("agent 版本 更新中map:{}", JSON.toJSONString(map));
        Map data = (Map) map.get("data");
        if (null != data) {
            String processId = MapUtil.getStr(data, "process_id");
            if (StringUtils.isNotBlank(processId)) {
                String progress = MapUtil.getStr(data, "progres");
                String status = MapUtil.getStr(data, "status");
                String msg = MapUtil.getStr(data, "msg");
                Date now = new Date();

                log.info("begin update...progres:{},status:{}", progress, status);

                Query workerQuery = Query.query(Criteria.where("process_id").is(processId));
                Update workUpdate = new Update();
                workUpdate.set("updateStatus", status);
                workUpdate.set("updateMsg", msg);
                workUpdate.set("progres", progress);
                workUpdate.set("updateTime", now);
                workUpdate.set("updatePingTime", now.getTime());
//                update(workerQuery, workUpdate);
                repository.getMongoOperations().updateMulti(workerQuery, workUpdate, Worker.class);

            }
        }
    }


    public Page<ApiWorkerStatusVo> findApiWorkerStatus(UserDetail userDetail) {
        Query query = Query.query(Criteria.where("worker_type").is("api-server"));
        query.addCriteria(Criteria.where("ping_time").gte(System.currentTimeMillis() - 30000L));
        query.addCriteria(Criteria.where("user_id").is(userDetail.getUserId()));
        WorkerDto worker = findOne(query);
        log.info("  findApiWorkerStatus  getMongoOperations find :{}", JsonUtil.toJson(worker));
        List<ApiWorkerStatusVo> list = Lists.newArrayList();
        if (null != worker) {
            ApiWorkerStatusVo workerStatusVo = new ApiWorkerStatusVo();
            workerStatusVo.setWorkerStatus(worker.getWorker_status());
            workerStatusVo.setServerDate(System.currentTimeMillis());
            list.add(workerStatusVo);
        } else {
            log.error("找不到woerker22222222，userDetail：{}", JSON.toJSONString(userDetail));
        }
        Page<ApiWorkerStatusVo> page=new Page<ApiWorkerStatusVo>();
        page.setItems(list);
        return page;

    }

    public void updateAll(Query query, Update update) {
        repository.getMongoOperations().updateMulti(query, update, Worker.class);
    }

    /**
     * 客户端上报心跳信息, 这个方法将会根据 process_id 和 worker_type 执行 upsert 操作
     * @param worker worker
     * @param loginUser loginUser
     * @return WorkerDto
     */
    public WorkerDto health(WorkerDto worker, UserDetail loginUser) {

        if (worker == null) {
            throw new BizException("IllegalArgument", "Parameter worker can't be empty.");
        }

        if (worker.getProcessId() == null || worker.getWorkerType() == null) {
            throw new BizException("IllegalArgument", "processId or workerType can't be empty.");
        }

        worker.setPingTime(System.currentTimeMillis());
        Object buildProfile = settingsService.getByCategoryAndKey(CategoryEnum.SYSTEM, KeyEnum.BUILD_PROFILE).getValue();
        boolean isCloud = buildProfile.equals("CLOUD") || buildProfile.equals("DRS") || buildProfile.equals("DFS");
        Criteria where = Criteria.where("process_id").is(worker.getProcessId()).and("worker_type").is(worker.getWorkerType());
        if (!WorkerSingletonLock.checkDBTag(worker.getSingletonLock(), worker.getWorkerType(), isCloud, () -> Optional
                .of(Query.query(where))
                .map(query -> {
                    query.fields().include("singletonLock", "ping_time");
                    return query;
                }).flatMap(query -> repository.findOne(query).map(w -> {
                    // 如果超时，返回当前 worker tag 表示可以启动
                    if (isAgentTimeout(w.getPingTime())) {
                        return null;
                    }
                    return w.getSingletonLock();
                }))
                .orElse(worker.getSingletonLock())
        )) {
            return null;
        }

        repository.upsert(Query.query(where),
                convertToEntity(Worker.class, worker), loginUser
        );
        workerPing.increment(worker.getWorkerType(), worker.getVersion());
        Query query = Query.query(where);
        query.fields().include("process_id", "worker_type", "isDeleted");
        Optional<Worker> workerOptional = repository.findOne(query);
        workerOptional.ifPresent(w -> {
            if (w.getIsDeleted() != null && w.getIsDeleted()) {
                worker.setIsDeleted(true);
            }
        });
        return worker;
    }

    public TaskDto setHostName(TaskDto dto) {
        String agentId = dto.getAgentId();
        Query query = new Query(Criteria.where("process_id").is(agentId));
        WorkerDto one = findOne(query);
        if (Objects.nonNull(one)) {
            dto.setHostName(one.getHostname());
            dto.setAgentName(one.getHostname());

            if (settingsService.isCloud()) {
                Optional.ofNullable(one.getTcmInfo()).ifPresent(info -> {
                    dto.setAgentName(info.getAgentName());
                });
            }
        }
        return dto;
    }

    public String checkTaskUsedAgent(String taskId, UserDetail user) {
        TaskDto taskDto = taskService.checkExistById(MongoUtils.toObjectId(taskId), user, "agentId");
        return checkUsedAgent(taskDto.getAgentId(), user);
    }
    public String checkUsedAgent(String processId, UserDetail user) {
        Criteria availableAgentCriteria = Criteria.where("worker_type").is("connector")
                .and("stopping").ne(true);
        availableAgentCriteria.and("process_id").is(processId);
        Query query = new Query(availableAgentCriteria);
        WorkerDto workerDto = findOne(query, user);
        if (workerDto == null || (workerDto.getDeleted() != null && workerDto.getDeleted())
                || (workerDto.getIsDeleted() != null && workerDto.getIsDeleted())) {
            return "deleted";
        }
        long outTime = System.currentTimeMillis() - 1000 * 5 * 2;
        Long pingTime = workerDto.getPingTime();
        if (pingTime != null && pingTime < outTime) {
            return "offline";
        }

        return "online";

    }

    public void sendStopWorkWs(String processId, UserDetail userDetail) {
        UpdataStatusRequest updataStatusRequest = new UpdataStatusRequest();
        ClusterStateDto clusterStateDto = clusterStateService.findOne(Query.query(Criteria.where("systemInfo.process_id").
                is(processId).and("status").is("running")));
        updataStatusRequest.setUuid(clusterStateDto.getUuid());
        updataStatusRequest.setOperation("stop");
        updataStatusRequest.setServer("backend");
        clusterStateService.updateStatus(updataStatusRequest, userDetail);
    }


    public WorkerDto findByProcessId(String processId, UserDetail userDetail, String... fields) {
        Criteria criteria = Criteria.where("process_id").is(processId);
        Query query = new Query(criteria);
        if (fields != null && fields.length != 0) {
            query.fields().include(fields);
        }
        return findOne(query, userDetail);
    }
}
