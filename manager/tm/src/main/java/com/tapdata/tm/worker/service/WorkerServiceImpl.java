package com.tapdata.tm.worker.service;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.lang.Assert;
import cn.hutool.core.map.MapUtil;
import com.alibaba.fastjson.JSON;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
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
import com.tapdata.tm.task.service.TaskExtendService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.constant.Operation;
import com.tapdata.tm.userLog.service.UserLogService;
import com.tapdata.tm.utils.EngineVersionUtil;
import com.tapdata.tm.utils.FunctionUtils;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.worker.WorkerSingletonLock;
import com.tapdata.tm.worker.dto.*;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.entity.WorkerExpire;
import com.tapdata.tm.worker.repository.WorkerRepository;
import com.tapdata.tm.worker.vo.ApiWorkerStatusVo;
import com.tapdata.tm.worker.vo.CalculationEngineVo;
import io.firedome.MultiTaggedCounter;
import io.micrometer.core.instrument.Metrics;
import io.tapdata.pdk.core.utils.CommonUtils;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/9/10 下午4:30
 * @description
 */
@Service
@Slf4j
public class WorkerServiceImpl extends WorkerService{

    @Autowired
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
    @Autowired
    private TaskExtendService taskExtendService;
    @Autowired
    private UserService userService;
    @Autowired
    private MongoTemplate mongoTemplate;

    private final MultiTaggedCounter workerPing;
    private Random random = new Random();

    public WorkerServiceImpl(@NonNull WorkerRepository repository) {
        super(repository);
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

        Criteria criteria = getAvailableAgentCriteria();
        return getWorkers(userDetail, criteria);
    }

    private List<Worker> getWorkers(UserDetail userDetail, Criteria criteria) {
        if (settingsService.isCloud()) {
            // query user have share worker
            WorkerExpire workerExpire = mongoTemplate.findOne(Query.query(Criteria.where("userId").is(userDetail.getUserId())), WorkerExpire.class);
            if (Objects.nonNull(workerExpire) && workerExpire.getExpireTime().after(new Date())) {
                criteria.orOperator(Criteria.where("user_id").is(userDetail.getUserId()), Criteria.where("createUser").is(workerExpire.getShareUser()));
                return repository.findAll(Query.query(criteria));
            } else {
                return repository.findAll(Query.query(criteria), userDetail);
            }
        } else {
            return repository.findAll(Query.query(criteria), userDetail);
        }
    }

    public List<Worker> findAllAgent(UserDetail userDetail) {
        if (Objects.isNull(userDetail)) {
            return null;
        }
        Criteria criteria = Criteria.where("worker_type").is("connector")
                .and("isDeleted").ne(true);

        return getWorkers(userDetail, criteria);
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
        return Criteria.where("worker_type").is("connector")
                .and("ping_time").gte(System.currentTimeMillis() - (overTime * 1000L))
                .and("isDeleted").ne(true).and("stopping").ne(true)
                .and("agentTags").ne("disabledScheduleTask");
    }

    public List<Worker> findAvailableAgentBySystem(UserDetail userDetail) {
        Criteria criteria = getAvailableAgentCriteria();
        return getWorkers(userDetail, criteria);
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
        Criteria criteria = getAvailableAgentCriteria();
        if (CollectionUtils.isNotEmpty(processIdList)) {
            criteria.and("process_id").in(processIdList);
        }

        return getWorkers(userDetail, criteria);
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

    public Map<String, WorkerProcessInfoDto> getProcessInfo(List<String> ids, UserDetail userDetail) {

        Map<String, WorkerProcessInfoDto> result = new HashMap<>();
        List<WorkerDto> workers = findAll(Query.query(Criteria.where("workerType").is("connector").and("process_id").in(ids)));

        workers.forEach(worker -> {

            // query task : 1.status is running 2.crontabExpressionFlag is true
            Criteria criteria = Criteria.where("agentId").is(worker.getProcessId())
                    .and("is_deleted").ne(true)
                    .and("user_id").is(userDetail.getUserId())
                    .and("status").nin(TaskDto.STATUS_DELETE_FAILED,TaskDto.STATUS_DELETING)
                    .orOperator(Criteria.where("status").in(TaskDto.STATUS_RUNNING, TaskDto.STATUS_SCHEDULING, TaskDto.STATUS_WAIT_RUN),
                     Criteria.where("crontabExpressionFlag").is(true),
                     Criteria.where("planStartDateFlag").is(true));
            Query query = Query.query(criteria);
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
        scheduleTasksDto.setThread(calculationEngineVo.getThreadLog());
        scheduleTasksService.save(scheduleTasksDto, userDetail);

        return calculationEngineVo;
    }

    public CalculationEngineVo calculationEngine(SchedulableDto entity, UserDetail userDetail, String type) {
        CalculationEngineVo calculationEngineVo = new CalculationEngineVo();
        String filter;
        int availableNum;
        int taskLimit = 0;
        int runningNum = 0;

        ArrayList<WorkSchedule> threadLog = new ArrayList<>();

        AtomicReference<String> scheduleAgentId = new AtomicReference<>("");

        Object jobHeartTimeout = settingsService.getByCategoryAndKey(CategoryEnum.WORKER, KeyEnum.WORKER_HEART_TIMEOUT).getValue();
        boolean isCloud = settingsService.isCloud();
        if ((userDetail.getUserId() == null || userDetail.getUserId().equals("")) && isCloud) {
            throw new BizException("NotFoundUserId");
        }
        Long findTime = System.currentTimeMillis() - Long.parseLong((String) jobHeartTimeout) * 1000L;

        // 53迭代Task上增加了指定Flow Engine的功能 --start
        String agentId = entity.getAgentId();
        if (StringUtils.isNotBlank(agentId)) {
            Criteria where = Criteria.where("worker_type").is("connector")
                    .and("ping_time").gte(findTime)
                    .and("isDeleted").ne(true)
                    .and("stopping").ne(true)
                    .and("process_id").is(agentId);
            WorkerDto worker = findOne(Query.query(where));

            runningNum = taskService.runningTaskNum(agentId, userDetail);
            if (Objects.nonNull(worker)) {
                availableNum = 1;
                taskLimit = getLimitTaskNum(worker, userDetail);

                calculationEngineVo.setProcessId(agentId);
                scheduleAgentId.set(agentId);
                calculationEngineVo.setManually(true);

                calculationEngineVo.setFilter(where.toString());

                WorkSchedule workSchedule = new WorkSchedule();
                workSchedule.setProcessId(worker.getProcessId());
                workSchedule.setWeight(worker.getWeight());
                workSchedule.setTaskRunNum(runningNum);
                workSchedule.setTaskLimit(taskLimit);
                threadLog.add(workSchedule);
                calculationEngineVo.setThreadLog(threadLog);

                entity.setAgentId(calculationEngineVo.getProcessId());
                entity.setScheduleTime(System.currentTimeMillis());

                calculationEngineVo.setAvailable(availableNum);
                calculationEngineVo.setTaskLimit(taskLimit);
                calculationEngineVo.setRunningNum(runningNum);
                calculationEngineVo.setTotalLimit(taskLimit);
                return calculationEngineVo;
            }
        }
        // 53迭代Task上增加了指定Flow Engine的功能 --end

        Criteria where = Criteria.where("worker_type").is("connector")
                .and("ping_time").gte(findTime)
                .and("isDeleted").ne(true)
                .and("stopping").ne(true);
        if (isCloud) {
            // query user have share worker
            WorkerExpire workerExpire = mongoTemplate.findOne(Query.query(Criteria.where("userId").is(userDetail.getUserId())), WorkerExpire.class);
            if (Objects.nonNull(workerExpire) && workerExpire.getExpireTime().after(new Date())) {
                where.and("user_id").in(userDetail.getUserId(), workerExpire.getShareTmUserId());
            } else {
                where.and("user_id").is(userDetail.getUserId());
            }
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
        List<WorkerDto> workers = findAll(query);
        if (CollectionUtils.isEmpty(workers)) {
            throw new BizException("Task.AgentNotFound");
        }
        availableNum = workers.size();

        AtomicInteger scheduleWeight = new AtomicInteger();
        AtomicInteger scheduleRunNum = new AtomicInteger();
        AtomicInteger scheduleTaskLimit = new AtomicInteger();
        AtomicInteger totalTaskLimit = new AtomicInteger();

        for (int i = 0; i < workers.size(); i++) {
            WorkerDto worker = workers.get(i);
            FunctionUtils.isTureOrFalse(worker.getUserId().equals(userDetail.getUserId())).trueOrFalseHandle(() -> worker.setWeight(99), () -> worker.setWeight(1));

            String processId = worker.getProcessId();
            runningNum = taskService.runningTaskNum(processId, userDetail);
            taskLimit = getLimitTaskNum(worker, userDetail);
            Integer weight = worker.getWeight();
            totalTaskLimit.addAndGet(taskLimit);
            if (isCloud && runningNum > taskLimit) {
                continue;
            }

            WorkSchedule workSchedule = new WorkSchedule();
            workSchedule.setProcessId(processId);
            workSchedule.setWeight(weight);
            workSchedule.setTaskRunNum(runningNum);
            workSchedule.setTaskLimit(taskLimit);
            threadLog.add(workSchedule);


            if (i == 0 || workSchedule.getProcessId() == null) {
                scheduleAgentId.set(processId);
                scheduleWeight.set(weight);
                scheduleRunNum.set(runningNum);
                scheduleTaskLimit.set(taskLimit);
            } else if (worker.getWeight() > scheduleWeight.get()) {
                scheduleAgentId.set(processId);
                scheduleWeight.set(weight);
                scheduleRunNum.set(runningNum);
                scheduleTaskLimit.set(taskLimit);
            } else if (worker.getWeight().equals(scheduleWeight.get()) &&  (taskLimit - runningNum) > (scheduleTaskLimit.get() - scheduleRunNum.get())) {
                scheduleAgentId.set(processId);
                scheduleWeight.set(weight);
                scheduleRunNum.set(runningNum);
                scheduleTaskLimit.set(taskLimit);
            }
        }
        int totalRunningNum = taskService.runningTaskNum(userDetail);
        filter = where.toString();
        String processId = scheduleAgentId.get();

        entity.setAgentId(processId);
        entity.setScheduleTime(System.currentTimeMillis());

        calculationEngineVo.setProcessId(processId);
        calculationEngineVo.setFilter(filter);
        calculationEngineVo.setThreadLog(threadLog);
        calculationEngineVo.setAvailable(availableNum);
        calculationEngineVo.setManually(false);
        int totalTask = totalTaskLimit.get() < 0 ? Integer.MAX_VALUE : totalTaskLimit.get();
        calculationEngineVo.setTaskLimit(totalTask);
        calculationEngineVo.setRunningNum(totalRunningNum);
        calculationEngineVo.setTotalLimit(totalTask);
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
//        if(worker.getPingTime()!=null&&worker.getPingTime()==1){
//            log.info("The engine {} has stopped",worker.getProcessId());
//        }else{
            worker.setPingTime(System.currentTimeMillis());
//        }
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
        Worker worker = getWorkers(userDetail, criteria).stream().findFirst().orElse(null);
        return BeanUtil.copyProperties(worker, WorkerDto.class);
    }

    public void createShareWorker(WorkerExpireDto workerExpireDto, UserDetail loginUser) {
        String userId = workerExpireDto.getUserId();
        if (StringUtils.isBlank(userId)) {
            throw new BizException("SHARE_AGENT_USER_ID_IS_NULL", "userId is null");
        }

        // check user is existed
        WorkerExpire one = mongoTemplate.findOne(Query.query(Criteria.where("userId").is(userId)), WorkerExpire.class);
        if (Objects.nonNull(one)) {
            throw new BizException("SHARE_AGENT_USER_EXISTED", "have applied for a public agent");
        }

        Object shareAgentDaysValue = settingsService.getValueByCategoryAndKey(CategoryEnum.SYSTEM, KeyEnum.SHARE_AGENT_EXPRIRE_DAYS);
        int shareAgentDays = Integer.parseInt(shareAgentDaysValue.toString());

        Object shareAgentUserValue = settingsService.getValueByCategoryAndKey(CategoryEnum.SYSTEM, KeyEnum.SHARE_AGENT_CREATE_USER);
        if (Objects.isNull(shareAgentUserValue)) {
            throw new BizException("SHARE_AGENT_CREATE_USER_NOT_FOUND", "Shared Agent user not found");
        }
        String[] shareAgentUser = shareAgentUserValue.toString().split(",");

        // get share agent user by random
        String shareAgentUserRandom = shareAgentUser[random.nextInt(shareAgentUser.length)];

        WorkerExpire workerExpire = new WorkerExpire();
        workerExpire.setUserId(loginUser.getUserId());
        workerExpire.setShareUser(shareAgentUserRandom);
        workerExpire.setCreateUser(loginUser.getUsername());
        Date date = new Date();
        workerExpire.setCreateAt(date);
        workerExpire.setLastUpdAt(date);
        UserDetail userDetail = userService.loadUserByUsername(shareAgentUserRandom);
        Assert.notNull(userDetail, "userDetail is null");
        workerExpire.setShareTmUserId(userDetail.getUserId());
        workerExpire.setShareTcmUserId(userDetail.getTcmUserId());
        workerExpire.setExpireTime(DateUtil.offsetDay(date, shareAgentDays));
        workerExpire.setSubscribeId(workerExpireDto.getSubscribeId());

        mongoTemplate.insert(workerExpire);
    }

    public WorkerExpireDto getShareWorker(UserDetail userDetail) {
        WorkerExpire workerExpire = mongoTemplate.findOne(Query.query(Criteria.where("userId").is(userDetail.getUserId())), WorkerExpire.class);
        if (Objects.nonNull(workerExpire)) {
            return BeanUtil.copyProperties(workerExpire, WorkerExpireDto.class);
        }

        return null;
    }

    public void checkWorkerExpire() {
        Date date = new Date();
        Criteria expireTime = Criteria.where("expireTime").lt(date).gt(DateUtil.offsetHour(date, -1));
        List<WorkerExpire> workerExpires = mongoTemplate.find(Query.query(expireTime), WorkerExpire.class);
        if (CollectionUtils.isNotEmpty(workerExpires)) {
            workerExpires.forEach(workerExpire -> {
                // query worker by shareUser
                List<WorkerDto> shareWorkers = findAll(Query.query(Criteria.where("user_id").is(workerExpire.getShareTmUserId())));
                shareWorkers.forEach(workerDto -> {
                    String processId = workerDto.getProcessId();
                    CommonUtils.ignoreAnyError(() -> taskExtendService.stopTaskByAgentIdAndUserId(processId, workerExpire.getUserId()), "TM");
                });
            });
        }
    }

    public int getLimitTaskNum(WorkerDto workerDto, UserDetail user) {
        // DAAS out of control
        if (!settingsService.isCloud()) return Integer.MAX_VALUE;

        if (workerDto == null || workerDto.getProcessId() == null) {
            return -1;
        }

        // query by public agent -- start
        if (!user.getUserId().equals(workerDto.getUserId())) {
            return 2;
        }
        // query by public agent -- end

        // query limit by tags -- start
        if (CollectionUtils.isEmpty(workerDto.getAgentTags())) {
            return Integer.MAX_VALUE;
        }

        String limitString = null;
        for (String agentTag : workerDto.getAgentTags()) {
            if (agentTag.startsWith("limitScheduleTask")) {
                limitString = agentTag;
                break;
            }
        }

        if (org.apache.commons.lang3.StringUtils.isBlank(limitString)) {
            return Integer.MAX_VALUE;
        }

        List<String> list = Splitter.on(':')
                .trimResults()
                .omitEmptyStrings()
                .splitToList(limitString);

        if (list.size() < 2) {
            return Integer.MAX_VALUE;
        }

        int limit = Integer.MAX_VALUE;
        try {
            limit = Integer.parseInt(list.get(1));
        } catch (Exception ignore) {
        }

        return limit;
        // query limit by tags -- end
    }

    public void deleteShareWorker(UserDetail loginUser) {
        Query query = Query.query(Criteria.where("userId").is(loginUser.getUserId()));
        Date date = new Date();
        Update expireTime = Update.update("expireTime", date).set("is_deleted", true).set("last_updated", date);
        mongoTemplate.updateFirst(query, expireTime, WorkerExpire.class);
    }
    public WorkerDto queryWorkerByProcessId(String processId){
        if (StringUtils.isBlank(processId)) throw new IllegalArgumentException("process id can not be empty");
        Query query = Query.query(Criteria.where("process_id").is(processId).and("worker_type").is("connector"));
        return findOne(query);
    }
    public List<Worker> queryAllBindWorker(){
        Query query = Query.query(Criteria.where("worker_type").is("connector").and("licenseBind").is(true));
        return repository.findAll(query);
    }
    public boolean bindByProcessId(WorkerDto workerDto, String processId, UserDetail userDetail){
        if (StringUtils.isBlank(processId)) throw new IllegalArgumentException("process id can not be empty");
        //if not exist
        WorkerDto res = queryWorkerByProcessId(processId);
        if (null == res){
            save(workerDto, userDetail);
        }
        Query query = Query.query(Criteria.where("process_id").is(processId).and("worker_type").is("connector"));
        Update update = Update.update("licenseBind", true);
        UpdateResult result = repository.update(query, update);
        if (null == result) return false;
        return result.getModifiedCount() == 1 ? true : false;
    }
    public boolean unbindByProcessId(String processId){
        if (StringUtils.isBlank(processId)) throw new IllegalArgumentException("process id can not be empty");
        Query query = Query.query(Criteria.where("process_id").is(processId).and("worker_type").is("connector"));
        Update update = Update.update("licenseBind", false);
        UpdateResult result = repository.update(query, update);
        if (null == result) return false;
        return result.getModifiedCount() == 1 ? true : false;
    }

    public Long getAvailableAgentCount(){
        return count(getAvailableAgentQuery());
    }

    public Long getLastCheckAvailableAgentCount(){
        int overTime = SettingsEnum.WORKER_HEART_OVERTIME.getIntValue(30) + 120;
        Criteria criteria = Criteria.where("worker_type").is("connector")
                .and("ping_time").gte(System.currentTimeMillis() - (overTime * 1000L))
                .and("isDeleted").ne(true)
                .and("agentTags").ne("disabledScheduleTask");
        return count(Query.query(criteria));
    }

    public String getWorkerCurrentTime(UserDetail userDetail){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        List<Worker> workerList = findAvailableAgent(userDetail);
        if(CollectionUtils.isNotEmpty(workerList)){
            Worker worker = workerList.get(0);
            if( null != worker.getWorkerDate()){
                return simpleDateFormat.format(worker.getWorkerDate());
            }
        }
        return simpleDateFormat.format(new Date());
    }

    public Boolean checkEngineVersion(UserDetail userDetail){
        boolean isCloud = settingsService.isCloud();
        if(isCloud){
            List<Worker> workers = findAvailableAgent(userDetail);
            if(CollectionUtils.isEmpty(workers)) return false;
            List<Worker> oldWorkers = workers.stream().filter(worker -> StringUtils.isBlank(worker.getVersion())
                    || !EngineVersionUtil.checkEngineTransFormSchema(worker.getVersion())).collect(Collectors.toList());
            return CollectionUtils.isEmpty(oldWorkers);
        }else{
            return true;
        }
    }
}
