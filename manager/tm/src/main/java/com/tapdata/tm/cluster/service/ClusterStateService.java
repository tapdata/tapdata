package com.tapdata.tm.cluster.service;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.map.MapUtil;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.Settings.constant.SettingsEnum;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.cluster.dto.*;
import com.tapdata.tm.cluster.entity.ClusterStateEntity;
import com.tapdata.tm.cluster.repository.ClusterStateRepository;
import com.tapdata.tm.clusterOperation.constant.AgentStatusEnum;
import com.tapdata.tm.clusterOperation.constant.ClusterOperationTypeEnum;
import com.tapdata.tm.clusterOperation.dto.ClusterOperationDto;
import com.tapdata.tm.clusterOperation.entity.ClusterOperationEntity;
import com.tapdata.tm.clusterOperation.service.ClusterOperationService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.dto.MessageDto;
import com.tapdata.tm.message.service.MessageService;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.tapdata.tm.utils.MongoUtils.toObjectId;

/**
 * @Author:
 * @Date: 2021/09/13
 * @Description:
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class ClusterStateService extends BaseService<ClusterStateDto, ClusterStateEntity, ObjectId, ClusterStateRepository> {

    @Autowired(required = false)
    @Lazy
    WorkerService workerService;

    @Autowired
    ClusterOperationService clusterOperationService;

    @Autowired
    private MessageService messageService;
    private SettingsService settingsService;
    private MongoTemplate mongoTemplate;

    public ClusterStateService(@NonNull ClusterStateRepository repository) {
        super(repository, ClusterStateDto.class, ClusterStateEntity.class);
    }

    protected void beforeSave(ClusterStateDto clusterState, UserDetail user) {

    }


    /**
     * 页面点击自动升级按钮时响应
     *
     * @param param
     * @return
     */
    public String updateAgent(UpdateAgentVersionParam param,UserDetail userDetail) {
        String retResult = "1";
        String processId = param.getProcessId();
        String version = param.getVersion();
        if ((StringUtils.isEmpty(processId))) {
            throw new BizException("Cluster.ProcessId.Null");
        } else if (StringUtils.isEmpty(param.getDownloadUrl())) {
            throw new BizException("Cluster.DownloadUrl.Null");
        }
        //根据processId 查找 clusterState
        Query query = Query.query(Criteria.where("systemInfo.process_id").is(processId));
        List<ClusterStateDto> clusterStateDtoList = findAll(query);
        if (CollectionUtils.isEmpty(clusterStateDtoList)) {
            //如果找不到就更新对应的worker信息
            Query workQuery = Query.query(Criteria.where("process_id").is(processId));
            Update update = Update.update("updateStatus", "fail");
            update.set("updateMsg", "can not find the agent");
            update.set("updateTime", new Date());
            update.set("updateVersion", version);
//            workerService.update(workQuery, update);
            workerService.update(query,update, userDetail);
            retResult = "1";
        } else {
            List<String> downList = Arrays.asList("tapdata", "tapdata.exe", "tapdata-agent", "log4j2.yml");
            clusterStateDtoList.forEach(clusterStateDto -> {
                addNewClusterOperation(clusterStateDto, param, downList);
                updateWorker(processId, param.getVersion(), userDetail);
            });

        }
        return retResult;
    }

    public Long updateStatus(UpdataStatusRequest updataStatusRequest, UserDetail userDetail){
        ClusterOperationDto clusterOperationDto = new ClusterOperationDto();
        clusterOperationDto.setUuid(updataStatusRequest.getUuid());
        clusterOperationDto.setOperationTime(new Date());
        clusterOperationDto.setServer(updataStatusRequest.getServer());
        clusterOperationDto.setOperation(updataStatusRequest.getOperation());
        clusterOperationDto.setStatus(0);
        ClusterOperationDto clusterOper = clusterOperationService.save(clusterOperationDto, userDetail);
        String server = updataStatusRequest.getServer() + "Operation";
        Map<String, Object> data = new HashMap<>();
        data.put("_id", clusterOper.getId());
        data.put("status", 0);
        data.put("msg", "");
        data.put("operation", updataStatusRequest.getOperation());
        UpdateResult updateResult = update(Query.query(Criteria.where("uuid").is(updataStatusRequest.getUuid())), Update.update(server, data));
        return updateResult.getModifiedCount();
    }

    public Integer addMonitor(ClusterStateMonitorRequest addMonitorRequest){

        ClusterStateDto clusterStateDto = findOne(Query.query(Criteria.where("systemInfo.uuid").is(addMonitorRequest.getUuid())));
        if (clusterStateDto == null){
            return 0;
        }
        if (clusterStateDto.getCustomMonitor() == null){
            clusterStateDto.setCustomMonitor(new ArrayList<>());
        }
        CustomMonitorInfo customMonitorInfo = new CustomMonitorInfo();
        customMonitorInfo.setId(new ObjectId());
        customMonitorInfo.setUuid(addMonitorRequest.getUuid());
        customMonitorInfo.setName(addMonitorRequest.getName());
        customMonitorInfo.setCommand(addMonitorRequest.getCommand());
        customMonitorInfo.setArguments(addMonitorRequest.getArguments());
        clusterStateDto.getCustomMonitor().add(customMonitorInfo);
        update(Query.query(Criteria.where("_id").is(clusterStateDto.getId())), clusterStateDto);
        MessageDto messageDto = new MessageDto();
        messageDto.setLevel("info");
        messageDto.setSystem("agent");
        messageDto.setMsg("newSeverCreatedSuccessfully");
        messageDto.setTitle("newSeverCreatedSuccessfully");
        messageDto.setServerName(clusterStateDto.getSystemInfo() != null ? clusterStateDto.getSystemInfo().getIp() : "");
        messageDto.setSourceId(addMonitorRequest.getUuid());
        messageDto.setMonitorName(addMonitorRequest.getName());
        messageService.add(messageDto);
        return 1;
    }

    public Integer editMonitor(ClusterStateMonitorRequest editMonitorRequest){

        ClusterStateDto clusterStateDto = findOne(Query.query(Criteria.where("systemInfo.uuid").is(editMonitorRequest.getUuid())));
        if (clusterStateDto == null){
            return 0;
        }
        if (clusterStateDto.getCustomMonitor() == null){
            clusterStateDto.setCustomMonitor(new ArrayList<>());
        }
        if (clusterStateDto.getCustomMonitorStatus() == null){
            clusterStateDto.setCustomMonitorStatus(new ArrayList<>());
        }
        for (CustomMonitorInfo customMonitorInfo : clusterStateDto.getCustomMonitor()) {
            if (customMonitorInfo != null
                    && editMonitorRequest.getId().equals(customMonitorInfo.getId().toHexString())
                    && editMonitorRequest.getUuid().equals(customMonitorInfo.getUuid())){
                customMonitorInfo.setName(editMonitorRequest.getName());
                customMonitorInfo.setCommand(editMonitorRequest.getCommand());
                customMonitorInfo.setArguments(editMonitorRequest.getArguments());
            }
        }
        for (CustomMonitorInfo customMonitorInfo : clusterStateDto.getCustomMonitorStatus()) {
            if (customMonitorInfo != null
                    && editMonitorRequest.getId().equals(customMonitorInfo.getId().toHexString())
                    && editMonitorRequest.getUuid().equals(customMonitorInfo.getUuid())){
                customMonitorInfo.setName(editMonitorRequest.getName());
                customMonitorInfo.setCommand(editMonitorRequest.getCommand());
                customMonitorInfo.setArguments(editMonitorRequest.getArguments());
            }
        }
        update(Query.query(Criteria.where("_id").is(clusterStateDto.getId())), clusterStateDto);
        return 1;
    }

    public Integer removeMonitor(ClusterStateMonitorRequest removeMonitorRequest){

        ClusterStateDto clusterStateDto = findOne(Query.query(Criteria.where("systemInfo.uuid").is(removeMonitorRequest.getUuid())));
        if (clusterStateDto == null){
            return 0;
        }

        String monitorName = "";
        if (CollectionUtils.isNotEmpty(clusterStateDto.getCustomMonitor())){
            monitorName = clusterStateDto.getCustomMonitor().stream()
                    .filter(customMonitorInfo -> customMonitorInfo.getId() != null
                            && removeMonitorRequest.getId().equals(customMonitorInfo.getId().toHexString()))
                    .findFirst().map(CustomMonitorInfo::getName).orElse("");
        }

        Update update = new Update();
        update.pull("customMonitor", new BasicDBObject("id", toObjectId(removeMonitorRequest.getId())));
        update(Query.query(Criteria.where("systemInfo.uuid").is(removeMonitorRequest.getUuid())), update);

        MessageDto messageDto = new MessageDto();
        messageDto.setLevel("warn");
        messageDto.setSystem("agent");
        messageDto.setMsg("newSeverDeletedSuccessfully");
        messageDto.setTitle("newSeverDeletedSuccessfully");
        messageDto.setServerName(clusterStateDto.getSystemInfo() != null ? clusterStateDto.getSystemInfo().getIp() : "");
        messageDto.setSourceId(removeMonitorRequest.getUuid());
        messageDto.setMonitorName(monitorName);
        messageService.add(messageDto);

        return 1;
    }

    private void addNewClusterOperation(ClusterStateDto clusterStateDto, UpdateAgentVersionParam param, List downloadList) {
        ClusterOperationEntity cluserOperationEntity = new ClusterOperationEntity();
        cluserOperationEntity.setOperationTime(new Date());
        cluserOperationEntity.setType(ClusterOperationTypeEnum.update.toString());
        cluserOperationEntity.setProcess_id(param.getProcessId());
        cluserOperationEntity.setUuid(clusterStateDto.getUuid());
        cluserOperationEntity.setDownloadUrl(param.getDownloadUrl());
        cluserOperationEntity.setToken(param.getToken());
        cluserOperationEntity.setDownloadList(downloadList);
        cluserOperationEntity.setStatus(AgentStatusEnum.NEED_UPDATE.getValue());
        cluserOperationEntity.setCreateAt(new Date());
        cluserOperationEntity.setLastUpdAt(new Date());
        repository.getMongoOperations().insert(cluserOperationEntity);
    }

    private void updateWorker(String processId, String version, UserDetail user) {
        Query query = Query.query(Criteria.where("process_id").is(processId));
        Update update = new Update();
        update.set("updateVersion", version);
        update.set("progres", "");
        update.set("updateTime", new Date());
        update.set("updateStatus", "preparing");
        update.set("updateMsg", "preparing");
        update.set("updatePingTime", new Date().getTime());
        workerService.update(query, update, user);
    }

    /**
     * customMonitorStatus  暂不处理
     *
     * @param map
     */
    public void statusInfo(Map map) {
        Map data = (Map) map.get("data");
        Double reportInterval = (Double) data.get("reportInterval");
        Map systemInfo = (Map) data.get("systemInfo");

        String work_dir= (String) systemInfo.getOrDefault("work_dir","");
        if (work_dir.contains("\\")){
            systemInfo.put("logDir",work_dir+"\\log");
        }
        else if (work_dir.contains("/")){
            systemInfo.put("logDir",work_dir+"/log");
        }
        String uuid = (String) systemInfo.get("uuid");

        Date now = new Date();
        Double newTtl = now.getTime() + reportInterval * 2;

        Query query = Query.query(Criteria.where("systemInfo.uuid").is(uuid));

        ClusterStateEntity clusterStateEntity = BeanUtil.mapToBean(data, ClusterStateEntity.class, false, CopyOptions.create());
        Document doc = new Document();
        repository.getMongoOperations().getConverter().write(clusterStateEntity, doc);
        Update update = Update.fromDocument(doc);
        update.set("status", "running");
        update.set("uuid",uuid);
        update.setOnInsert("insertTime", now);
        update.set("ttl", new Date(newTtl.longValue()));
        update.set("last_updated",new Date());
        log.info("insert ClusterState data:{} ", JSON.toJSONString(update));

        repository.getMongoOperations().upsert(query, update, "ClusterState");

    }


    public void logsFinished(Map map) {
        log.info("in logsFinished map:{}",JSON.toJSONString(map));
        if (null!=map&&null!=map.get("data")){
            Map data= (Map) map.get("data");
            String uuid = MapUtil.getStr(data,"uuid");
            Query query = Query.query(Criteria.where("uuid").is(uuid));
            Update update=new Update();
            update.set("getLogs.server",data.get("server"));
            update.set("getLogs.$.state",2);
            update(query,update);
        }
    }

    /**
     * get simple flow engine info list
     * @return accessNodeInfo list
     */
    public List<AccessNodeInfo> findAccessNodeInfo(UserDetail userDetail) {
        //需要过滤有效的work数据
        List<AccessNodeInfo> result = Lists.newArrayList();
        List<Worker> workerList = workerService.findAllAgent(userDetail);
        if (CollectionUtils.isEmpty(workerList)) {
            return result;
        }

        boolean isCloud = settingsService.isCloud();
        int overTime = SettingsEnum.WORKER_HEART_OVERTIME.getIntValue(30);
        long liveTime = System.currentTimeMillis() - (overTime * 1000L);

        workerList.forEach(worker -> {
            AtomicReference<String> hostname = new AtomicReference<>(worker.getHostname());
            if (isCloud) {
                Optional.ofNullable(worker.getTcmInfo()).ifPresent(tcmInfo -> hostname.set(tcmInfo.getAgentName()));
            }

            String status = "running";
            if (Objects.nonNull(worker.getStopping()) && worker.getStopping()) {
                status = "stopped";
            } else if (Objects.isNull(worker.getPingTime()) || worker.getPingTime() < liveTime) {
                status = "stopped";
            }

            AccessNodeInfo accessNodeInfo = new AccessNodeInfo(worker.getProcessId(), hostname.get(), worker.getProcessId(), status);
            result.add(accessNodeInfo);
        });

        return result;
    }

    public void stopCluster() {
        Query query = Query.query(Criteria.where("status").ne("stopped").and("ttl").lt(new Date()));

        List<ClusterStateDto> list = findAll(query);
        if (CollectionUtils.isEmpty(list)) {
            return;
        }

        Update update = new Update();
        update.set("status", "stopped");
        mongoTemplate.updateFirst(query, update, ClusterStateEntity.class);
    }

    public Page<ClusterStateDto> getAll(Filter filter) {
        Page<ClusterStateDto> page = this.find(filter);

        Optional.ofNullable(page).flatMap(p -> Optional.ofNullable(p.getItems())).ifPresent(items -> {
            List<String> processIds = items.stream().map(n -> n.getSystemInfo().getProcess_id()).collect(Collectors.toList());

            List<String> availableProcessIds = Lists.newArrayList();
            List<Worker> workers = workerService.findAvailableAgentBySystem(processIds);
            Optional.ofNullable(workers).ifPresent(w -> w.forEach(k -> availableProcessIds.add(k.getProcessId())));

            items.forEach(m -> {
                Optional.ofNullable(m.getManagement()).ifPresent(management -> management.setServiceStatus(management.getStatus()));
                Optional.ofNullable(m.getApiServer()).ifPresent(api -> api.setServiceStatus(api.getStatus()));
                Optional.ofNullable(m.getEngine()).ifPresent(fe -> {
                    if (availableProcessIds.contains(m.getSystemInfo().getProcess_id())) {
                        fe.setServiceStatus(fe.getStatus());
                    } else {
                        fe.setServiceStatus("stopped");
                    }

                });
            });
        });
        return page;
    }
}
