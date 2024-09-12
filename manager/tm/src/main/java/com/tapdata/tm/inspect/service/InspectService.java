package com.tapdata.tm.inspect.service;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.lang.Assert;
import cn.hutool.extra.cglib.CglibUtil;
import com.google.common.collect.Maps;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.service.AlarmSettingService;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.alarm.constant.AlarmComponentEnum;
import com.tapdata.tm.alarm.constant.AlarmStatusEnum;
import com.tapdata.tm.alarm.constant.AlarmTypeEnum;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.bean.PlatformInfo;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.constant.AlarmSettingTypeEnum;
import com.tapdata.tm.commons.task.constant.NotifyEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingVO;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dataflow.dto.DataFlowDto;
import com.tapdata.tm.dataflow.service.DataFlowService;
import com.tapdata.tm.inspect.bean.*;
import com.tapdata.tm.inspect.constant.InspectMethod;
import com.tapdata.tm.inspect.constant.InspectResultEnum;
import com.tapdata.tm.inspect.constant.InspectStatusEnum;
import com.tapdata.tm.inspect.constant.Mode;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.inspect.dto.InspectResultDto;
import com.tapdata.tm.inspect.entity.InspectEntity;
import com.tapdata.tm.inspect.repository.InspectRepository;
import com.tapdata.tm.inspect.vo.InspectDetailVo;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.message.constant.MsgTypeEnum;
import com.tapdata.tm.message.service.MessageService;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.task.constant.SyncType;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.constant.Operation;
import com.tapdata.tm.userLog.service.UserLogService;
import com.tapdata.tm.utils.*;
import com.tapdata.tm.worker.service.WorkerService;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @Author:
 * @Date: 2021/09/14
 * @Description:
 */
@Service
@Slf4j
public class InspectService extends BaseService<InspectDto, InspectEntity, ObjectId, InspectRepository> {
    @Autowired
    private MessageService messageService;
    @Autowired
    private SettingsService settingsService;
    @Autowired
    private UserLogService userLogService;
    @Autowired
    private InspectDetailsService inspectDetailsService;
    @Autowired
    private InspectResultService inspectResultService;
    @Autowired
    private MessageQueueService messageQueueService;
    @Autowired
    private WorkerService workerService;
    @Autowired
    private AlarmSettingService alarmSettingService;
    @Autowired
    private AlarmService alarmService;
    private final MetadataInstancesService metadataInstancesService;

    public InspectService(@NonNull InspectRepository repository, MetadataInstancesService metadataInstancesService) {
        super(repository, InspectDto.class, InspectEntity.class);
        this.metadataInstancesService = metadataInstancesService;
    }

    protected void beforeSave(InspectDto inspect, UserDetail user) {
        if (InspectStatusEnum.WAITING.getValue().equals(inspect.getStatus())) {
            inspect.setScheduleTimes(0);
        }

        List<String> tags = new ArrayList<>();
        if (inspect.getPlatformInfo() != null) {
            PlatformInfo platformInfo = inspect.getPlatformInfo();
            if (platformInfo.getRegion() != null) {
                tags.add(platformInfo.getRegion());
            }

            if (platformInfo.getZone() != null) {
                tags.add(platformInfo.getZone());
            }

            if (platformInfo.getAgentType() != null) {
                tags.add(platformInfo.getAgentType());
            }

            if (platformInfo.getIsThrough() != null && platformInfo.getIsThrough()) {
                tags.add("internet");
            }
            inspect.setAgentTags(tags);
        }


        if (StringUtils.isNotBlank(inspect.getFlowId())) {
            Criteria criteria = Criteria.where("id").is(MongoUtils.toObjectId(inspect.getFlowId()));
            Query query = new Query(criteria);
            query.fields().include("agentTags");
            DataFlowService dataFlowService = SpringContextHelper.getBean(DataFlowService.class);
            DataFlowDto dataFlowDto = dataFlowService.findOne(query, user);
            if (dataFlowDto != null) {
                if (CollectionUtils.isNotEmpty(dataFlowDto.getAgentTags())) {
                    inspect.setAgentTags(dataFlowDto.getAgentTags());
                }
            }
        }
    }


    /**
     * @param id
     * @param user
     * @return
     */
    @Transactional
    public Map<String, Long> delete(String id, UserDetail user) {
        deleteLogicsById(id);

        //移除定时器
        CronUtil.removeJob(id);
        long detailNum = 0;
        long resultNum = 0;
        Criteria criteria = Criteria.where("inspect_id").is(id);
        detailNum = inspectDetailsService.deleteAll(new Query(criteria));
        resultNum = inspectResultService.deleteAll(new Query(criteria));
        Map<String, Long> result = new HashMap<>();
        result.put("detailNum", detailNum);
        result.put("resultNum", resultNum);

        //add message
        InspectEntity inspectEntity = repository.getMongoOperations().findById(MongoUtils.toObjectId(id), InspectEntity.class);
        if (null != inspectEntity) {
            messageService.addInspect(inspectEntity.getName(), id, MsgTypeEnum.DELETED, Level.INFO, user);
        }
        return result;
    }


    private void joinResult(List<InspectDto> inspectDtos) {
        List<String> inspectIdList = inspectDtos.stream().map(InspectDto::getId).map(ObjectId::toString).collect(Collectors.toList());
        List<InspectResultDto> inspectResultDtoList = inspectResultService.findAll(Query.query(Criteria.where("inspect_id").in(inspectIdList)));

        Map<String, List<InspectResultDto>> inspectIdToInspectResult = inspectResultDtoList.stream().collect(Collectors.groupingBy(InspectResultDto::getInspect_id));

        for (InspectDto inspectDto : inspectDtos) {
//            InspectResultDto inspectResultDto = inspectResultService.getLatestInspectResult(inspectDto.getId());
            String inspectId = inspectDto.getId().toString();
            List<InspectResultDto> singleResultList = inspectIdToInspectResult.getOrDefault(inspectId, new ArrayList<InspectResultDto>());
            if (CollectionUtils.isNotEmpty(singleResultList)) {
                Comparator<InspectResultDto> nameComparator = Comparator.comparing(InspectResultDto::getCreateAt).reversed();
                singleResultList.sort(nameComparator);

                InspectResultDto latestResult=singleResultList.get(0);
                Pair<Integer, String> pair = ImmutablePair.of(0, InspectResultDto.RESULT_PASSED);
                pair = joinResult(inspectDto, latestResult);
                latestResult.setSourceTotal(latestResult.getFirstSourceTotal());
                inspectDto.setLastStartTime(inspectDto.getScheduleTime());
                inspectDto.setInspectResult(latestResult);
                inspectDto.setDifferenceNumber(pair.getKey());
                inspectDto.setResult(pair.getValue());
            } else {
                log.error("inspectDto  为：{}  还没有inspectResult", inspectDto);
            }
        }
    }


    public static Pair<Integer, String> joinResult(InspectDto inspectDto, InspectResultDto inspectResultDto) {
        int differenceNumber = 0;
        String inspectResultResult = InspectResultDto.RESULT_PASSED;
        if (inspectDto == null) {
            return ImmutablePair.of(differenceNumber, inspectResultResult);
        }
        boolean add = StringUtils.isNotBlank(inspectDto.getInspectMethod()) && !"row_count".equals(inspectDto.getInspectMethod());

        if (CollectionUtils.isNotEmpty(inspectResultDto.getStats())) {
            for (Stats stat : inspectResultDto.getStats()) {
                if (!InspectResultDto.RESULT_PASSED.equals(stat.getResult())) {
                    inspectResultResult = InspectResultDto.RESULT_FAILED;
                }

                if (stat.getSourceOnly() == null) {
                    stat.setSourceOnly(0L);
                }

                if (stat.getTargetOnly() == null) {
                    stat.setTargetOnly(0L);
                }

                if (stat.getRowFailed() == null) {
                    stat.setRowFailed(0L);
                }

                if (add) {
                    Long sourceOnly = stat.getSourceOnly() == null ? (0L) : stat.getSourceOnly();
                    Long targetOnly = stat.getTargetOnly() == null ? (0L) : stat.getTargetOnly();
                    Long rowFailed = stat.getRowFailed() == null ? (0L) : stat.getRowFailed();
                    differenceNumber += (sourceOnly + targetOnly + rowFailed);
                }

            }
        }
        return ImmutablePair.of(differenceNumber, inspectResultResult);
    }


    public Page<InspectDto> list(Filter filter, UserDetail userDetail) {
        Where where = filter.getWhere();
        Map notDeleteMap = new HashMap();
        notDeleteMap.put("$ne", true);
        where.put("is_deleted", notDeleteMap);
        if (!userDetail.isRoot() && !userDetail.isFreeAuth()) {
            where.put("user_id", userDetail.getUserId());
        }
        Page<InspectDto> page = find(filter);

        List<InspectDto> inspectDtoList = page.getItems();
        if (CollectionUtils.isNotEmpty(inspectDtoList)) {
            joinResult(inspectDtoList);
        }

//        List<InspectListVo> inspectListVoList = com.tapdata.tm.utils.BeanUtil.deepCloneList(inspectDtoList, InspectListVo.class);
//        page.setItems(inspectListVoList);
        return page;
    }

    public InspectDto findById(Filter filter, UserDetail userDetail) {
        InspectDto inspectDto = super.findOne(filter, userDetail);
        Criteria criteria = Criteria.where("inspect_id").is(inspectDto.getId().toHexString());
        Query query = new Query(criteria);
        query.with(Sort.by(Sort.Direction.DESC, "_id"));
        query.fields().exclude("inspect.timing");
        List<InspectResultDto> resultDtos = inspectResultService.findAll(query);
        if (CollectionUtils.isNotEmpty(resultDtos)) {
            InspectResultDto inspectResultDto = resultDtos.get(0);
            inspectResultDto.setSourceTotal(inspectResultDto.getFirstSourceTotal());
            Pair<Integer, String> pair = InspectService.joinResult(inspectDto, inspectResultDto);
//            inspectResultDto.setDifferenceNumber(pair.getKey());
            inspectDto.setInspectResult(resultDtos.get(0));
            inspectDto.setDifferenceNumber(pair.getKey());
        }
        InspectDetailVo inspectDetailVo = BeanUtil.copyProperties(inspectDto, InspectDetailVo.class);
        return inspectDto;
    }


    @Override
    public InspectDto save(InspectDto inspectDto, UserDetail user) {
        inspectDto.setUserId(user.getUserId());
        List<Task> taskList = inspectDto.getTasks();
        if (CollectionUtils.isNotEmpty(taskList)) {
            taskList.forEach(task -> {
                task.setTaskId(UUIDUtil.getUUID());
            });
        }

        PlatformInfo platformInfo = inspectDto.getPlatformInfo();
        List agentTags = new ArrayList();
        if (StringUtils.isNotBlank(platformInfo.getAgentType())) {
            agentTags.add(platformInfo.getAgentType());
        }
        inspectDto.setAgentTags(agentTags);

      /*  if (Mode.MANUAL.getValue().equals(inspectDto.getMode())){
            inspectDto.setStatus(InspectStatusEnum.SCHEDULING.getValue());
        }
        else {
            inspectDto.setStatus(InspectStatusEnum.WAITING.getValue());
        }*/
        Date now = new Date();
        //保存的时候，直接把ping_time 保存为当前时间
        inspectDto.setPing_time(now.getTime());
        inspectDto.setLastStartTime(0L);

				supplementAlarm(inspectDto, user);

        super.save(inspectDto, user);

				if (InspectStatusEnum.SCHEDULING.getValue().equalsIgnoreCase(inspectDto.getStatus())) {
					Where where = new Where();
					where.put("id", inspectDto.getId().toString());
					executeInspect(where, inspectDto, user);
				}
        return inspectDto;
    }


    /**
     * 新建数据复制任务的时候，需要新建数据校验，就调用这个方法
     * 数据开发，目前不支持数据校验
     *
     * @param taskDto
     * @param userDetail
     * @return
     */
    public void saveInspect(TaskDto taskDto, UserDetail userDetail) {
        try {
            String synType = taskDto.getSyncType();
            String status = taskDto.getStatus();
            if (SyncType.MIGRATE.getValue().equals(synType)) {
                InspectDto inspectDto = new InspectDto();
                inspectDto.setName(taskDto.getName());
                inspectDto.setTaskId(taskDto.getId().toString());
                inspectDto.setPing_time(new Date().getTime());
                inspectDto.setUserId(userDetail.getUserId());
                inspectDto.setMode("manual");
                inspectDto.setInspectMethod("cdcField");
//                inspectDto.setAgentTags(Arrays.asList("private"));
                workerService.scheduleTaskToEngine(inspectDto, userDetail);
                super.save(inspectDto, userDetail);
            }
        } catch (Exception e) {
            log.error("新建任务同时，新建数据校验失败", e);
        }
    }

    public InspectDto createCheckByTask(TaskDto taskDto, UserDetail userDetail) {
        InspectDto inspectDto = new InspectDto();
        inspectDto.setFlowId(taskDto.getId().toHexString());
        inspectDto.setName(taskDto.getName() + "_inspect_" + DateUtil.formatTime(DateUtil.date()));
        inspectDto.setMode("manual");
        inspectDto.setInspectMethod("row_count");
        inspectDto.setInspectDifferenceMode("All");
        Limit limit = new Limit();
        limit.setKeep(100);
        inspectDto.setLimit(limit);
        inspectDto.setEnabled(true);
        inspectDto.setStatus("waiting");
        inspectDto.setPing_time(System.currentTimeMillis());

        inspectDto.setScheduleTimes(0);

        List<AlarmSettingVO> alarmSettings = new ArrayList<>();
        alarmSettings.add(AlarmSettingVO.builder()
                .type(AlarmSettingTypeEnum.INSPECT)
                .open(true)
                .key(AlarmKeyEnum.INSPECT_TASK_ERROR)
                .notify(Lists.newArrayList(NotifyEnum.SYSTEM, NotifyEnum.EMAIL)).build());
        Map<String, Object> map = Maps.newHashMap();
        map.put("maxDifferentialRows", 0);
        alarmSettings.add(AlarmSettingVO.builder()
                .type(AlarmSettingTypeEnum.INSPECT)
                .open(true)
                .key(AlarmKeyEnum.INSPECT_COUNT_ERROR)
                .notify(Lists.newArrayList(NotifyEnum.SYSTEM, NotifyEnum.EMAIL))
                .params(map).build());
        inspectDto.setAlarmSettings(alarmSettings);

        List<Task> taskList = new ArrayList<>();
        List<MetadataInstancesDto> instancesDtos = metadataInstancesService.findByTaskId(taskDto.getId().toHexString(), userDetail);
        Assert.notEmpty(instancesDtos);
        Map<String, List<MetadataInstancesDto>> metaMap = instancesDtos.stream().collect(Collectors.groupingBy(MetadataInstancesDto::getNodeId));

        taskDto.getDag().getSources().forEach(node -> {
            List<MetadataInstancesDto> metadataInstancesDtos = metaMap.get(node.getId());
            if (CollectionUtils.isNotEmpty(metadataInstancesDtos)) {
                metadataInstancesDtos.forEach(meta -> {
                    Task task = new Task();
                    task.setTaskId(UUIDUtil.getUUID());
                    task.setFullMatch(true);
                    task.setShowAdvancedVerification(false);

                    Source source = new Source();
                    source.setNodeId(node.getId());
                    source.setConnectionName(node.getName());
                    source.setNodeName(node.getName());

                    AtomicReference<String> sortColumn = new AtomicReference<>("");
                    meta.getFields().stream().filter(field -> field.getPrimaryKeyPosition() == 1).findFirst().ifPresent(key -> {
                        sortColumn.set(key.getFieldName());
                    });
                    if (StringUtils.isEmpty(sortColumn.get())) {
                        meta.getFields().stream().filter(field -> field.getColumnPosition() == 1).findFirst().ifPresent(key -> {
                            sortColumn.set(key.getFieldName());
                        });
                    }
                    source.setSortColumn(sortColumn.get());
                    source.setTable(meta.getOriginalName());
                    if (TaskDto.SYNC_TYPE_MIGRATE.equals(taskDto.getSyncType())) {
                        DatabaseNode databaseNode = (DatabaseNode) node;
                        source.setConnectionId(databaseNode.getConnectionId());
                        source.setDatabaseType(databaseNode.getDatabaseType());
                    } else {
                        TableNode tableNode = (TableNode) node;
                        source.setConnectionId(tableNode.getConnectionId());
                        source.setDatabaseType(tableNode.getDatabaseType());
                    }
                    if (Lists.newArrayList("Clickhouse", "Kafka").contains(source.getDatabaseType())) {
                        throw new BizException("Kafka or Clickhouse not suppport row count inspect!");
                    }

                    task.setSource(source);
                    taskList.add(task);
                });
            }
        });

        taskDto.getDag().getTargets().forEach(node -> {
            List<MetadataInstancesDto> metadataInstancesDtos = metaMap.get(node.getId());
            if (CollectionUtils.isNotEmpty(metadataInstancesDtos)) {
                metadataInstancesDtos.forEach(meta -> {

                    Task task = taskList.stream().filter(t -> meta.getAncestorsName().equals(t.getSource().getTable()))
                            .findFirst().orElseThrow(() -> new BizException("not found task by AncestorsName:" + meta.getAncestorsName()));

                    Source target = new Source();
                    target.setNodeId(node.getId());
                    target.setConnectionName(node.getName());
                    target.setNodeName(node.getName());

                    AtomicReference<String> sortColumn = new AtomicReference<>("");
                    meta.getFields().stream().filter(field -> field.getPrimaryKeyPosition() == 1).findFirst().ifPresent(key -> {
                        sortColumn.set(key.getFieldName());
                    });
                    if (StringUtils.isEmpty(sortColumn.get())) {
                        meta.getFields().stream().filter(field -> field.getColumnPosition() == 1).findFirst().ifPresent(key -> {
                            sortColumn.set(key.getFieldName());
                        });
                    }

                    target.setSortColumn(sortColumn.get());
                    target.setTable(meta.getOriginalName());
                    if (TaskDto.SYNC_TYPE_MIGRATE.equals(taskDto.getSyncType())) {
                        DatabaseNode databaseNode = (DatabaseNode) node;
                        target.setConnectionId(databaseNode.getConnectionId());
                        target.setDatabaseType(databaseNode.getDatabaseType());
                    } else {
                        TableNode tableNode = (TableNode) node;
                        target.setConnectionId(tableNode.getConnectionId());
                        target.setDatabaseType(tableNode.getDatabaseType());
                    }
                    if (Lists.newArrayList("Clickhouse", "Kafka").contains(target.getDatabaseType())) {
                        throw new BizException("Kafka or Clickhouse not suppport row count inspect!");
                    }

                    task.setTarget(target);
                });
            }
        });

        inspectDto.setTasks(taskList);

        save(inspectDto, userDetail);

        return inspectDto;
    }


    public List<InspectDto> findByTaskIdList(List<String> taskIdList) {
        Query query = Query.query(Criteria.where("flowId").in(taskIdList).and("is_deleted").ne(true));
        return findAll(query);
    }

    public UpdateResult deleteByTaskId(String taskId) {
        UpdateResult updateResult = null;
        try {
            Query query = Query.query(Criteria.where("taskId").is(taskId));
            Update update = new Update().set("is_deleted", true);
            updateResult = repository.getMongoOperations().updateMulti(query, update, InspectEntity.class);
        } catch (Exception e) {
            log.error("删除校验任务异常", e);
        }
        return updateResult;
    }

    /**
     * 页面点击和egine都会调用该方法
     *
     * @param where
     * @param user
     * @return
     */
    public InspectDto updateInspectByWhere(Where where, InspectDto updateDto, UserDetail user) {
        InspectDto retDto = null;
        if (InspectStatusEnum.SCHEDULING.getValue().equals(updateDto.getStatus())) {
            log.info("用户点击了执行校验");
            retDto = executeInspect(where, updateDto, user);
            userLogService.addUserLog(Modular.INSPECT, Operation.START, user.getUserId(), retDto.getId().toString(), retDto.getName());
        } else {
            String status = updateDto.getStatus();
            String result = updateDto.getResult();
            String name = updateDto.getName();
            String id = (String) where.getOrDefault("id", "");
            log.info("engine 调用了 updateInspectByWhere.  where :{}  InspectDto:{} ", JsonUtil.toJson(where), JsonUtil.toJson(updateDto));
            super.updateByWhere(where, updateDto, user);
            retDto = updateDto;

            if (InspectStatusEnum.STOPPING.getValue().equals(status)) {
							log.info("校验 停止 inspect:{}", updateDto);
							InspectDto dto = findById(new ObjectId(id));
							stopInspectTask(dto);
							userLogService.addUserLog(Modular.INSPECT, Operation.STOP, user.getUserId(), dto.getId().toHexString(), dto.getName());
						} else if (InspectStatusEnum.ERROR.getValue().equals(status)) {
                log.info("校验 出错 inspect:{}", updateDto);
                messageService.addInspect(name, id, MsgTypeEnum.INSPECT_ERROR, Level.ERROR, user);
            } else if (InspectStatusEnum.DONE.getValue().equals(status)) {
                log.info("校验 完成 inspect:{}", updateDto);
                if (InspectResultEnum.FAILED.getValue().equals(result)) {
                    messageService.addInspect(name, id, MsgTypeEnum.INSPECT_VALUE, Level.ERROR, user);
                }
            }
        }
        return retDto;
    }

    /**
     * 只会再点击执行任务的时候调用，
     *
     * @param where
     * @param user
     * @return
     */
    public InspectDto executeInspect(Where where, InspectDto updateDto, UserDetail user) {
        String id = (String) where.get("id");
        InspectDto inspectDto = null;
        ObjectId objectId = MongoUtils.toObjectId(id);
        inspectDto = findById(objectId);

        InspectStatusEnum inspectStatus = InspectStatusEnum.of(inspectDto.getStatus());
        if (inspectStatus == InspectStatusEnum.RUNNING) {
            throw new BizException("Inspect.Start.Failed", "运行中的任务不能再次启动");
        }

        inspectDto.setStatus(InspectStatusEnum.SCHEDULING.getValue());

        if (StringUtils.isNotEmpty(updateDto.getByFirstCheckId())) {
            //是二次校验
            inspectDto.setByFirstCheckId(updateDto.getByFirstCheckId());
        } else {
            //不是二次校验
            inspectDto.setByFirstCheckId("");
        }
        inspectDto.setAgentId(null); // 执行前先清除，重新分配 Agent
        workerService.scheduleTaskToEngine(inspectDto, user);
        Date now = new Date();
        inspectDto.setPing_time(now.getTime());
        inspectDto.setLastStartTime(now.getTime());

        if (StringUtils.isEmpty(inspectDto.getAgentId())) {
            throw new BizException("Inspect.ProcessId.NotFound");
        }
        String agentId = inspectDto.getAgentId();
        where.put("id", objectId);
        updateByWhere(where, inspectDto, user);

        inspectDto.setInspectResultId(updateDto.getInspectResultId());
        inspectDto.setTaskIds(updateDto.getTaskIds());

        if (!StringUtils.isEmpty(inspectDto.getInspectResultId())) { // 重新执行校验
            List<String> taskIds = inspectDto.getTaskIds();
            if (taskIds == null || taskIds.size() == 0) {
                InspectResultDto inspectResult = inspectResultService.findById(new ObjectId(inspectDto.getInspectResultId()));
                taskIds = inspectResult.getStats().stream().filter(stats -> "failed".equals(stats.getResult()))
                        .map(Stats::getTaskId).collect(Collectors.toList());
            }
            List<String> finalTaskIds = taskIds;
            List<Task> newTasks = inspectDto.getTasks().stream().filter(task -> finalTaskIds.contains(task.getTaskId()))
                    .collect(Collectors.toList());
            inspectDto.setTasks(newTasks);

            long result = inspectDetailsService.deleteAll(Query.query(
                    Criteria.where("inspect_id").is(id)
                            .and("taskId").in(finalTaskIds)
                            .and("inspectResultId").is(inspectDto.getInspectResultId())));
            log.info("Remove inspect details before restart inspect task (inspectId={}, inspectResultId={}, taskId=[{}]), delete {} records.",
                    id, inspectDto.getInspectResultId(), String.join(",", finalTaskIds), result);
        }

        startInspectTask(inspectDto, agentId);
        return inspectDto;
    }


    /**
     * 开始数据校验  点击校验按钮，更新报错，新增的时候都会调用
     *
     * @param inspectDto
     * @param processId
     * @return
     */
    private String startInspectTask(InspectDto inspectDto, String processId) {
        try {
            String json = JsonUtil.toJsonUseJackson(inspectDto);
            Map<String, Object> data = JsonUtil.parseJson(json, Map.class);
            data.put("type", "data_inspect");
            data.remove("timing");
            //data里面要放需要校验的数据
            MessageQueueDto messageQueueDto = new MessageQueueDto();
            messageQueueDto.setReceiver(processId);
            messageQueueDto.setSender("");
            messageQueueDto.setData(data);
            messageQueueDto.setType("pipe");

            log.info("发送websocket 执行数据校验, processId = {}, name {}, inspectId {}", processId, inspectDto.getName(), inspectDto.getId());
            messageQueueService.sendMessage(messageQueueDto);
        } catch (Exception e) {
            log.error("启动websocket 异常", e);
        }
        return processId;
    }
    /**
     * 停止数据校验
     *
     * @param inspectDto
     * @return
     */
    private void stopInspectTask(InspectDto inspectDto) {
			try {
				String processId = inspectDto.getAgentId();
				if (null == processId || processId.isEmpty()) {
					log.error("Stop inspect '{}' failed, agentId can not be empty", inspectDto.getId().toHexString());
					return;
				}

				Map<String, Object> data = new HashMap<>();
				data.put("id", inspectDto.getId().toHexString());
				data.put("type", "data_inspect");
				data.put("status", InspectStatusEnum.STOPPING.getValue());
				//data里面要放需要校验的数据
				MessageQueueDto messageQueueDto = new MessageQueueDto();
				messageQueueDto.setReceiver(processId);
				messageQueueDto.setSender("");
				messageQueueDto.setData(data);
				messageQueueDto.setType("pipe");

				log.info("Send stop inspect websocket message success, processId = {}, name {}, inspectId {}", processId, inspectDto.getName(), inspectDto.getId());
				messageQueueService.sendMessage(messageQueueDto);
			} catch (Exception e) {
				log.error("Stop inspect failed: {}", e.getMessage(), e);
			}
		}

    /**
     * 编辑的时候，如果新增了校验条件，就要新生成一个taskId ,如果原有taskId ,就不用
     *
     * @param objectId
     * @param inspectDto
     * @param userDetail
     * @return
     */
    public InspectDto updateById(ObjectId objectId, InspectDto inspectDto, UserDetail userDetail) {
        Where where = new Where();
        where.put("id", objectId);
				supplementAlarm(inspectDto, userDetail);

        List<Task> newTaskList = inspectDto.getTasks();
        if (CollectionUtils.isNotEmpty(newTaskList)) {
            newTaskList.forEach(task -> {
                if (null == task.getTaskId()) {
                    task.setTaskId(UUIDUtil.getUUID());
                }
            });
        }
        workerService.scheduleTaskToEngine(inspectDto, userDetail);

        String agentId = inspectDto.getAgentId();
        updateByWhere(where, inspectDto, userDetail);

        //编辑的时候，都先把就的定时任务删掉
        CronUtil.removeJob(inspectDto.getId().toString());

				if (InspectStatusEnum.SCHEDULING.getValue().equalsIgnoreCase(inspectDto.getStatus())) {
					startInspectTask(inspectDto, agentId);
				}
        return inspectDto;
    }


    public List<InspectDto> findByName(String name) {
        Query query = Query.query(Criteria.where("name").is(name).and("is_deleted").ne(true));
        List<InspectDto> inspectDtoList = findAll(query);

        return inspectDtoList;
    }

 /*   public List<InspectDto> findByPingTimeOut(List<InspectStatusEnum> inspectStatusEnumList) {
        List<String> statusList = inspectStatusEnumList.stream().map(InspectStatusEnum::getValue).collect(Collectors.toList());

        Query query = Query.query(Criteria.where("status").in(statusList).and("is_deleted").ne(true));
        List<InspectDto> inspectDtoList = findAll(query);

        return inspectDtoList;
    }*/

    /**
     * * upsert  0跳过已有数据  1 覆盖已有数据
     *
     * @param json
     * @param upsert
     */
    public void importData(String json, String upsert, UserDetail userDetail) {
        Map map = JsonUtil.parseJson(json, Map.class);
        List<Map> data = (List) map.get("data");

        if (CollectionUtils.isNotEmpty(data)) {
            for (Map singleDataMap : data) {
                Query query = new Query();
                String id = singleDataMap.get("id").toString();
                singleDataMap.remove("id");
                if (StringUtils.isNotBlank(id)) {
                    InspectDto existedDto = findById(MongoUtils.toObjectId(id));
                    Map existedPoperty = BeanUtil.beanToMap(existedDto);
                    if ("0".equals(upsert)) {
//                        跳过已有数据,
                        singleDataMap.forEach((key, value) ->
                        {
                            if (existedPoperty.containsKey(key)) {
                                singleDataMap.put(key, existedPoperty.get(key));
                            }
                        });

                    }
                    query.addCriteria(Criteria.where("id").is(id));
                }
                InspectDto newDto = BeanUtil.mapToBean(singleDataMap, InspectDto.class, false, CopyOptions.create());
                newDto.setIs_deleted(false);
//                newDto.setResult("");
                newDto.setPing_time(new Date().getTime());
//                newDto.setInspectResult(null);
                newDto.setLastStartTime(null);
/*                newDto.setErrorMsg("");
                newDto.setStatus("");*/
//                newDto.setDifferenceNumber(0);
                super.upsert(query, newDto, userDetail);
            }
        }
    }

    public void setRepeatInspectTask() {
        log.info("begin to start repeat inspect");
        Query query = Query.query(Criteria.where("mode").is(Mode.CRON.getValue()).and("enabled").is(true).and("is_deleted").ne(true));
        List<InspectDto> inspectDtoList = findAll(query);

        inspectDtoList.forEach(inspectDto -> {
            Timing timing = inspectDto.getTiming();
            if (null != timing) {
                Date startDate = new Date(timing.getStart());
                Date endDate = new Date(timing.getEnd());
                Long intervals = inspectDto.getTiming().getIntervals();
                String intervalUnit = inspectDto.getTiming().getIntervalsUnit();
                String id = inspectDto.getId().toString();
                CronUtil.addJob(startDate, endDate, intervals, intervalUnit, id);
            } else {
                log.error("数据有误，定时校验任务timing为空 inspectDto：{}", JsonUtil.toJson(inspectDto));
            }
        });
        log.info("finish to start repeat inspect");
    }


    public UpdateResult updateStatusById(String id, InspectStatusEnum inspectStatusEnum) {
        Update update = new Update();
        update.set("status", inspectStatusEnum.getValue());
        Query updateQuery = new Query(Criteria.where("id").is(id));
        return repository.getMongoOperations().updateFirst(updateQuery, update, InspectEntity.class);
    }

    public UpdateResult updateStatusByIds(List<ObjectId> idList, InspectStatusEnum status) {
        Update update = new Update();
        update.set("status", status.getValue());
        Query updateQuery = new Query(Criteria.where("id").in(idList));
        return repository.getMongoOperations().updateMulti(updateQuery, update, InspectEntity.class);
    }

    public Map<String, Integer> inspectPreview(UserDetail user) {
        Query query = Query.query(Criteria.where("is_deleted").ne(true).and("user_id").is(user.getUserId()));
        List<InspectDto> inspectDtos = findAll(query);
        if (CollectionUtils.isNotEmpty(inspectDtos)) {
            joinResult(inspectDtos);
        }

        int error = inspectDtos.stream().filter(item->InspectStatusEnum.ERROR.getValue().equals(item.getStatus())).collect(Collectors.toList()).size();
        int passed =  inspectDtos.stream().filter(item->InspectResultEnum.PASSED.getValue().equals(item.getResult())
                                                &&(!InspectStatusEnum.ERROR.getValue().equals(item.getStatus()) )).collect(Collectors.toList()).size();

        int countDiff = inspectDtos.stream().filter(item->InspectResultEnum.FAILED.getValue().equals(item.getResult())
                                                        &&InspectMethod.ROW_COUNT.getValue().equals(item.getInspectMethod())
                                                        &&!InspectStatusEnum.ERROR.getValue().equals(item.getStatus()) ).collect(Collectors.toList()).size();

        int valueDiff = inspectDtos.stream().filter(item->InspectResultEnum.FAILED.getValue().equals(item.getResult())
                                                        &&!InspectMethod.ROW_COUNT.getValue().equals(item.getInspectMethod())
                                                        &&!InspectStatusEnum.ERROR.getValue().equals(item.getStatus()) ).collect(Collectors.toList()).size();;


        /*for (InspectDto inspectDto : inspectDtos) {
            if ("error".equals(inspectDto.getStatus())) {
                error++;
            } else if ("passed".equals(inspectDto.getResult())) {
                passed++;
            } else if ("row_count".equals(inspectDto.getInspectMethod())) {
                countDiff++;
            } else {
                valueDiff++;
            }
        }*/
        Map<String, Integer> chart5 = new HashMap<>();
        chart5.put("total", inspectDtos.size());
        chart5.put("error", error);
        chart5.put("passed", passed);
        chart5.put("countDiff", countDiff);
        chart5.put("valueDiff", valueDiff);
        return chart5;
    }

    public List<InspectDto> findByStatus(InspectStatusEnum inspectStatusEnum) {
        Query query = Query.query(Criteria.where("status").is(inspectStatusEnum.getValue()));
        List<InspectDto> inspectDtoList = findAllNotDeleted(query);
        return inspectDtoList;
    }

    public List<InspectDto> findByResult(boolean passed) {
        Query query;
        if (passed) {
            query = Query.query(Criteria.where("result").is(InspectStatusEnum.PASSED.getValue()));
        } else {
            query = Query.query(Criteria.where("result").ne(InspectStatusEnum.PASSED.getValue()));
        }
        return findAllNotDeleted(query);
    }

    public void cleanDeadInspect() {
        Settings settings = settingsService.getByCategoryAndKey(CategoryEnum.JOB, KeyEnum.JOB_HEART_TIMEOUT);
        Long timeOutMillSeconds = Long.valueOf(settings.getValue().toString());

        Date now = new Date();
        //超时时长 为 5分钟
        Long timeOutDate = now.getTime() - timeOutMillSeconds;
        Query query = Query.query(Criteria.where("status").is(InspectStatusEnum.SCHEDULING.getValue()).and("ping_time").lt(timeOutDate));
        Update update = new Update().set("status", InspectStatusEnum.ERROR.getValue());

        List list = findAll(query);
        log.info(JsonUtil.toJson(list));

        UpdateResult updateResult = update(query, update);
    }

		public void supplementAlarm(InspectDto inspectDto, UserDetail userDetail) {
        try {
            List<AlarmSettingDto> settingDtos = alarmSettingService.findAllAlarmSetting(userDetail);
            Map<AlarmKeyEnum, AlarmSettingDto> settingDtoMap = settingDtos.stream().collect(Collectors.toMap(AlarmSettingDto::getKey, Function.identity(), (e1, e2) -> e1));
            if (CollectionUtils.isEmpty(inspectDto.getAlarmSettings())) {
                List<AlarmSettingDto> alarmSettingDtos = Lists.newArrayList();
                alarmSettingDtos.add(settingDtoMap.get(AlarmKeyEnum.INSPECT_TASK_ERROR));
                alarmSettingDtos.add(settingDtoMap.get(AlarmKeyEnum.INSPECT_COUNT_ERROR));
                alarmSettingDtos.add(settingDtoMap.get(AlarmKeyEnum.INSPECT_VALUE_ERROR));
                inspectDto.setAlarmSettings(CglibUtil.copyList(alarmSettingDtos, AlarmSettingVO::new));
            }
        } catch (Exception e) {
            log.warn("supplement alarm error", e);
        }
    }

	@Override
	public long updateByWhere(Where where, InspectDto dto, UserDetail userDetail) {
			//更新状态
		String status = dto.getStatus();
		switch (Objects.requireNonNull(InspectStatusEnum.of(status))) {
			case ERROR:
			case FAILED:
				List<InspectDto> inspectDtos = this.findAll(new Filter(where));
				if (CollectionUtils.isNotEmpty(inspectDtos)) {
					for (InspectDto inspectDto : inspectDtos) {
						boolean checkOpen = alarmService.checkOpen(inspectDto.getAlarmSettings(), AlarmKeyEnum.INSPECT_TASK_ERROR, null, userDetail);
						if (checkOpen) {
							Map<String, Object> param = Maps.newHashMap();
							param.put("inspectName", inspectDto.getName());
							param.put("alarmDate", DateUtil.now());
							AlarmInfo errorInfo = AlarmInfo.builder().status(AlarmStatusEnum.ING).level(Level.CRITICAL).component(AlarmComponentEnum.FE)
											.type(AlarmTypeEnum.INSPECT_ALARM).agentId(inspectDto.getAgentId()).inspectId(inspectDto.getId().toHexString())
											.name(inspectDto.getName()).summary("INSPECT_TASK_ERROR").metric(AlarmKeyEnum.INSPECT_TASK_ERROR)
											.param(param)
											.lastNotifyTime(DateUtil.date())
											.build();
							errorInfo.setUserId(inspectDto.getUserId());
							alarmService.save(errorInfo);
						}
					}
				}
				break;
			case SCHEDULING:
				Object id = where.get("id");
				if (id instanceof ObjectId) {
					id = ((ObjectId) id).toHexString();
				} else {
					id = id.toString();
				}
				alarmService.closeWhenInspectTaskRunning((String) id);
				break;

		}
		return super.updateByWhere(where, dto, userDetail);
	}

	public List<InspectDto> findAllByIds(List<String> inspectIds) {
		List<ObjectId> ids = inspectIds.stream().map(ObjectId::new).collect(Collectors.toList());

		Query query = new Query(Criteria.where("_id").in(ids));
		List<InspectEntity> entityList = findAllEntity(query);
		return CglibUtil.copyList(entityList, InspectDto::new);
//        return findAll(query);
	}
}
