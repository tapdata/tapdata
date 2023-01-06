package com.tapdata.tm.monitoringlogs.service;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson.JSON;
import com.tapdata.manager.common.utils.IOUtils;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.monitoringlogs.entity.MonitoringLogsEntity;
import com.tapdata.tm.monitoringlogs.param.MonitoringLogCountParam;
import com.tapdata.tm.monitoringlogs.param.MonitoringLogExportParam;
import com.tapdata.tm.monitoringlogs.param.MonitoringLogQueryParam;
import com.tapdata.tm.monitoringlogs.repository.MonitoringLogsRepository;
import com.tapdata.tm.monitoringlogs.vo.MonitoringLogCountVo;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.enums.TaskState;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.MessageUtil;
import com.tapdata.tm.utils.MongoUtils;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.GroupOperation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.util.CloseableIterator;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.text.MessageFormat;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.zip.ZipOutputStream;

/**
 * @Author:
 * @Date: 2022/06/20
 * @Description:
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class MonitoringLogsService extends BaseService<MonitoringLogsDto, MonitoringLogsEntity, ObjectId, MonitoringLogsRepository> {
    private static final int MAX_DATA_SIZE = 100;
    private static final int MAX_MESSAGE_CHAR_LENGTH = 2000;
    private MongoTemplate mongoOperations;
    private TaskService taskService;

    public MonitoringLogsService(@NonNull MonitoringLogsRepository repository) {
        super(repository, MonitoringLogsDto.class, MonitoringLogsEntity.class);
    }

    protected void beforeSave(MonitoringLogsDto monitoringLogs, UserDetail user) {

    }

    public void batchSave(List<MonitoringLogsDto> monitoringLoges, UserDetail user) {

        BulkOperations bulkOperations = repository.getMongoOperations().bulkOps(BulkOperations.BulkMode.UNORDERED, MonitoringLogsEntity.class);
        for (MonitoringLogsDto monitoringLoge : monitoringLoges) {
            beforeSave(monitoringLoge, user);
        }

        List<MonitoringLogsEntity> monitoringLogsEntities = convertToEntity(MonitoringLogsEntity.class, monitoringLoges);

        for (MonitoringLogsEntity monitoringLogsEntity : monitoringLogsEntities) {
            monitoringLogsEntity.setTimestamp(System.currentTimeMillis());
            repository.applyUserDetail(monitoringLogsEntity, user);
            if (Objects.nonNull(monitoringLogsEntity.getData())) {
                monitoringLogsEntity.setDataJson(JSON.toJSONString(monitoringLogsEntity.getData()));
            }
            bulkOperations.insert(monitoringLogsEntity);
        }

        bulkOperations.execute();
    }

    public Page<MonitoringLogsDto> query(MonitoringLogQueryParam param) {
        String taskId = param.getTaskId();
        if (null == taskId) {
            return null;
        }

        ObjectId objectId = MongoUtils.toObjectId(taskId);

        if (objectId == null) {
            return null;
        }

        Criteria criteria = Criteria.where("taskId").is(taskId);
        if (StringUtils.isNotBlank(param.getTaskRecordId())) {
            criteria.and("taskRecordId").is(param.getTaskRecordId());
        }

        Long start = param.getStart();

        String type = param.getType();
        if ("monitor".equals(type)) {
            if (!param.isStartEndValid()) {
                log.error("Invalid value for start or end param:{}", JSON.toJSONString(param));
                return new Page<>(0, new ArrayList<>());
            }
            // monitor log save will after task stopTime 5s, so add 10s;
            Long end = param.getEnd();
            end += 10000L;
            criteria.and("timestamp").gte(start - 5000L).lt(end);
        } else if ("testRun".equals(type)) {
            criteria.and("timestamp").gte(start);
        }

        if (StringUtils.isNotEmpty(param.getNodeId())) {
            criteria.and("nodeId").is(param.getNodeId());
        }

        // keyword search filter
        if (StringUtils.isNotEmpty(param.getSearch())) {
            String search = param.getSearch();
            criteria.orOperator(
//                    new Criteria("taskName").regex(search),
                    new Criteria("nodeName").regex(search),
                    new Criteria("message").regex(search),
                    new Criteria("dataJson").regex(search),
                    new Criteria("errorStack").regex(search),
                    new Criteria("logTags").elemMatch(new Criteria("$regex").is(search))
            );
        }

        // log level filter
        List<String> levels = param.getLevels();
        if (null == levels) {
            levels = param.getFullLevels();
        }
        criteria.and("level").in(levels);

        // sort and _id filter, avoid duplicate data
        Sort sort = Sort.by("date");
        switch (param.getOrder()) {
            case MonitoringLogQueryParam.ORDER_ASC:
                sort = sort.ascending();
                break;
            case MonitoringLogQueryParam.ORDER_DESC:
                sort = sort.descending();
                break;
        }

        Query query = new Query(criteria);
        query.with(sort);

        long total = mongoOperations.count(query, MonitoringLogsEntity.class);
        if (total == 0) {
            return new Page<>(0, new ArrayList<>());
        }

        // query limitation
        query.skip((param.getPage() - 1) * param.getPageSize());
        query.limit(param.getPageSize().intValue());
        List<MonitoringLogsEntity> logEntities = mongoOperations.find(query, MonitoringLogsEntity.class);
        List<MonitoringLogsDto> logs = new ArrayList<>();
        for (MonitoringLogsEntity logEntity : logEntities) {
            if (null != logEntity.getData() && logEntity.getData().size() > MAX_DATA_SIZE) {
                logEntity.setData(logEntity.getData().subList(0, MAX_DATA_SIZE - 1));
            }
            if (null != logEntity.getMessage() && logEntity.getMessage().length() > MAX_MESSAGE_CHAR_LENGTH) {
                logEntity.setMessage((logEntity.getMessage().substring(0, MAX_MESSAGE_CHAR_LENGTH - 1)) + "...");
            }
            logs.add(convertToDto(logEntity, MonitoringLogsDto.class));
        }

        return new Page<>(total, logs);
    }

    public List<MonitoringLogCountVo> count(MonitoringLogCountParam param) {
        return count(param.getTaskId(), param.getTaskRecordId());
    }

    public List<MonitoringLogCountVo> count(String taskId, String taskRecordId) {
       List<MonitoringLogCountVo> data = new ArrayList<>();
        if (null == taskId || null == taskRecordId) {
            return data;
        }
        Criteria criteria = Criteria.where("taskId").is(taskId).and("taskRecordId").is(taskRecordId);
        MatchOperation match = Aggregation.match(criteria);
        GroupOperation group = Aggregation.group("nodeId", "level").count().as("count");
        Aggregation aggregation = Aggregation.newAggregation(match, group);
        mongoOperations.aggregateStream(aggregation, "monitoringLogs", Map.class).forEachRemaining(item -> {
            MonitoringLogCountVo vo = new MonitoringLogCountVo();
            Map<String, String> _id = (Map<String, String>) item.get("_id");
            String nodeId = _id.get("nodeId");
            if (null != nodeId) {
                vo.setNodeId(nodeId);
            }
            vo.setLevel(_id.get("level"));
            vo.setCount((int) item.get("count"));
            data.add(vo);
        });

        return data;
    }

    public void export (MonitoringLogExportParam param, ZipOutputStream stream) {
        if (null == param.getTaskId()) {
            return;
        }

        Criteria criteria = Criteria.where("taskId").is(param.getTaskId());
        if (StringUtils.isNotBlank(param.getTaskRecordId())) {
            criteria.and("taskRecordId").is(param.getTaskRecordId());
        }

        if (!param.isStartEndValid()) {
            throw new BizException("Invalid value for start or end");
        }

        criteria.and("timestamp").gte(param.getStart()).lt(param.getEnd());

        Query query = new Query(criteria);
        query.with(Sort.by("timestamp").ascending());

        CloseableIterator<MonitoringLogsEntity> iter = mongoOperations.stream(query, MonitoringLogsEntity.class);
        AtomicLong count = new AtomicLong();
        iter.forEachRemaining(logEntity -> {
            try {
                MonitoringLogsDto logDto = convertToDto(logEntity, MonitoringLogsDto.class);
                count.addAndGet(1);

                stream.write((logDto.formatMonitoringLog() + "\n").getBytes());
            } catch (IOException e) {
                throw new BizException("Export.IOError", e);
            }
        });
        IOUtils.closeQuietly(iter);
        if (count.get() == 0) {
            try {
                stream.write(("Can't find any logs by query " + param).getBytes());
            } catch (IOException e) {
                throw new BizException("Export.IOError", e);
            }
        }
    }

    public void taskStateMachineLog(TaskDto taskDto, UserDetail user, DataFlowEvent event,
                                    StateMachineResult stateMachineResult, long cost) {
        MonitoringLogsDto.MonitoringLogsDtoBuilder builder = MonitoringLogsDto.builder();
        builder.taskId(taskDto.getId().toHexString())
                .taskName(taskDto.getName())
                .taskRecordId(taskDto.getTaskRecordId())
                .date(DateUtil.date())
                .timestamp(System.currentTimeMillis())
                .level("INFO")
        ;
        String message;
        //在什么时间(时间戳), 操作人(admin), 对任务执行了什么操作(启动任务), 任务从 待启动 变为 调度中 / 启动中, 时间花费多少(100ms)
        if (TaskState.STOPPED.getName().equals(stateMachineResult.getAfter())) {
            String template = "{0} operator[{1}] task result[{2}] change status {3} to {4}, cost {5}ms, agentId: {6}";
            message = MessageFormat.format(template, user.getUsername(), event.getName(), stateMachineResult.getCode(),
                    stateMachineResult.getBefore(), stateMachineResult.getAfter(), cost, taskDto.getAgentId());
        } else {
            String template = "{0} operator[{1}] task result[{2}] change status {3} to {4}, cost {5}ms";
            message = MessageFormat.format(template, user.getUsername(), event.getName(), stateMachineResult.getCode(),
                    stateMachineResult.getBefore(), stateMachineResult.getAfter(), cost);
        }

        save(builder.message(message).build(), user);
    }

    public void startTaskMonitoringLog(TaskDto taskDto, UserDetail user, Date date) {
        MonitoringLogsDto.MonitoringLogsDtoBuilder builder = MonitoringLogsDto.builder();
        builder.taskId(taskDto.getId().toHexString())
                .taskName(taskDto.getName())
                .taskRecordId(taskDto.getTaskRecordId())
                .date(date)
                .timestamp(System.currentTimeMillis())
                .level("INFO")
                .message("Start task...")
                ;


        save(builder.build(), user);
    }

    public void startTaskErrorLog(TaskDto taskDto, UserDetail user, Object e, Level level) {
        MonitoringLogsDto.MonitoringLogsDtoBuilder builder = MonitoringLogsDto.builder();
        builder.taskId(taskDto.getId().toHexString())
                .taskName(taskDto.getName())
                .taskRecordId(taskDto.getTaskRecordId())
                .date(DateUtil.date())
                .timestamp(System.currentTimeMillis())
                .level(level.name())
        ;
        String msg;
        if (e instanceof BizException) {
            msg = MessageUtil.getMessage(((BizException) e).getErrorCode());
        } else if (e instanceof Exception) {
            msg = ((Exception) e).getMessage();
        } else if (e instanceof String) {
            msg = (String) e;
        } else {
            msg = e.toString();
        }

        save(builder.message(msg).build(), user);
    }

    public void agentAssignMonitoringLog(TaskDto taskDto, String assigned, Integer available, UserDetail user, Date now) {
        MonitoringLogsDto.MonitoringLogsDtoBuilder builder = MonitoringLogsDto.builder();
        long time = now.getTime();
        builder.taskId(taskDto.getId().toHexString())
                .taskName(taskDto.getName())
                .taskRecordId(taskDto.getTaskRecordId())
                .date(now)
                .timestamp(time)
                .logTag("Agent Available Check")
                .level("INFO")
        ;
        if (available != null) {
            builder.message(String.format("%s agents are available now, task is assigned to agent %s", available, assigned));
        } else {
            builder.message(String.format("Task is assigned to agent %s manually", assigned));
        }

        save(builder.build(), user);
    }

    public void deleteLogs(String taskId) {
        mongoOperations.remove(new Query(Criteria.where("taskId").is(taskId)), MonitoringLogsEntity.class);
    }

    public List<TaskDagCheckLog> getJsNodeLog(String testRunTaskId, String taskName) {
        List<MonitoringLogsEntity> list = mongoOperations.find(Query.query(Criteria.where("taskId").is(testRunTaskId)), MonitoringLogsEntity.class);
        if (CollectionUtils.isNotEmpty(list)) {
            return list.stream().map(log -> {
                TaskDagCheckLog info = new TaskDagCheckLog();
                info.setTaskId(log.getTaskId());
                info.setCheckType(DagOutputTemplateEnum.MODEL_PROCESS_CHECK.name());
                // 2022-12-08 18:56:44【新任务@14:19:54】【模型推演检测】：
                String date = DateUtil.toLocalDateTime(log.getDate()).format(DateTimeFormatter.ofPattern(DatePattern.NORM_DATETIME_PATTERN));
                String message = "{0}【{1}】【JS处理节点】{2}：";
                info.setLog(MessageFormat.format(message, date, taskName, log.getMessage()));
                info.setGrade(Level.valueOf(log.getLevel()));
                info.setCreateAt(log.getDate());
                info.setId(log.getId());
                return info;
            }).collect(Collectors.toList());
        }

        return null;
    }
}