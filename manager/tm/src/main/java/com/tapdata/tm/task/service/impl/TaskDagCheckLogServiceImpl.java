package com.tapdata.tm.task.service.impl;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import cn.hutool.extra.spring.SpringUtil;
import com.google.common.base.Splitter;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.NodeEnum;
import com.tapdata.tm.commons.dag.process.CustomProcessorNode;
import com.tapdata.tm.commons.dag.process.JsProcessorNode;
import com.tapdata.tm.commons.dag.process.MigrateJsProcessorNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.monitor.dto.TaskLogDto;
import com.tapdata.tm.monitoringlogs.service.MonitoringLogsService;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.repository.TaskDagCheckLogRepository;
import com.tapdata.tm.task.service.DagLogStrategy;
import com.tapdata.tm.task.service.TaskDagCheckLogService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.vo.TaskDagCheckLogVo;
import com.tapdata.tm.task.vo.TaskLogInfoVo;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MongoUtils;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.text.MessageFormat;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Service
@Setter(onMethod_ = {@Autowired})
public class TaskDagCheckLogServiceImpl implements TaskDagCheckLogService {

    private TaskDagCheckLogRepository repository;
    private MongoTemplate mongoTemplate;
    private TaskService taskService;
    private MonitoringLogsService monitoringLogsService;

    @Override
    public TaskDagCheckLog save(TaskDagCheckLog log) {
        return repository.save(log);
    }

    @Override
    public List<TaskDagCheckLog> saveAll(List<TaskDagCheckLog> logs) {
        return repository.saveAll(logs);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List<TaskDagCheckLog> dagCheck(TaskDto taskDto, UserDetail userDetail, boolean onlySave) {

        if(taskDto.getDag().checkMultiDag()){
            throw new BizException("不支持多条链路，请编辑后重试");
        }

        List<TaskDagCheckLog> result = Lists.newArrayList();

        LinkedList<DagOutputTemplateEnum> checkList = onlySave ? DagOutputTemplateEnum.getSaveCheck() : DagOutputTemplateEnum.getStartCheck();
        checkList.forEach(c -> {
            DagLogStrategy dagLogStrategy = SpringUtil.getBean(c.getBeanName(), DagLogStrategy.class);
            List<TaskDagCheckLog> logs = dagLogStrategy.getLogs(taskDto, userDetail);
            if (CollectionUtils.isNotEmpty(logs)) {
                result.addAll(logs);
            }
        });

        SpringUtil.getBean(this.getClass()).saveAll(result);

        return result;
    }

    @Override
    public TaskDagCheckLogVo getLogs(TaskLogDto dto) {
        String taskId = dto.getTaskId();
        String nodeId = dto.getNodeId();
        String grade = dto.getGrade();
        String keyword = dto.getKeyword();

        TaskDto taskDto = taskService.findById(MongoUtils.toObjectId(taskId));
        LinkedHashMap<String, String> nodeMap = taskDto.getDag().getNodes().stream()
                .collect(Collectors.toMap(Node::getId, Node::getName,(x, y) -> y, LinkedHashMap::new));

        Criteria criteria = Criteria.where("taskId").is(taskId);
        Criteria modelCriteria = Criteria.where("taskId").is(taskId);
        if (StringUtils.isNotBlank(nodeId)) {
            criteria.and("nodeId").is(nodeId);
            modelCriteria.and("nodeId").is(nodeId);
        }
        if (StringUtils.isNotBlank(grade)) {
            if (grade.contains(",")) {
                criteria.and("grade").in(Splitter.on(",").trimResults().splitToList(grade));
                modelCriteria.and("grade").in(Splitter.on(",").trimResults().splitToList(grade));
            } else {
                criteria.and("grade").is(grade);
                modelCriteria.and("grade").is(grade);
            }
        }
        if (StringUtils.isNotBlank(keyword)) {
            criteria.regex("log").is(keyword);
            modelCriteria.regex("log").is(keyword);
        }

        List<String> delayList = Lists.of(DagOutputTemplateEnum.MODEL_PROCESS_CHECK.name(),
                DagOutputTemplateEnum.SOURCE_CONNECT_CHECK.name(),
                DagOutputTemplateEnum.TARGET_CONNECT_CHECK.name());
        Query logQuery = new Query(criteria.and("checkType").nin(delayList));
        logQuery.with(Sort.by("_id"));
        List<TaskDagCheckLog> taskDagCheckLogs = find(logQuery);
        if (CollectionUtils.isEmpty(taskDagCheckLogs)) {
            TaskDagCheckLogVo taskDagCheckLogVo = new TaskDagCheckLogVo();
            taskDagCheckLogVo.setNodes(nodeMap);
            return taskDagCheckLogVo;
        }

        Query modelQuery = new Query(modelCriteria.and("checkType").in(delayList));
        modelQuery.with(Sort.by("_id"));
        List<TaskDagCheckLog> modelLogs = find(modelQuery);
        // check task nodes has js node
        Optional<Node> jsNode = taskDto.getDag().getNodes().stream()
                .filter(n -> (n instanceof MigrateJsProcessorNode || n instanceof JsProcessorNode) && !(n instanceof CustomProcessorNode))
                .findFirst();
        if (jsNode.isPresent()) {
            List<TaskDagCheckLog> jsNodeLog = monitoringLogsService.getJsNodeLog(taskDto.getTransformTaskId(), taskDto.getName(), NodeEnum.valueOf(jsNode.get().getType()).getNodeName());
            Optional.ofNullable(jsNodeLog).ifPresent(modelLogs::addAll);
        }

        LinkedList<TaskLogInfoVo> data = packCheckLogs(taskDto, taskDagCheckLogs);
        TaskDagCheckLogVo result = new TaskDagCheckLogVo(nodeMap, data, null, 0, 0, false);

        LinkedList<TaskLogInfoVo> all = new LinkedList<>(data);
        if (CollectionUtils.isNotEmpty(modelLogs)) {
            LinkedList<TaskLogInfoVo> collect = packCheckLogs(taskDto, modelLogs);
            result.setModelList(collect);
            boolean present = taskDto.getTransformed();
            result.setOver(present);

            all.addAll(collect);
        }

        AtomicInteger errorNum = new AtomicInteger();
        AtomicInteger warnNum = new AtomicInteger();
        all.forEach(a -> {
            switch (a.getGrade()) {
                case ERROR:
                    errorNum.getAndIncrement();
                    break;
                case WARN:
                    warnNum.getAndIncrement();
                    break;
            }
        });

        result.setErrorNum(errorNum.get());
        result.setWarnNum(warnNum.get());
        return result;
    }

    private LinkedList<TaskLogInfoVo> packCheckLogs(TaskDto taskDto, List<TaskDagCheckLog> taskDagCheckLogs) {
        return taskDagCheckLogs.stream()
                .map(g -> {
                    String log = g.getLog();
                    log = StringUtils.replace(log, "$taskName", taskDto.getName());
                    log = StringUtils.replace(log, "$date", DateUtil.toLocalDateTime(g.getCreateAt()).format(DateTimeFormatter.ofPattern(DatePattern.NORM_DATETIME_PATTERN)));

                    TaskLogInfoVo taskLogInfoVo = new TaskLogInfoVo(g.getId().toHexString(), g.getGrade(), log);
                    taskLogInfoVo.setTime(g.getCreateAt());
                    return taskLogInfoVo;
                })
                .sorted(Comparator.comparing(TaskLogInfoVo::getTime))
                .collect(Collectors.toCollection(LinkedList::new));
    }

    @Override
    public void removeAllByTaskId(String taskId) {
        mongoTemplate.remove(Query.query(Criteria.where("taskId").is(taskId)), TaskDagCheckLog.class);
    }

    public List<TaskDagCheckLog> find(Query query) {
        return mongoTemplate.find(query, TaskDagCheckLog.class);
    }

    @Override
    public void createLog(String taskId, String userId, Level grade, DagOutputTemplateEnum templateEnum,
                          boolean delOther, boolean needSave, Object ... param) {
        Date now = new Date();
        if (delOther) {
            mongoTemplate.remove(Query.query(Criteria.where("taskId").is(taskId)
                    .and("checkType").is(templateEnum.name())
            ), TaskDagCheckLog.class);
        }

        TaskDagCheckLog log = new TaskDagCheckLog();
        log.setTaskId(taskId);
        log.setCheckType(templateEnum.name());
        log.setCreateAt(now);
        log.setCreateUser(userId);
        log.setGrade(grade);

        String template;
        if (Level.INFO.equals(grade)) {
            template = templateEnum.getInfoTemplate();
        } else if (Level.ERROR.equals(grade)){
            template = templateEnum.getErrorTemplate();
        } else {
            template = templateEnum.getInfoTemplate();
        }
        String content = MessageFormat.format(template, param);
        log.setLog(content);

        if (needSave) {
            this.save(log);
        }

    }
}
