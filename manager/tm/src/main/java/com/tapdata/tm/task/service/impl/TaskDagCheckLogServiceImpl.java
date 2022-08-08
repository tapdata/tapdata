package com.tapdata.tm.task.service.impl;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import cn.hutool.extra.spring.SpringUtil;
import com.google.common.base.Splitter;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.MigrateJsProcessorNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.observability.dto.TaskLogDto;
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
import java.util.stream.Collectors;

@Service
@Setter(onMethod_ = {@Autowired})
public class TaskDagCheckLogServiceImpl implements TaskDagCheckLogService {

    private TaskDagCheckLogRepository repository;
    private MongoTemplate mongoTemplate;
    private TaskService taskService;

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
//        int offset = dto.getOffset();
//        int limit = dto.getLimit();

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
        logQuery.with(Sort.by("createTime"));
        List<TaskDagCheckLog> taskDagCheckLogs = find(logQuery);
        if (CollectionUtils.isEmpty(taskDagCheckLogs)) {
            TaskDagCheckLogVo taskDagCheckLogVo = new TaskDagCheckLogVo();
            taskDagCheckLogVo.setNodes(nodeMap);
            return taskDagCheckLogVo;
        }

        Query modelQuery = new Query(modelCriteria.and("checkType").in(delayList));
        modelQuery.with(Sort.by("createTime"));
        List<TaskDagCheckLog> modelLogs = find(modelQuery);

        LinkedList<TaskLogInfoVo> data = taskDagCheckLogs.stream()
                .map(g -> {
                    String log = g.getLog();
                    log = StringUtils.replace(log, "$taskName", taskDto.getName());
                    log = StringUtils.replace(log, "$date", DateUtil.toLocalDateTime(g.getCreateAt()).format(DateTimeFormatter.ofPattern(DatePattern.NORM_DATETIME_PATTERN)));

                    return new TaskLogInfoVo(g.getId().toHexString(), g.getGrade(), log);
                })
                .collect(Collectors.toCollection(LinkedList::new));

        TaskDagCheckLogVo result = new TaskDagCheckLogVo(nodeMap, data, null, false);

        if (CollectionUtils.isNotEmpty(modelLogs)) {
            // duplication
            modelLogs = modelLogs.stream()
                    .collect(Collectors.collectingAndThen(Collectors.toCollection(() ->
                            new TreeSet<>(Comparator.comparing(TaskDagCheckLog::getLog))), LinkedList::new));

            LinkedList<TaskLogInfoVo> collect = modelLogs.stream()
                    .map(g -> {
                        String log = g.getLog();
                        log = StringUtils.replace(log, "$taskName", taskDto.getName());
                        log = StringUtils.replace(log, "$date", DateUtil.toLocalDateTime(g.getCreateAt()).format(DateTimeFormatter.ofPattern(DatePattern.NORM_DATETIME_PATTERN)));

                        return new TaskLogInfoVo(g.getId().toHexString(), g.getGrade(), log);
                    })
                    .collect(Collectors.toCollection(LinkedList::new));

            result.setModelList(collect);

            boolean present = modelLogs.stream()
                    .filter(n -> DagOutputTemplateEnum.MODEL_PROCESS_CHECK.name().equals(n.getCheckType()))
                    .anyMatch(n -> {
                        int size = taskDto.getDag().getSourceNode().getFirst().getTableNames().size();
                        return n.getLog().contains(size + "/" + size);
                    });
            result.setOver(present);
        }

        return result;
    }

    @Override
    public void removeAllByTaskId(String taskId) {
        mongoTemplate.remove(Query.query(Criteria.where("taskId").is(taskId)), TaskDagCheckLog.class);
    }

    public List<TaskDagCheckLog> find(Query query) {
        return mongoTemplate.find(query, TaskDagCheckLog.class);
    }

    @Override
    public void createLog(String taskId, String userId, String grade, DagOutputTemplateEnum templateEnum,
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
        if (StringUtils.equals(Level.INFO.getValue(), grade)) {
            template = templateEnum.getInfoTemplate();
        } else if (StringUtils.equals(Level.ERROR.getValue(), grade)){
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
