package com.tapdata.tm.task.service.impl;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import cn.hutool.extra.spring.SpringUtil;
import com.alibaba.fastjson.JSON;
import com.google.common.base.Splitter;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.NodeEnum;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.CustomProcessorNode;
import com.tapdata.tm.commons.dag.process.JsProcessorNode;
import com.tapdata.tm.commons.dag.process.MigrateJsProcessorNode;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.monitor.dto.TaskLogDto;
import com.tapdata.tm.monitoringlogs.service.MonitoringLogsService;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.repository.TaskDagCheckLogRepository;
import com.tapdata.tm.task.service.DagLogStrategy;
import com.tapdata.tm.task.service.TaskDagCheckLogService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.utils.CacheUtils;
import com.tapdata.tm.task.vo.TaskDagCheckLogVo;
import com.tapdata.tm.task.vo.TaskLogInfoVo;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MessageUtil;
import com.tapdata.tm.utils.MongoUtils;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

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
    private MetadataInstancesService metadataInstancesService;

    @Override
    public TaskDagCheckLog save(TaskDagCheckLog log) {
        return repository.save(log);
    }

    @Override
    public List<TaskDagCheckLog> saveAll(List<TaskDagCheckLog> logs) {
        return repository.saveAll(logs);
    }

    @Override
    public List<TaskDagCheckLog> dagCheck(TaskDto taskDto, UserDetail userDetail, boolean startTask, Locale locale) {

        if(taskDto.getDag().checkMultiDag()){
            throw new BizException("This system does not support multiple links. Please edit and try again.");
        }

        List<TaskDagCheckLog> result = Lists.newArrayList();
        if (!Lists.newArrayList(TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC).contains(taskDto.getSyncType())) {
            return result;
        }
        LinkedList<DagOutputTemplateEnum> checkList = startTask ? DagOutputTemplateEnum.getStartCheck() : DagOutputTemplateEnum.getSaveCheck();
        checkList.forEach(c -> {
            DagLogStrategy dagLogStrategy = SpringUtil.getBean(c.getBeanName(), DagLogStrategy.class);
            List<TaskDagCheckLog> logs = dagLogStrategy.getLogs(taskDto, userDetail, locale);
            if (CollectionUtils.isNotEmpty(logs)) {
                result.addAll(logs);
            }
        });
        return result;
    }

    @Override
    public TaskDagCheckLogVo getLogs(TaskLogDto dto, UserDetail userDetail, Locale locale) {
        String taskId = dto.getTaskId();
        String nodeId = dto.getNodeId();
        String grade = dto.getGrade();
        String keyword = dto.getKeyword();

        TaskDto taskDto = taskService.findById(MongoUtils.toObjectId(taskId));
        boolean transformed = taskDto.getTransformed();
        String cacheKey = "dagCheck-" + taskId;
        List<TaskDagCheckLog> checkLogs;
        if (CacheUtils.isExist(cacheKey)) {
            checkLogs = (List<TaskDagCheckLog>) CacheUtils.get(cacheKey);
        } else {
            checkLogs = this.dagCheck(taskDto, userDetail, dto.isStartTask(), locale);
        }

        // check task nodes has js node
        DAG dag = taskDto.getDag();
        Optional<Node> jsNode = dag.getNodes().stream()
                .filter(n -> (n instanceof MigrateJsProcessorNode || n instanceof JsProcessorNode) && !(n instanceof CustomProcessorNode))
                .findFirst();
        if (jsNode.isPresent()) {
            List<TaskDagCheckLog> jsNodeLog = monitoringLogsService.getJsNodeLog(taskDto.getTransformTaskId(), taskDto.getName(), NodeEnum.valueOf(jsNode.get().getType()).getNodeName());
            Optional.ofNullable(jsNodeLog).ifPresent(checkLogs::addAll);
        }

        if (Objects.nonNull(checkLogs)) {
            CacheUtils.put(cacheKey, checkLogs);
        }

        if (transformed) {
            // check each node schema
            List<String> tableNames = dag.getTargets().stream()
                    .filter(node -> node instanceof DatabaseNode || node instanceof TableNode)
                    .flatMap(node -> {
                        if (node instanceof DatabaseNode) {
                            return ((DatabaseNode) node).getSyncObjects().get(0).getObjectNames().stream();
                        } else {
                            return Lists.newArrayList(((TableNode) node).getTableName()).stream();
                        }
                    }).collect(Collectors.toList());

            List<MetadataInstancesDto> allSchemaList = metadataInstancesService.findByTaskId(taskId, userDetail);
            HashMap<String, List<String>> nodeSchemaMap = allSchemaList.stream()
                    .collect(Collectors.groupingBy(MetadataInstancesDto::getNodeId, HashMap::new,
                            Collectors.mapping(MetadataInstancesDto::getName, Collectors.toList())));
            boolean schemaPass = true;
            for (Node node : dag.getNodes()) {
                if (dag.getSourceNodes().contains(node)) {
                    continue;
                }
                List<String> nodeList = nodeSchemaMap.get(node.getId());
                if (CollectionUtils.isEmpty(nodeList)) {
                    String template = MessageUtil.getDagCheckMsg(locale, "MODEL_PROCESS_FAIL");
                    TaskDagCheckLog modelLog = this.createLog(taskId, nodeId, userDetail.getUserId(), Level.ERROR, DagOutputTemplateEnum.MODEL_PROCESS_CHECK, template, JSON.toJSONString(tableNames));
                    checkLogs.add(modelLog);

                    schemaPass = false;
                    break;
                } else if ((TaskDto.SYNC_TYPE_MIGRATE.equals(taskDto.getSyncType()) && nodeList.size() < tableNames.size())) {
                    List temp = new ArrayList(tableNames);
                    temp.removeAll(nodeList);

                    String template = MessageUtil.getDagCheckMsg(locale, "MODEL_PROCESS_FAIL");
                    TaskDagCheckLog modelLog = this.createLog(taskId, nodeId, userDetail.getUserId(), Level.ERROR, DagOutputTemplateEnum.MODEL_PROCESS_CHECK, template, JSON.toJSONString(temp));
                    checkLogs.add(modelLog);

                    schemaPass = false;
                    break;
                }
            }

            if (schemaPass) {
                String template = MessageUtil.getDagCheckMsg(locale, "MODEL_PROCESS_INFO");

                int number;
                if (TaskDto.SYNC_TYPE_SYNC.equals(taskDto.getSyncType())) {
                    number = 1;
                } else {
                    number = dag.getSourceNode().getFirst().getTableNames().size();
                }
                TaskDagCheckLog modelLog = this.createLog(taskId, null, userDetail.getUserId(), Level.INFO, DagOutputTemplateEnum.MODEL_PROCESS_CHECK, template, number, number);
                assert checkLogs != null;
                checkLogs.add(modelLog);
            }
            // remove cache
            CacheUtils.invalidate(cacheKey);
        }

        LinkedHashMap<String, String> nodeMap = dag.getNodes().stream()
                .collect(Collectors.toMap(Node::getId, Node::getName,(x, y) -> y, LinkedHashMap::new));

        List<TaskDagCheckLog> checkLogList;
        if (CollectionUtils.isEmpty(checkLogs)) {
            TaskDagCheckLogVo taskDagCheckLogVo = new TaskDagCheckLogVo();
            taskDagCheckLogVo.setNodes(nodeMap);
            return taskDagCheckLogVo;
        } else {
            checkLogList = checkLogs.stream().filter(g -> {
                boolean nodeFlag = true;
                if (StringUtils.isNotBlank(nodeId)) {
                    nodeFlag = nodeId.equals(g.getNodeId());
                }

                boolean gradeFlag = true;
                if (StringUtils.isNotBlank(grade)) {
                    if (grade.contains(",")) {
                        gradeFlag = Splitter.on(",").trimResults().splitToList(grade).contains(g.getGrade().name());
                    } else {
                        gradeFlag = grade.equals(g.getGrade().name());
                    }
                }

                boolean keywordFlag = true;
                if (StringUtils.isNotBlank(keyword)) {
                    keywordFlag = g.getLog().contains(keyword);
                }

                return nodeFlag && gradeFlag && keywordFlag;
            }).collect(Collectors.toList());
        }

        LinkedList<TaskLogInfoVo> data = packCheckLogs(taskDto, checkLogList);
        TaskDagCheckLogVo result = new TaskDagCheckLogVo(nodeMap, data, null, 0, 0, false);

        LinkedList<TaskLogInfoVo> all = new LinkedList<>(data);
        result.setOver(transformed);

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

                    TaskLogInfoVo taskLogInfoVo = new TaskLogInfoVo(UUID.randomUUID().toString(), g.getGrade(), log);
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
    public TaskDagCheckLog createLog(String taskId, String nodeId, String userId, Level grade, DagOutputTemplateEnum templateEnum, String template, Object ... param) {
        Date now = new Date();
        String content = MessageFormat.format(template, param);

        TaskDagCheckLog log = TaskDagCheckLog.builder()
                .taskId(taskId).nodeId(nodeId)
                .checkType(templateEnum.name())
                .grade(grade).log(content).build();
        log.setCreateAt(now);
        log.setCreateUser(userId);
        return log;
    }
}
