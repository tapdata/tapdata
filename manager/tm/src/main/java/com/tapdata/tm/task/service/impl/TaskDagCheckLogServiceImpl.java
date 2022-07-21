package com.tapdata.tm.task.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.ListUtil;
import cn.hutool.extra.cglib.CglibUtil;
import cn.hutool.extra.spring.SpringUtil;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
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
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
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
        int offset = dto.getOffset();
        int limit = dto.getLimit();

        Criteria criteria = Criteria.where("taskId").is(taskId);
        if (StringUtils.isNotBlank(nodeId)) {
            criteria.and("nodeId").is(nodeId);
        }
        if (StringUtils.isNotBlank(grade)) {
            criteria.and("grade").is(grade);
        }
        if (StringUtils.isNotBlank(keyword)) {
            criteria.regex("log").is(keyword);
        }
        if (offset > 0) {
            criteria.and("_id").gte(offset);
        }
        Query query = new Query(criteria);
        long count = mongoTemplate.count(query, TaskDagCheckLog.class);

        TaskDto taskDto = taskService.findById(MongoUtils.toObjectId(taskId));
        LinkedHashMap<String, String> nodeMap = taskDto.getDag().getNodes().stream()
                .collect(Collectors.toMap(Node::getId, Node::getName,(x, y) -> y, LinkedHashMap::new));

        query.with(Sort.by("createTime"));
        query.with(Pageable.ofSize(limit));
        List<TaskDagCheckLog> taskDagCheckLogs = find(query);
        if (CollectionUtils.isEmpty(taskDagCheckLogs)) {
            TaskDagCheckLogVo taskDagCheckLogVo = new TaskDagCheckLogVo();
            taskDagCheckLogVo.setNodes(nodeMap);
            return taskDagCheckLogVo;
        }

        LinkedList<TaskLogInfoVo> data = taskDagCheckLogs.stream()
                .map(g -> new TaskLogInfoVo(g.getId().toHexString(), g.getGrade(), g.getLog()))
                .collect(Collectors.toCollection(LinkedList::new));

        return new TaskDagCheckLogVo(nodeMap, data, count, data.getLast().getId());
    }

    @Override
    public void removeAllByTaskId(String taskId) {
        mongoTemplate.findAllAndRemove(Query.query(Criteria.where("taskId").is(taskId)), TaskDagCheckLog.class);
    }

    public List<TaskDagCheckLog> find(Query query) {
        return mongoTemplate.find(query, TaskDagCheckLog.class);
    }
}
