package com.tapdata.tm.task.service.impl;

import cn.hutool.extra.spring.SpringUtil;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.repository.TaskDagCheckLogRepository;
import com.tapdata.tm.task.service.DagLogStrategy;
import com.tapdata.tm.task.service.TaskDagCheckLogService;
import com.tapdata.tm.utils.Lists;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.LinkedList;
import java.util.List;

@Service
@Setter(onMethod_ = {@Autowired})
public class TaskDagCheckLogServiceImpl implements TaskDagCheckLogService {

    private TaskDagCheckLogRepository repository;

    @Override
    public TaskDagCheckLog save(TaskDagCheckLog log) {
        return repository.save(log);
    }

    @Override
    public List<TaskDagCheckLog> saveAll(List<TaskDagCheckLog> logs) {
        return repository.saveAll(logs);
    }

    /**
     * 保存时：
     * 1.任务设置检测
     * 2.源节点设置检测
     * 3.JS节点设置检测
     * 4.表编辑节点设置检测
     * 5.字段编辑节点设置检测
     * 6.目标节点设置检测
     * @param taskDto 任务信息
     */
    @Override
    public List<TaskDagCheckLog> checkWhenSave(TaskDto taskDto, UserDetail userDetail) {
        List<TaskDagCheckLog> result = Lists.newArrayList();

        LinkedList<DagOutputTemplateEnum> checkList = DagOutputTemplateEnum.getSaveCheck();
        checkList.forEach(c -> {
            DagLogStrategy dagLogStrategy = SpringUtil.getBean(c.getBeanName(), DagLogStrategy.class);
            List<TaskDagCheckLog> logs = dagLogStrategy.getLogs(taskDto, userDetail);
            result.addAll(logs);
        });
        return result;
    }

    /**
     * 仅启动时:
     * 1.agent可用性检测
     * 2.源连接检测
     * 3.目标连接检测
     * 4.字符编码检测
     * 5.表名大小写检测
     * 6.模型推演检测
     * 7.数据校验检测
     * @param taskDto 任务信息
     */
    @Override
    public List<TaskDagCheckLog> checkWhenStart(TaskDto taskDto, UserDetail userDetail) {
        List<TaskDagCheckLog> result = Lists.newArrayList();
        LinkedList<DagOutputTemplateEnum> checkList = DagOutputTemplateEnum.getStartCheck();

        checkList.forEach(c -> {
            DagLogStrategy dagLogStrategy = SpringUtil.getBean(c.getBeanName(), DagLogStrategy.class);
            List<TaskDagCheckLog> logs = dagLogStrategy.getLogs(taskDto, userDetail);
            result.addAll(logs);
        });
        return result;
    }
}
