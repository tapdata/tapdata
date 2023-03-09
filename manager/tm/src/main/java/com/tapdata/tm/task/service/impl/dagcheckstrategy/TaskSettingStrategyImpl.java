package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import cn.hutool.core.date.DateUtil;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.service.DagLogStrategy;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MessageUtil;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;

@Component("taskSettingStrategy")
@Setter(onMethod_ = {@Autowired})
public class TaskSettingStrategyImpl implements DagLogStrategy {
    
    private TaskService taskService;
    
    private final DagOutputTemplateEnum templateEnum = DagOutputTemplateEnum.TASK_SETTING_CHECK;
    
    @Override
    public List<TaskDagCheckLog> getLogs(TaskDto taskDto, UserDetail userDetail, Locale locale) {
        ObjectId taskId = taskDto.getId();
        String taskName = taskDto.getName();
        String current = DateUtil.now();
        Date now = new Date();

        Query query = new Query(Criteria.where("name").is(taskName).and("_id").ne(taskId).and("is_deleted").ne(true));
        List<TaskDto> dtos = taskService.findAllDto(query, userDetail);
        String template;
        Level grade;
        if (CollectionUtils. isEmpty(dtos)) {
            template = MessageUtil.getDagCheckMsg(locale, "TASK_SETTING_INFO");
            grade = Level.INFO;
        } else {
            template = MessageUtil.getDagCheckMsg(locale, "TASK_SETTING_ERROR");
            grade = Level.ERROR;
        }

        String content = MessageFormat.format(template, current);
        
        TaskDagCheckLog log = new TaskDagCheckLog();
        log.setTaskId(taskId.toHexString());
        log.setCheckType(templateEnum.name());
        log.setCreateAt(now);
        log.setCreateUser(userDetail.getUserId());
        log.setLog(content);
        log.setGrade(grade);

        return Lists.newArrayList(log);
    }
}
