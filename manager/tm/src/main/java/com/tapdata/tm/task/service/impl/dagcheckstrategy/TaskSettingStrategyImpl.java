package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import cn.hutool.core.date.DateUtil;
import com.google.common.collect.Maps;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
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
import java.util.*;
import java.util.stream.Collectors;

@Component("taskSettingStrategy")
@Setter(onMethod_ = {@Autowired})
public class TaskSettingStrategyImpl implements DagLogStrategy {
    
    private TaskService taskService;
    private DataSourceService dataSourceService;

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

        List<TaskDagCheckLog> result = Lists.newArrayList(log);

        List<String> sourceList = taskDto.getDag().getSourceNodes().stream().map(node -> ((DataParentNode) node).getConnectionId()).collect(Collectors.toList());
        List<String> targetList = taskDto.getDag().getTargets().stream().map(node -> ((DataParentNode) node).getConnectionId()).collect(Collectors.toList());

        List<String> all = Lists.newArrayList();
        all.addAll(sourceList);
        all.addAll(targetList);

        List<DataSourceConnectionDto> connectionDtos = dataSourceService.findAllByIds(all);
        Map<String, Object> timeZoneMap = connectionDtos.stream()
                .filter(dto -> Objects.nonNull(dto.getConfig().get("timezone")))
                .collect(Collectors.toMap(con -> con.getId().toHexString(), dto -> dto.getConfig().get("timezone"), (pre, after) -> pre));
        if (!timeZoneMap.isEmpty()) {
            List<Object> sourceTimeZoneList = sourceList.stream().map(timeZoneMap::get).distinct().collect(Collectors.toList());
            List<Object> targetTimeZoneList = sourceList.stream().map(timeZoneMap::get).distinct().collect(Collectors.toList());

            if (CollectionUtils.isNotEmpty(sourceTimeZoneList)) {
                sourceTimeZoneList.removeAll(targetTimeZoneList);
                if (CollectionUtils.isNotEmpty(sourceTimeZoneList)) {
                    TaskDagCheckLog checkLog = TaskDagCheckLog.builder().taskId(taskId.toHexString()).checkType(templateEnum.name())
                            .grade(Level.WARN)
                            .log(MessageUtil.getDagCheckMsg(locale, "TASK_SETTING_TIMEZONE_CHECK"))
                            .build();
                    checkLog.setCreateAt(now);
                    checkLog.setCreateUser(userDetail.getUserId());
                    result.add(checkLog);
                }
            }
        }

        return result;
    }
}
