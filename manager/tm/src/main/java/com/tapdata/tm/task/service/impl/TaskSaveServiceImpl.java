package com.tapdata.tm.task.service.impl;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.task.constant.HaveCheckLogException;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.service.TaskDagCheckLogService;
import com.tapdata.tm.task.service.TaskSaveService;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
@Setter(onMethod_ = {@Autowired})
public class TaskSaveServiceImpl implements TaskSaveService {

    private TaskDagCheckLogService taskDagCheckLogService;

    @Override
    public void taskSaveCheckLog(TaskDto taskDto, UserDetail userDetail) throws HaveCheckLogException {
        List<TaskDagCheckLog> taskDagCheckLogs = taskDagCheckLogService.dagCheck(taskDto, userDetail, true);
        if (CollectionUtils.isNotEmpty(taskDagCheckLogs)) {
            Optional<TaskDagCheckLog> any = taskDagCheckLogs.stream().filter(log -> StringUtils.equals(Level.ERROR.getValue(), log.getGrade())).findAny();
            if (any.isPresent()) {
                throw new HaveCheckLogException("hava error log");
            }
        }
    }
}
