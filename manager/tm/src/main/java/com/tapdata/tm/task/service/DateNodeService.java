package com.tapdata.tm.task.service;


import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;

public interface DateNodeService {

    void checkTaskDateNode(TaskDto taskDto, UserDetail userDetail);


}
