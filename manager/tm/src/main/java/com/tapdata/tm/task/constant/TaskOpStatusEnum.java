package com.tapdata.tm.task.constant;

import com.tapdata.tm.utils.Lists;

import java.util.List;

import static com.tapdata.tm.commons.task.dto.TaskDto.*;

/**
 * @Author: Zed
 * @Date: 2022/3/11
 * @Description:
 */
public enum TaskOpStatusEnum {
    to_start_status(Lists.of(STATUS_EDIT, STATUS_STOP, STATUS_COMPLETE, STATUS_ERROR, STATUS_SCHEDULE_FAILED, STATUS_WAIT_START)),
    to_stop_status(Lists.of(STATUS_RUNNING, STATUS_WAIT_RUN, STATUS_STOPPING)),
    to_renew_status(Lists.of(STATUS_EDIT, STATUS_WAIT_START, STATUS_STOP, STATUS_COMPLETE, STATUS_ERROR, STATUS_SCHEDULE_FAILED, STATUS_RENEW_FAILED)),
    to_error_status(Lists.of(STATUS_RUNNING, STATUS_STOPPING)),
    to_complete_status(Lists.of(STATUS_RUNNING, STATUS_STOPPING)),
    to_delete_status(Lists.of(STATUS_EDIT, STATUS_WAIT_START, STATUS_STOP, STATUS_COMPLETE, STATUS_ERROR, STATUS_SCHEDULE_FAILED, STATUS_RENEW_FAILED, STATUS_DELETE_FAILED, STATUS_RENEWING)),
    ;



    private final List<String> statusList;

    TaskOpStatusEnum(List<String> statusList) {
        this.statusList = statusList;
    }

    public List<String> v() {
        return statusList;
    }

}
