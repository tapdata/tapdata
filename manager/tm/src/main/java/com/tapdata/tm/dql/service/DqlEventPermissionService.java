package com.tapdata.tm.dql.service;

import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.permissions.constants.DataPermissionActionEnums;
import com.tapdata.tm.permissions.constants.DataPermissionDataTypeEnums;
import com.tapdata.tm.permissions.constants.DataPermissionMenuEnums;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.MongoUtils;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Service;

@Service
public class DqlEventPermissionService {
    private static final String EX_CODE_NO_PERMISSION = "NoPermission";
    private final TaskService taskService;

    public DqlEventPermissionService(TaskService taskService) {
        this.taskService = taskService;
    }

    public void checkMenuVisible(UserDetail userDetail) {
        DataPermissionHelper.check(
                userDetail,
                DataPermissionMenuEnums.ExceptionEvents,
                DataPermissionActionEnums.View,
                DataPermissionDataTypeEnums.Task,
                null,
                () -> true,
                this::throwNoPermission
        );
    }

    public void checkTaskVisible(String taskId, UserDetail userDetail) {
        checkTaskAction(taskId, userDetail, DataPermissionActionEnums.View);
    }

    public void checkTaskEditable(String taskId, UserDetail userDetail) {
        checkTaskAction(taskId, userDetail, DataPermissionActionEnums.Edit);
    }

    private void checkTaskAction(String taskId, UserDetail userDetail, DataPermissionActionEnums action) {
        if (taskId == null || !ObjectId.isValid(taskId)) {
            throwNoPermission();
            return;
        }
        ObjectId objectId = MongoUtils.toObjectId(taskId);
        DataPermissionHelper.checkOfQuery(
                userDetail,
                DataPermissionDataTypeEnums.Task,
                action,
                taskService.dataPermissionFindById(objectId, new Field()),
                (dto) -> DataPermissionMenuEnums.ofTaskSyncType(dto.getSyncType()),
                () -> true,
                this::throwNoPermission
        );
    }

    private <T> T throwNoPermission() {
        throw new RuntimeException(EX_CODE_NO_PERMISSION);
    }
}
