package com.tapdata.tm.dql.service;

import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dql.vo.DqlEventQueryVo;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.permissions.constants.DataPermissionActionEnums;
import com.tapdata.tm.permissions.constants.DataPermissionDataTypeEnums;
import com.tapdata.tm.permissions.constants.DataPermissionMenuEnums;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.MongoUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

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

    /**
     * Checks the exception-events menu and resolves the task data scope for a query.
     * A task-qualified query is narrowed to that one task after its visibility is checked;
     * an unqualified query is narrowed to the task ids returned by the existing task
     * repository data-permission filter.
     */
    public Set<String> resolveVisibleTaskIds(DqlEventQueryVo query, UserDetail userDetail) {
        checkMenuVisible(userDetail);
        if (query != null && StringUtils.isNotBlank(query.getTaskId())) {
            checkTaskVisible(query.getTaskId(), userDetail);
            return Set.of(query.getTaskId());
        }
        return visibleTaskIds(userDetail);
    }

    private Set<String> visibleTaskIds(UserDetail userDetail) {
        Query query = new Query();
        query.fields().include("_id");
        List<TaskEntity> tasks = taskService.findAll(query, userDetail);
        if (tasks == null || tasks.isEmpty()) {
            return Collections.emptySet();
        }
        return tasks.stream()
                .map(TaskEntity::getId)
                .filter(Objects::nonNull)
                .map(ObjectId::toHexString)
                .collect(Collectors.toUnmodifiableSet());
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
        throw new BizException(EX_CODE_NO_PERMISSION);
    }
}
