package com.tapdata.tm.skiperrortable.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.permissions.constants.DataPermissionActionEnums;
import com.tapdata.tm.permissions.constants.DataPermissionDataTypeEnums;
import com.tapdata.tm.permissions.constants.DataPermissionMenuEnums;
import com.tapdata.tm.skiperrortable.SkipErrorTableStatusEnum;
import com.tapdata.tm.skiperrortable.dto.TaskSkipErrorTableDto;
import com.tapdata.tm.skiperrortable.service.ITaskSkipErrorTableService;
import com.tapdata.tm.skiperrortable.vo.SkipErrorTableRecoveredVo;
import com.tapdata.tm.skiperrortable.vo.SkipErrorTableStatusVo;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.MongoUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import org.bson.types.ObjectId;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * 任务-错误表跳过-实体
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/8/21 15:23 Create
 */
@Tag(name = "ClusterState", description = "数据源模型相关接口")
@RestController
@RequestMapping({"/api/Task", "/api/task"})
public class TaskSkipErrorTableController extends BaseController {
    private static final String EX_CODE_NO_PERMISSION = "NoPermission";

    private final ITaskSkipErrorTableService service;
    private final TaskService taskService;

    public TaskSkipErrorTableController(ITaskSkipErrorTableService service, TaskService taskService) {
        this.service = service;
        this.taskService = taskService;
    }

    private <T> T throwNoPermission() {
        throw new RuntimeException(EX_CODE_NO_PERMISSION);
    }

    private <T> T checkAndCallback(HttpServletRequest request, UserDetail userDetail, ObjectId id, DataPermissionActionEnums action, Supplier<T> supplier) {
        id = Optional.ofNullable(DataPermissionHelper.signDecode(request, id.toHexString())).map(MongoUtils::toObjectId).orElse(id);
        return DataPermissionHelper.checkOfQuery(
                userDetail,
                DataPermissionDataTypeEnums.Task,
                action,
                taskService.dataPermissionFindById(id, new Field()),
                (dto) -> DataPermissionMenuEnums.ofTaskSyncType(dto.getSyncType()),
                supplier,
                this::throwNoPermission
        );
    }

    private void checkOfTaskId(HttpServletRequest request, UserDetail userDetail, String taskId, DataPermissionActionEnums action) {
        if (null == taskId
                || !ObjectId.isValid(taskId)
                || !checkAndCallback(request, userDetail, new ObjectId(taskId), action, () -> true)
        ) {
            throwNoPermission();
        }
    }

    @Operation(summary = "FE 回调接口，用于上报跳过的错误表信息")
    @PostMapping("/{taskId}/skip-error-table")
    public ResponseMessage<TaskSkipErrorTableDto> addSkipTable(HttpServletRequest request
            , @PathVariable(name = "taskId") String taskId
            , @RequestBody TaskSkipErrorTableDto dto
    ) {
        UserDetail userDetail = getLoginUser();
        checkOfTaskId(request, userDetail, taskId, DataPermissionActionEnums.Edit);

        dto.setTaskId(taskId);
        TaskSkipErrorTableDto newDto = service.addSkipTable(dto);
        return success(newDto);
    }

    @Operation(summary = "FE 回调接口，错误表恢复")
    @PostMapping("/{taskId}/skip-error-table-recovered")
    public ResponseMessage<Boolean> deleteByTableNames(HttpServletRequest request
            , @PathVariable(name = "taskId") String taskId
            , @RequestBody SkipErrorTableRecoveredVo vo
    ) {
        UserDetail userDetail = getLoginUser();
        checkOfTaskId(request, userDetail, taskId, DataPermissionActionEnums.Edit);

        if (null == vo) {
            return failed(new BizException("IllegalArgument", "vo"));
        } else if (null == vo.getSourceTables() || vo.getSourceTables().isEmpty()) {
            return failed(new BizException("IllegalArgument", "sourceTables"));
        }

        long modifyCounts = service.deleteByTaskId(taskId, vo.getSourceTables());
        return success(modifyCounts > 0);
    }

    @Operation(summary = "FE 回调接口，查询操作恢复的表信息")
    @GetMapping("/{taskId}/skip-error-table-status")
    public ResponseMessage<List<SkipErrorTableStatusVo>> queryTableStatus(HttpServletRequest request
            , @PathVariable(name = "taskId") String taskId
    ) {
        UserDetail userDetail = getLoginUser();
        checkOfTaskId(request, userDetail, taskId, DataPermissionActionEnums.Edit);

        List<SkipErrorTableStatusVo> allRecoverTableNames = service.listTableStatus(taskId);
        return success(allRecoverTableNames);
    }

    @Operation(summary = "查询任务中因错误跳过的表信息")
    @GetMapping("/{taskId}/skip-error-table")
    public ResponseMessage<Page<TaskSkipErrorTableDto>> pageByTaskId(HttpServletRequest request
            , @PathVariable(name = "taskId") String taskId
            , @RequestParam(name = "tableFilter", required = false) String tableFilter
            , @RequestParam(name = "skip", required = false, defaultValue = "0") long skip
            , @RequestParam(name = "limit", required = false, defaultValue = "10") int limit
            , @RequestParam(name = "order", required = false) String order
    ) {
        UserDetail userDetail = getLoginUser();
        checkOfTaskId(request, userDetail, taskId, DataPermissionActionEnums.View);

        Page<TaskSkipErrorTableDto> page = service.pageOfTaskId(taskId, tableFilter, skip, limit, order);
        return success(page);
    }

    @Operation(summary = "恢复错误表同步")
    @PutMapping("/{taskId}/skip-error-table-recovering")
    public ResponseMessage<Boolean> changeTable2Recovering(HttpServletRequest request
            , @PathVariable(name = "taskId") String taskId
            , @RequestParam(name = "sourceTables", required = false) List<String> sourceTables
    ) {
        UserDetail userDetail = getLoginUser();
        checkOfTaskId(request, userDetail, taskId, DataPermissionActionEnums.Edit);

        long modifyCounts = service.changeTableStatus(taskId, sourceTables, SkipErrorTableStatusEnum.RECOVERING);
        return success(modifyCounts > 0);
    }

}
