package com.tapdata.tm.alarm.controller;

import com.alibaba.fastjson.JSON;
import com.tapdata.tm.alarm.dto.AlarmChannelDto;
import com.tapdata.tm.alarm.dto.AlarmListInfoVo;
import com.tapdata.tm.alarm.dto.AlarmListReqDto;
import com.tapdata.tm.alarm.dto.TaskAlarmInfoVo;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.alarm.service.AlarmReceiverAccess;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.alarm.AlarmDatasourceDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiverCandidates;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiverPreview;
import com.tapdata.tm.commons.task.dto.alarm.AlarmVO;
import com.tapdata.tm.commons.task.dto.alarm.LegacyEmailReceivers;
import com.tapdata.tm.commons.task.dto.alarm.PreviewEmailMask;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.alarm.TaskAlertRequest;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.permissions.constants.DataPermissionActionEnums;
import com.tapdata.tm.permissions.constants.DataPermissionDataTypeEnums;
import com.tapdata.tm.permissions.constants.DataPermissionMenuEnums;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.MongoUtils;
import org.bson.types.ObjectId;
import com.tapdata.tm.message.dto.MessageDto;
import com.tapdata.tm.utils.WebUtils;
import io.swagger.v3.oas.annotations.Operation;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Locale;

/**
 * @author jiuyetx
 * @date 2022/9/7
 */
@RestController
@RequestMapping("/api/alarm")
@Setter(onMethod_ = {@Autowired})
@Slf4j
public class AlarmController extends BaseController {
    private AlarmService alarmService;
    private TaskService taskService;
    private AlarmReceiverAccess alarmReceiverAccess;

    @Operation(summary = "find all alarm")
    @GetMapping("list")
    public ResponseMessage<Page<AlarmListInfoVo>> list(@RequestParam(required = false)String status,
                                                       @RequestParam(required = false)Long start,
                                                       @RequestParam(required = false)Long end,
                                                       @RequestParam(required = false)String keyword,
                                                       @RequestParam(defaultValue = "1")Integer page,
                                                       @RequestParam(defaultValue = "20")Integer size,
                                                       HttpServletRequest request) {
        Locale locale = WebUtils.getLocale(request);
        return success(alarmService.list(status, start, end, keyword, page, size, getLoginUser(), locale));
    }

    @Operation(summary = "find all alarm by task")
    @PostMapping("list_task")
    public ResponseMessage<TaskAlarmInfoVo> findListByTask(@RequestBody AlarmListReqDto dto, HttpServletRequest request) {
        Locale locale = WebUtils.getLocale(request);
        dto.setLocale(locale);
        return success(alarmService.listByTask(dto));
    }

    @Operation(summary = "close alarm")
    @PostMapping("close")
    public ResponseMessage<Void> close(@RequestParam String[] ids) {
        alarmService.close(ids, getLoginUser());
        return success();
    }

    /**
     * 目前只有agent的状态变动的时候，tcm调用该方法
     * @param messageDto
     * @return
     */
    @Operation(summary = "新增消息")
    @PostMapping("addMsg")
    public ResponseMessage<MessageDto> addMsg(@RequestBody MessageDto messageDto) {
        log.info("接收到新增信息请求  ,  messageDto:{}", JSON.toJSONString(messageDto));
        MessageDto messageDtoRet = alarmService.add(messageDto,getLoginUser());
        return success(messageDtoRet);
    }

    @Operation(summary = "新增数据源监控消息")
    @PostMapping("addDatasourceMsg")
    public ResponseMessage<MessageDto> addMsg(@RequestBody AlarmDatasourceDto alarmDatasourceDto) {
        log.info("接收到新增信息请求  ,  alarmDatasourceDto:{}", JSON.toJSONString(alarmDatasourceDto));
        AlarmInfo alarmInfo = new AlarmInfo();
        BeanUtils.copyProperties(alarmDatasourceDto, alarmInfo);
        alarmService.save(alarmInfo);
        return success();
    }

    @Operation(summary = "Ingest structured task alert from engine")
    @PostMapping("task-alerts")
    public ResponseMessage<Void> ingestTaskAlert(@RequestBody TaskAlertRequest request) {
        alarmService.ingestTaskAlert(request);
        return success();
    }

    @Operation(summary = "Get available notification channels")
    @GetMapping("channels")
    public ResponseMessage<List<AlarmChannelDto>> alarmChannel (){
        return success(alarmService.getAvailableChannels());
    }

    @PostMapping("/updateTaskAlarm")
    public ResponseMessage<Void> updateTaskAlarm(HttpServletRequest request, @RequestBody AlarmVO alarm){
        UserDetail user = getLoginUser();
        ObjectId taskId = MongoUtils.toObjectId(alarm.getTaskId());
        if (taskId == null) {
            throw new BizException("IllegalArgument", "taskId");
        }
        checkTask(request, user, taskId, DataPermissionActionEnums.Edit, () -> {
            TaskDto current = taskService.findByTaskId(taskId, "alarmReceivers", "emailReceivers");
            if (LegacyEmailReceivers.needsNormalize(alarm)) {
                LegacyEmailReceivers.normalize(alarm, current == null ? null : current.getEmailReceivers());
            }
            // 不复刻 service 的写入判定：只要请求带了相对任务当前列表新增的 USER / USER_GROUP 就校验（未改动的条目零开销放行）
            alarmReceiverAccess.checkNewReferences(user, alarm.getAlarmReceivers(), current == null ? null : current.getAlarmReceivers());
            alarmService.updateTaskAlarm(alarm, user);
            return null;
        });
        return success();
    }

    @GetMapping("/receiverPreview")
    public ResponseMessage<AlarmReceiverPreview> receiverPreview(HttpServletRequest request, @RequestParam String taskId) {
        UserDetail user = getLoginUser();
        ObjectId id = MongoUtils.toObjectId(taskId);
        if (id == null) {
            throw new BizException("IllegalArgument", "taskId");
        }
        AlarmReceiverPreview preview = checkTask(request, user, id, DataPermissionActionEnums.View,
                () -> alarmService.previewReceivers(taskId, user == null ? null : user.getUserId()));
        // 只有任务查看权限时，不能借预览拿到组成员、系统默认收件人的地址
        if (preview != null && !alarmReceiverAccess.canViewUserDirectory(user)) {
            TaskDto task = taskService.findByTaskId(id, "alarmReceivers", "emailReceivers");
            PreviewEmailMask.mask(preview, task == null ? java.util.Set.of()
                    : PreviewEmailMask.explicitEmails(task.getAlarmReceivers(), task.getEmailReceivers()));
        }
        return success(preview);
    }

    @GetMapping("/receiverCandidates")
    public ResponseMessage<AlarmReceiverCandidates> receiverCandidates(HttpServletRequest request,
                                                                        @RequestParam(required = false) String taskId,
                                                                        @RequestParam(required = false) List<String> taskIds) {
        UserDetail user = getLoginUser();
        java.util.Set<String> ids = new java.util.LinkedHashSet<>();
        if (taskId != null && !taskId.isBlank()) {
            ids.add(taskId);
        }
        if (taskIds != null) {
            ids.addAll(taskIds);
        }
        List<String> editableTaskIds = new java.util.ArrayList<>();
        for (String id : ids) {
            ObjectId objectId = MongoUtils.toObjectId(id);
            if (objectId != null && Boolean.TRUE.equals(
                    checkTask(request, user, objectId, DataPermissionActionEnums.Edit, () -> true, () -> false))) {
                editableTaskIds.add(objectId.toHexString());
            }
        }
        if (editableTaskIds.isEmpty()) {
            throw insufficientPermissions(DataPermissionActionEnums.Edit);
        }
        // 没有用户管理权限时只给本人所在组（含子组）的成员和组，外加这些任务已引用的接收人；邮箱一律不返回
        boolean fullDirectory = alarmReceiverAccess.canViewUserDirectory(user);
        AlarmReceiverCandidates candidates = alarmService.receiverCandidates(
                fullDirectory ? null : user.getUserId(), editableTaskIds);
        if (!fullDirectory && candidates != null && candidates.getUsers() != null) {
            for (AlarmReceiverCandidates.CandidateUser candidateUser : candidates.getUsers()) {
                if (candidateUser != null) {
                    candidateUser.setEmail(null);
                }
            }
        }
        return success(candidates);
    }

    private <T> T checkTask(HttpServletRequest request, UserDetail user, ObjectId id, DataPermissionActionEnums action, java.util.function.Supplier<T> supplier) {
        return checkTask(request, user, id, action, supplier, () -> {
            throw insufficientPermissions(action);
        });
    }

    private <T> T checkTask(HttpServletRequest request, UserDetail user, ObjectId id, DataPermissionActionEnums action,
                            java.util.function.Supplier<T> supplier, java.util.function.Supplier<T> unAuthSupplier) {
        // 子任务带 parent_task_sign 时按父任务校验
        ObjectId decoded = java.util.Optional.ofNullable(DataPermissionHelper.signDecode(request, id.toHexString()))
                .map(MongoUtils::toObjectId).orElse(id);
        return DataPermissionHelper.checkOfQuery(
                user,
                DataPermissionDataTypeEnums.Task,
                action,
                taskService.dataPermissionFindById(decoded, new Field()),
                dto -> DataPermissionMenuEnums.ofTaskSyncType(dto.getSyncType()),
                supplier,
                unAuthSupplier);
    }

    private BizException insufficientPermissions(DataPermissionActionEnums action) {
        return new BizException("insufficient.permissions",
                needAction(DataPermissionDataTypeEnums.Task, java.util.List.of(action)),
                needAction(DataPermissionDataTypeEnums.Task, java.util.List.of(action)));
    }

    @Operation(summary = "add Task Retry Message")
    @PostMapping("update")
    public ResponseMessage<Void> updateByWhere(@RequestParam("where") String whereJson, @RequestBody String reqBody) {
        Where where = parseWhere(whereJson);
        Document update = Document.parse(reqBody);
        if(where.containsKey("taskId")){
            alarmService.taskRetryAlarm((String) where.get("taskId"),(Document)update.get("$set"));
        }
        return success();
    }
}
