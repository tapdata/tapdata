package com.tapdata.tm.notification.record.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.notification.record.dto.NotificationRecordDetailVo;
import com.tapdata.tm.notification.record.dto.NotificationRecordQuery;
import com.tapdata.tm.notification.record.dto.NotificationRecordVo;
import com.tapdata.tm.notification.record.dto.ReadStatePreviewVo;
import com.tapdata.tm.notification.record.dto.ReadStateRequest;
import com.tapdata.tm.notification.record.dto.SystemNotificationInventoryVo;
import com.tapdata.tm.notification.record.service.NotificationRecordService;
import com.tapdata.tm.utils.WebUtils;
import io.swagger.v3.oas.annotations.Operation;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.List;

/**
 * 通知记录（TAP-13052）。旧的 /api/Messages 接口保留兼容。
 */
@RestController
@RequestMapping("/api/notifications")
public class NotificationRecordController extends BaseController {
    private final NotificationRecordService recordService;

    public NotificationRecordController(NotificationRecordService recordService) {
        this.recordService = recordService;
    }

    @Operation(summary = "list notification records aggregated by alarm event")
    @GetMapping
    public ResponseMessage<Page<NotificationRecordVo>> list(@ModelAttribute NotificationRecordQuery query, HttpServletRequest request) {
        return success(recordService.list(query, getLoginUser(), WebUtils.getLocale(request)));
    }

    @Operation(summary = "unread notification count of current user")
    @GetMapping("unread-count")
    public ResponseMessage<Long> unreadCount() {
        return success(recordService.unreadCount(getLoginUser()));
    }

    @Operation(summary = "preview records affected by batch read state change")
    @GetMapping("read-state/preview")
    public ResponseMessage<ReadStatePreviewVo> previewReadState(@ModelAttribute NotificationRecordQuery query,
                                                                @RequestParam("read") boolean read, HttpServletRequest request) {
        return success(recordService.previewReadState(query, read, getLoginUser(), WebUtils.getLocale(request)));
    }

    @Operation(summary = "batch update read state with explicit ids")
    @PatchMapping("read-state")
    public ResponseMessage<Long> updateReadState(@RequestBody ReadStateRequest request) {
        if (request == null || request.getRead() == null) {
            throw new BizException("IllegalArgument", "read");
        }
        return success(recordService.updateReadState(request.getIds(), request.getRead(), getLoginUser(), true));
    }

    @Operation(summary = "historical system notification inventory")
    @GetMapping("system-notification-inventory")
    public ResponseMessage<List<SystemNotificationInventoryVo>> systemNotificationInventory() {
        UserDetail user = getLoginUser();
        if (!user.isRoot()) {
            throw new BizException("NotAuthorized");
        }
        return success(recordService.systemNotificationInventory());
    }

    @Operation(summary = "notification record detail with deliveries")
    @GetMapping("{id}")
    public ResponseMessage<NotificationRecordDetailVo> detail(@PathVariable("id") String id, HttpServletRequest request) {
        return success(recordService.detail(id, getLoginUser(), WebUtils.getLocale(request)));
    }

    @Operation(summary = "update read state of one record")
    @PatchMapping("{id}/read-state")
    public ResponseMessage<Long> updateReadState(@PathVariable("id") String id, @RequestBody ReadStateRequest request) {
        if (request == null || request.getRead() == null) {
            throw new BizException("IllegalArgument", "read");
        }
        return success(recordService.updateReadState(Collections.singletonList(id), request.getRead(), getLoginUser(), false));
    }
}
