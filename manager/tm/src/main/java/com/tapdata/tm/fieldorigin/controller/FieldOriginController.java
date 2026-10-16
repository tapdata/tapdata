package com.tapdata.tm.fieldorigin.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.fieldorigin.dto.FieldOriginQueryParam;
import com.tapdata.tm.fieldorigin.dto.FieldOriginQueryResult;
import com.tapdata.tm.fieldorigin.dto.FieldOriginSnapshot;
import com.tapdata.tm.fieldorigin.dto.FieldOriginVersionDto;
import com.tapdata.tm.fieldorigin.dto.FieldOriginVersionInfo;
import com.tapdata.tm.fieldorigin.service.FieldOriginService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Field origin metadata of task target tables (TAP-13082).
 */
@Tag(name = "FieldOrigin", description = "Field origin metadata of task target tables")
@RestController
@RequestMapping("/api/field-origin")
public class FieldOriginController extends BaseController {
    @Autowired
    private FieldOriginService fieldOriginService;

    @Operation(summary = "Rebuild field origins of a task, no new version is created when nothing changed")
    @PostMapping("/rebuild")
    public ResponseMessage<FieldOriginVersionDto> rebuild(@RequestParam("taskId") String taskId) {
        return success(fieldOriginService.rebuild(taskId, getLoginUser()));
    }

    @Operation(summary = "Batch query origins of changed field paths, a[0].b is normalized to a[].b and container paths return children")
    @PostMapping("/query")
    public ResponseMessage<FieldOriginQueryResult> query(@RequestBody FieldOriginQueryParam param) {
        return success(fieldOriginService.query(param, getLoginUser()));
    }

    @Operation(summary = "All field origins of a version, the active version when version is empty")
    @GetMapping("/snapshot")
    public ResponseMessage<FieldOriginSnapshot> snapshot(@RequestParam("taskId") String taskId,
                                                         @RequestParam(value = "version", required = false) Long version,
                                                         @RequestParam(value = "targetConnectionId", required = false) String targetConnectionId,
                                                         @RequestParam(value = "targetTable", required = false) String targetTable) {
        return success(fieldOriginService.snapshot(taskId, version, targetConnectionId, targetTable, getLoginUser()));
    }

    @Operation(summary = "Active version and latest build status of a task")
    @GetMapping("/version")
    public ResponseMessage<FieldOriginVersionInfo> version(@RequestParam("taskId") String taskId) {
        return success(fieldOriginService.version(taskId, getLoginUser()));
    }
}
