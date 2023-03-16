package com.tapdata.tm.monitoringlogs.controller;

import com.fasterxml.jackson.core.type.TypeReference;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.monitoringlogs.param.MonitoringLogCountParam;
import com.tapdata.tm.monitoringlogs.param.MonitoringLogExportParam;
import com.tapdata.tm.monitoringlogs.param.MonitoringLogQueryParam;
import com.tapdata.tm.monitoringlogs.service.MonitoringLogsService;
import com.tapdata.tm.monitoringlogs.vo.MonitoringLogCountVo;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;


/**
 * @Date: 2022/06/20
 * @Description:
 */
@Tag(name = "MonitoringLogs", description = "MonitoringLogs相关接口")
@RestController
@RequestMapping("/api/MonitoringLogs")
@Slf4j
public class MonitoringLogsController extends BaseController {

    @Autowired
    private MonitoringLogsService monitoringLogsService;

    /**
     * batch into the data source
     *
     * @param monitoringLoges
     * @return
     */
    @Operation(summary = "Create a new batch of instances of the model and persist it into the data source")
    @PostMapping("batch")
    public ResponseMessage<List<MonitoringLogsDto>> save(@RequestBody List<MonitoringLogsDto> monitoringLoges) {
        for (MonitoringLogsDto monitoringLoge : monitoringLoges) {
            monitoringLoge.setId(null);
        }
        monitoringLogsService.batchSave(monitoringLoges, getLoginUser());

        return success();
    }

    @Operation(summary = "Create a new batch of instances of the model and persist it into the data source")
    @PostMapping("batchJson")
    public ResponseMessage<List<MonitoringLogsDto>> batchJson(@RequestBody List<String> list) {
        if (CollectionUtils.isEmpty(list)) {
            return success();
        }
        List<MonitoringLogsDto> monitoringLoges = new ArrayList<>();
        for (String json : list) {
            MonitoringLogsDto monitoringLogsDto = JsonUtil.parseJsonUseJackson(json, new TypeReference<MonitoringLogsDto>() {
            });
            monitoringLoges.add(monitoringLogsDto);
        }
        for (MonitoringLogsDto monitoringLoge : monitoringLoges) {
            monitoringLoge.setId(null);
        }
        monitoringLogsService.batchSave(monitoringLoges, getLoginUser());

        return success();
    }

    /**
     * Find all instances of the model matched by filter from the data source with pagination.
     */
    @Operation(summary = "Find all instances of the model matched by filter from the data source")
    @PostMapping("query")
    public ResponseMessage<Page<MonitoringLogsDto>> query(
            @RequestBody MonitoringLogQueryParam param) {
        return success(monitoringLogsService.query(param));
    }

    /**
     * Find all instances of the model matched by filter from the data source with pagination.
     */
    @Operation(summary = "Find all instances of the model matched by filter from the data source")
    @PostMapping("count")
    public ResponseMessage<List<MonitoringLogCountVo>> count(
            @RequestBody MonitoringLogCountParam param) {
        return success(monitoringLogsService.count(param.getTaskId(), param.getTaskRecordId()));
    }

    /**
     * Export all instances of the monitoring logs matched by filter from the data source.
     */
    @Operation(summary = "Export all instances of the monitoring logs matched by filter from the data source.")
    @PostMapping("export")
    public void export(
            @RequestBody MonitoringLogExportParam param,
            HttpServletResponse response
    ) throws IOException {
        String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        String filename = param.getTaskId() +"-" + date + "-log";

        response.setContentType("application/zip");
        response.setHeader("Content-Disposition", "attachment; filename=\"" + filename + ".zip\"");
        ZipOutputStream zipOutputStream = new ZipOutputStream(response.getOutputStream());
        zipOutputStream.putNextEntry(new ZipEntry(filename + ".log"));
        try {
            monitoringLogsService.export(param, zipOutputStream);
        } catch (Exception e) {
            log.error("export monitoring logs failed", e);
        } finally {
            zipOutputStream.closeEntry();
            zipOutputStream.flush();
            zipOutputStream.close();
        }
    }
}