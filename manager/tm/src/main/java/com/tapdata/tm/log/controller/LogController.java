package com.tapdata.tm.log.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.log.dto.LogDto;
import com.tapdata.tm.log.service.LogService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;


/**
 * @Date: 2021/09/14
 * @Description:
 */
@Tag(name = "Logs", description = "Logs相关接口")
@RestController
@RequestMapping("/api/Logs")
@Slf4j
public class LogController extends BaseController {

    @Autowired
    private LogService logService;

    /**
     * Create a new instance of the model and persist it into the data source
     * @param logs
     * @return
     */
    @Operation(summary = "Create a new instance of the model and persist it into the data source")
    @PostMapping
    public ResponseMessage<LogDto> save(@RequestBody LogDto logs) {
        logs.setId(null);
        return success(logService.save(logs, getLoginUser()));
    }

    /**
     * Find all instances of the model matched by filter from the data source
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find all instances of the model matched by filter from the data source")
    @GetMapping
    public ResponseMessage<Page<LogDto>> find(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"fields\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(logService.find(filter, getLoginUser()));
    }

    private static Pattern pattern = Pattern.compile("^\\d+$");

    @Operation(summary = "Export logs by where to download")
    @GetMapping("/export")
    public void export(@RequestParam("where") String whereJson, HttpServletResponse response) throws IOException {
        Where where = parseWhere(whereJson);

        Number start = parseNumber(where.get("start"));
        Number end = parseNumber(where.get("end"));
        String dataFlowId = where.containsKey("dataFlowId") ? where.get("dataFlowId").toString() : null;

        if (dataFlowId == null) {
            response.setContentType("application/json");
            response.getWriter().println("{\"code\": \"MissingParameter\", \"message\": \"dataFlowId can't be empty.\"}");
            return;
        }

        String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        String filename = dataFlowId +"-" + date + "-log";

        response.setContentType("application/zip");
        response.setHeader("Content-Disposition", "attachment; filename=\"" + filename + ".zip\"");

        ZipOutputStream zipOutputStream = new ZipOutputStream(response.getOutputStream());
        zipOutputStream.putNextEntry(new ZipEntry(filename + ".log"));

        try {
            logService.export(start, end, dataFlowId, zipOutputStream);
        } catch (Exception e) {
            log.error("export logs failed", e);
        } finally {
            zipOutputStream.closeEntry();
            zipOutputStream.flush();
            zipOutputStream.close();
        }
    }

    private Number parseNumber(Object obj) {
        if (obj instanceof Number) {
            return (Number) obj;
        } else if (obj instanceof String && pattern.matcher((String)obj).matches()) {
            return new BigDecimal((String)obj).longValue();
        }
        return null;
    }

}
