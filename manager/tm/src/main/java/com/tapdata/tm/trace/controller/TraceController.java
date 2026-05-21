package com.tapdata.tm.trace.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.trace.dto.TaskLineageDto;
import com.tapdata.tm.trace.param.TaskLineageParam;
import com.tapdata.tm.trace.service.bloodline.BloodlineFinder;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/5/20 13:14 Create
 * @description
 */
@Tag(name = "Data Trace", description = "Data Trace相关接口")
@RestController
@RequestMapping("/api/lineage")
public class TraceController extends BaseController {
    @Resource(name = "bloodlineFinder")
    private BloodlineFinder bloodlineFinder;

    @GetMapping("/wide-table/bloodline-diagram")
    public ResponseMessage<TaskLineageDto> findDataTraceDag(
            @RequestParam(required = true, name = "connectionId") String connectionId,
            @RequestParam(required = true, name = "table") String table,
            @RequestParam(required = false, name = "trackedFields") List<String> trackedFields) {
        final TaskLineageParam param = TaskLineageParam.instance()
                .connectionId(connectionId)
                .table(table)
                .traceFilterFieldNames(trackedFields);
        return success(bloodlineFinder.findTaskLineage(param));
    }
}
