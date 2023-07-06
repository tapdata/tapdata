package com.tapdata.tm.function.python.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.function.python.dto.PythonFunctionDto;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import com.tapdata.tm.function.python.service.PythonFunctionService;

/**
 * @author GavinXiao
 * @description PythonFunctionController create by Gavin
 * @create 2023/7/4 19:51
 **/
@Tag(name = "PythonFunction", description = "PythonFunction相关接口")
@RestController
@RequestMapping("/api/python-functions")
public class PythonFunctionController extends BaseController {
    @Autowired
    private PythonFunctionService pyFunctionService;

    // filter : {"limit":1000,"where":{"type":"system","category":{"$in":["enhanced","standard"]}}}
    @Operation(summary = "Find all instances of the model matched by filter from the data source")
    @GetMapping
    public ResponseMessage<Page<PythonFunctionDto>> find(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"fields\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        Where where = filter.getWhere();
        if (where != null && "system".equals(where.get("type"))) {
            return success(pyFunctionService.find(filter));
        }
        return success(pyFunctionService.find(filter, getLoginUser()));
    }
}
