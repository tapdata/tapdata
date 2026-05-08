package com.tapdata.tm.v2.api.schema.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.v2.api.schema.service.ReLoadApiSchemaService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/5/8 09:52 Create
 * @description
 */
@RequiredArgsConstructor
@RestController
@RequestMapping("/api/schema")
@Slf4j
@Tag(name = "Api Server Schema", description = "API Server Schema related interfaces")
@ApiResponses(value = {@ApiResponse(description = "successful operation", responseCode = "200")})
public class ReLoadApiSchemaController extends BaseController {
    @Resource(name = "reLoadApiSchemaService")
    ReLoadApiSchemaService reLoadApiSchemaService;

    /**
     * Server Top column on the homepage
     */
    @Operation(summary = "Reload Api Schema")
    @PostMapping("/reload")
    public void reloadApiSchema(@RequestParam(name = "connectionId") String connectionId,
                                @RequestParam(name = "tableName") String tableName,
                                HttpServletRequest request, HttpServletResponse response) {
        reLoadApiSchemaService.reloadApiSchema(connectionId, tableName, request, response, getLoginUser());
    }
}
