//package com.tapdata.tm.duckdb.controller;
//
//import com.tapdata.tm.base.controller.BaseController;
//import com.tapdata.tm.base.dto.ResponseMessage;
//import com.tapdata.tm.commons.util.JsonUtil;
//import com.tapdata.tm.duckdb.dto.DuckDbSqlTestRequest;
//import com.tapdata.tm.duckdb.dto.DuckDbSqlTestResponse;
//import com.tapdata.tm.duckdb.dto.DuckDbTableSchemaDto;
//import com.tapdata.tm.duckdb.dto.DuckDbDeadLetterDto;
//import com.tapdata.tm.proxy.controller.ProxyController;
//import io.swagger.v3.oas.annotations.Operation;
//import io.swagger.v3.oas.annotations.Parameter;
//import io.swagger.v3.oas.annotations.tags.Tag;
//import io.tapdata.pdk.apis.entity.message.ServiceCaller;
//import lombok.extern.slf4j.Slf4j;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.web.bind.annotation.*;
//
//import java.util.List;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.TimeUnit;
//
//@Tag(name = "DuckDbSqlNode", description = "DuckDB SQL 节点管理")
//@Slf4j
//@RestController
//@RequestMapping("/api/duckdb")
//public class DuckDbSqlNodeController extends BaseController {
//
//    @Autowired
//    private ProxyController proxyController;
//
//    @Operation(summary = "测试 DuckDB SQL 语句执行")
//    @PostMapping("/test")
//    public ResponseMessage<DuckDbSqlTestResponse> test(@RequestBody DuckDbSqlTestRequest request) {
//        DuckDbSqlTestResponse response = callRemoteService("DuckDbSqlNodeTestRunService", "testSql", DuckDbSqlTestResponse.class, request);
//        return success(response);
//    }
//
//    @Operation(summary = "获取 DuckDB 表结构信息")
//    @GetMapping("/table-schema")
//    public ResponseMessage<DuckDbTableSchemaDto> tableSchema(
//            @Parameter(description = "表名") @RequestParam String tableName) {
//        DuckDbTableSchemaDto schema = callRemoteService("DuckDbSqlNodeTestRunService", "getTableSchema", DuckDbTableSchemaDto.class, tableName);
//        return success(schema);
//    }
//
//    @Operation(summary = "获取 DuckDB 死信队列记录")
//    @GetMapping("/dead-letter")
//    public ResponseMessage<List<DuckDbDeadLetterDto>> deadLetter(
//            @Parameter(description = "节点 ID") @RequestParam String nodeId,
//            @Parameter(description = "记录数量限制") @RequestParam(defaultValue = "50") int limit) {
//        List<DuckDbDeadLetterDto> deadLetters = callRemoteService("DuckDbSqlNodeTestRunService", "listDeadLetters", List.class, nodeId, limit);
//        return success(deadLetters);
//    }
//
//    private <T> T callRemoteService(String className, String methodName, Class<T> resultClass, Object... args) {
//        CompletableFuture<T> future = new CompletableFuture<>();
//
//        ServiceCaller serviceCaller = new ServiceCaller()
//                .className(className)
//                .method(methodName)
//                .args(args);
//
//        proxyController.executeServiceCaller(serviceCaller, (result, throwable) -> {
//            if (throwable != null) {
//                future.completeExceptionally(throwable);
//            } else {
//                if (resultClass.isInstance(result)) {
//                    future.complete(resultClass.cast(result));
//                } else {
//                    future.complete(JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(result), resultClass));
//                }
//            }
//        });
//
//        try {
//            return future.get(90, TimeUnit.SECONDS);
//        } catch (Exception e) {
//            throw new RuntimeException("Remote service call failed: " + e.getMessage(), e);
//        }
//    }
//}
