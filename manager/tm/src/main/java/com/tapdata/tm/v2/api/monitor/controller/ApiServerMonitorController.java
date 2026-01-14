package com.tapdata.tm.v2.api.monitor.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.v2.api.monitor.main.dto.ApiDetail;
import com.tapdata.tm.v2.api.monitor.main.dto.ApiItem;
import com.tapdata.tm.v2.api.monitor.main.dto.ApiOfEachServer;
import com.tapdata.tm.v2.api.monitor.main.dto.ApiTopOnHomepage;
import com.tapdata.tm.v2.api.monitor.main.dto.ChartAndDelayOfApi;
import com.tapdata.tm.v2.api.monitor.main.dto.ServerChart;
import com.tapdata.tm.v2.api.monitor.main.dto.ServerItem;
import com.tapdata.tm.v2.api.monitor.main.dto.ServerOverviewDetail;
import com.tapdata.tm.v2.api.monitor.main.dto.ServerTopOnHomepage;
import com.tapdata.tm.v2.api.monitor.main.dto.TopApiInServer;
import com.tapdata.tm.v2.api.monitor.main.dto.TopWorkerInServer;
import com.tapdata.tm.v2.api.monitor.main.param.ApiChart;
import com.tapdata.tm.v2.api.monitor.main.param.ApiDetailParam;
import com.tapdata.tm.v2.api.monitor.main.param.ApiListParam;
import com.tapdata.tm.v2.api.monitor.main.param.ApiWithServerDetail;
import com.tapdata.tm.v2.api.monitor.main.param.QueryBase;
import com.tapdata.tm.v2.api.monitor.main.param.ServerChartParam;
import com.tapdata.tm.v2.api.monitor.main.param.ServerDetail;
import com.tapdata.tm.v2.api.monitor.main.param.ServerListParam;
import com.tapdata.tm.v2.api.monitor.main.param.TopApiInServerParam;
import com.tapdata.tm.v2.api.monitor.main.param.TopWorkerInServerParam;
import com.tapdata.tm.v2.api.monitor.service.ApiMetricsRawQuery;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.Resource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/26 10:15 Create
 * @description
 */
@RequiredArgsConstructor
@RestController
@RequestMapping("/api/monitor")
@Slf4j
@Tag(name = "Api Server Monitor", description = "API Server Monitor related interfaces")
@ApiResponses(value = {@ApiResponse(description = "successful operation", responseCode = "200")})
public class ApiServerMonitorController extends BaseController {

    @Resource(name = "apiMetricsRawQuery")
    ApiMetricsRawQuery apiMetricsRawQuery;

    /**
     * Server Top column on the homepage
     */
    @Operation(summary = "Server Top column on the homepage")
    @GetMapping("/server")
    public ResponseMessage<ServerTopOnHomepage> serverTopOnHomepage(@RequestParam(required = false, name = "startAt") Long startAt,
                                                                    @RequestParam(required = false, name = "endAt") Long endAt) {
        QueryBase param = new QueryBase();
        param.setStartAt(startAt);
        param.setEndAt(endAt);
        return success(apiMetricsRawQuery.serverTopOnHomepage(param));
    }

    /**
     * Server Overview List
     */
    @Operation(summary = "Server Overview List")
    @GetMapping("/server/list")
    public ResponseMessage<List<ServerItem>> serverOverviewList(@RequestParam(required = false) String serverName,
                                                                @RequestParam(required = false) Long startAt,
                                                                @RequestParam(required = false) Long endAt) {
        ServerListParam param = new ServerListParam();
        param.setServerName(serverName);
        param.setStartAt(startAt);
        param.setEndAt(endAt);
        return success(apiMetricsRawQuery.serverOverviewList(param));
    }

    /**
     * Server Overview Detail
     */
    @Operation(summary = "Server Overview Detail")
    @GetMapping("/server/detail")
    public ResponseMessage<ServerOverviewDetail> serverOverviewDetail(@RequestParam(required = true) String serverId,
                                                                      @RequestParam(required = false) Long startAt,
                                                                      @RequestParam(required = false) Long endAt) {
        ServerDetail param = new ServerDetail();
        param.setServerId(serverId);
        param.setStartAt(startAt);
        param.setEndAt(endAt);
        return success(apiMetricsRawQuery.serverOverviewDetail(param));
    }


    /**
     * Server Chart
     */
    @Operation(summary = "Server Chart")
    @GetMapping("/server/chart")
    public ResponseMessage<ServerChart> serverChart(@RequestParam(required = true) String serverId,
                                                    @RequestParam(required = false) Long startAt,
                                                    @RequestParam(required = false) Long endAt) {
        ServerChartParam param = new ServerChartParam();
        param.setServerId(serverId);
        param.setStartAt(startAt);
        param.setEndAt(endAt);
        return success(apiMetricsRawQuery.serverChart(param));
    }


    /**
     * Top api in server
     */
    @Operation(summary = "Top api in server")
    @GetMapping("/server/api")
    public ResponseMessage<List<TopApiInServer>> topApiInServer(@RequestParam(required = true) String serverId,
                                                                @RequestParam(required = false) String orderBy,
                                                                @RequestParam(required = false) Long startAt,
                                                                @RequestParam(required = false) Long endAt) {
        TopApiInServerParam param = new TopApiInServerParam();
        param.setServerId(serverId);
        param.setOrderBy(QueryBase.SortInfo.parse(orderBy));
        param.setStartAt(startAt);
        param.setEndAt(endAt);
        return success(apiMetricsRawQuery.topApiInServer(param));
    }

    /**
     * Top worker in server
     *
     * @param tag CPU: 仅查询cpu分布
     *            ALL: 查询cpu分布 + Worker列表, default
     */
    @Operation(summary = "Top worker in server")
    @GetMapping("/server/worker")
    public ResponseMessage<TopWorkerInServer> topWorkerInServer(@RequestParam(required = true) String serverId,
                                                                @RequestParam(required = false, defaultValue = "ALL") String tag,
                                                                @RequestParam(required = false) Long startAt,
                                                                @RequestParam(required = false) Long endAt) {
        TopWorkerInServerParam param = new TopWorkerInServerParam();
        param.setServerId(serverId);
        param.setStartAt(startAt);
        param.setEndAt(endAt);
        param.setTag(tag);
        return success(apiMetricsRawQuery.topWorkerInServer(param));
    }


    /**
     * Api Top column on the homepage
     */
    @Operation(summary = "Api Top column on the homepage")
    @GetMapping("/api")
    public ResponseMessage<ApiTopOnHomepage> apiTopOnHomepage(@RequestParam(required = false) Long startAt,
                                                              @RequestParam(required = false) Long endAt) {
        QueryBase param = new QueryBase();
        param.setStartAt(startAt);
        param.setEndAt(endAt);
        return success(apiMetricsRawQuery.apiTopOnHomepage(param));
    }

    /**
     * Api Overview List
     */
    @Operation(summary = "Api Overview List")
    @GetMapping("/api/list")
    public ResponseMessage<List<ApiItem>> apiOverviewList(@RequestParam(required = false) String orderBy,
                                                          @RequestParam(required = false) Long startAt,
                                                          @RequestParam(required = false) Long endAt) {
        ApiListParam param = new ApiListParam();
        param.setOrderBy(QueryBase.SortInfo.parse(orderBy));
        param.setStartAt(startAt);
        param.setEndAt(endAt);
        return success(apiMetricsRawQuery.apiOverviewList(param));
    }

    /**
     * Api Overview Detail
     */
    @Operation(summary = "Api Overview Detail")
    @GetMapping("/api/detail")
    public ResponseMessage<ApiDetail> apiOverviewDetail(@RequestParam(required = true) String apiId,
                                                        @RequestParam(required = false) Long startAt,
                                                        @RequestParam(required = false) Long endAt) {
        ApiDetailParam param = new ApiDetailParam();
        param.setApiId(apiId);
        param.setStartAt(startAt);
        param.setEndAt(endAt);
        return success(apiMetricsRawQuery.apiOverviewDetail(param));
    }

    /**
     * api的各 Server 表现分布
     */
    @Operation(summary = "api的各 Server 表现分布")
    @GetMapping("/api/server")
    public ResponseMessage<List<ApiOfEachServer>> apiOfEachServer(@RequestParam(required = true) String apiId,
                                                                  @RequestParam(required = false) Long startAt,
                                                                  @RequestParam(required = false) Long endAt) {
        ApiWithServerDetail param = new ApiWithServerDetail();
        param.setApiId(apiId);
        param.setStartAt(startAt);
        param.setEndAt(endAt);
        return success(apiMetricsRawQuery.apiOfEachServer(param));
    }

    /**
     * 吞吐量与延迟趋势
     */
    @Operation(summary = "吞吐量与延迟趋势")
    @GetMapping("/api/chart")
    public ResponseMessage<ChartAndDelayOfApi> delayOfApi(@RequestParam(required = true) String apiId,
                                                          @RequestParam(required = false) Long startAt,
                                                          @RequestParam(required = false) Long endAt) {
        ApiChart param = new ApiChart();
        param.setApiId(apiId);
        param.setStartAt(startAt);
        param.setEndAt(endAt);
        return success(apiMetricsRawQuery.delayOfApi(param));
    }
}
