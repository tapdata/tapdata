package com.tapdata.tm.monitor.controller;

import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.monitor.constant.TableNameEnum;
import com.tapdata.tm.monitor.dto.SampleVo;
import com.tapdata.tm.monitor.dto.StatisticVo;
import com.tapdata.tm.monitor.dto.TransmitTotalVo;
import com.tapdata.tm.monitor.param.AggregateMeasurementParam;
import com.tapdata.tm.monitor.param.MeasurementQueryParam;
import com.tapdata.tm.monitor.service.MeasurementService;
import com.tapdata.tm.monitor.vo.GetMeasurementVo;
import com.tapdata.tm.monitor.vo.GetStaticVo;
import com.tapdata.tm.monitor.vo.QueryMeasurementVo;
import io.tapdata.common.sample.request.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping(value = "/api/measurement")
@Slf4j
public class MeasureController extends BaseController {

    @Autowired
    MeasurementService measurementService;

    @PostMapping("points")
    public ResponseMessage points(@RequestBody BulkRequest bulkRequest) {
        log.info("MeasureController-- {}", JsonUtil.toJson(bulkRequest));
        try {
            List samples = bulkRequest.getSamples();
            List statistics = bulkRequest.getStatistics();
            if (CollectionUtils.isNotEmpty(samples)) {
                measurementService.addAgentMeasurement(samples);
            }
            if (CollectionUtils.isNotEmpty(statistics)) {
                measurementService.addAgentStatistics(statistics);
            }
        } catch (Exception e) {
            log.error("添加秒点异常", e);
            return failed("添加秒点异常");
        }
        return success();
    }


    @Deprecated
    @PostMapping("query")
    public ResponseMessage query(@RequestBody QueryMeasurementParam queryMeasurementParam) {
        QueryMeasurementVo queryMeasurementVo = new QueryMeasurementVo();
        List<StatisticVo> statisticVoList = new ArrayList();
        List sampleVoList = new ArrayList();
        try {

            List<QuerySampleParam> querySampleParamList = queryMeasurementParam.getSamples();
            if (CollectionUtils.isNotEmpty(querySampleParamList)) {
                for (QuerySampleParam querySampleParam : querySampleParamList) {
                    String type = querySampleParam.getType();
                    if ("headAndTail".equals(type)) {
                        sampleVoList.add(measurementService.queryHeadAndTail(querySampleParam).get(0));
                    } else {
                        sampleVoList.add(measurementService.querySample(querySampleParam).get(0));
                    }
                }
            }

            List<QueryStisticsParam> queryStaisticsParamList = queryMeasurementParam.getStatistics();
            if (CollectionUtils.isNotEmpty(queryStaisticsParamList)) {
                statisticVoList = measurementService.queryStatistics(queryMeasurementParam.getStatistics());
            }

            queryMeasurementVo.setSamples(sampleVoList);
            queryMeasurementVo.setStatistics(statisticVoList);
        } catch (Exception e) {
             log.error("查询秒点异常", e);
        }
        return success(queryMeasurementVo);
    }

    /**
     * 查询首页传输总览
     *
     * @param queryMeasurementParam
     * @return
     */
    @Deprecated
    @GetMapping("queryTransmitTotal")
    public ResponseMessage queryTransmitTotal(@RequestBody QueryMeasurementParam queryMeasurementParam) {
        TransmitTotalVo transmitTotalVo = new TransmitTotalVo();
        try {
//            List sampleVoList = measurementService.querySample(queryMeasurementParam.getSamples());
            transmitTotalVo = measurementService.queryTransmitTotal(getLoginUser());
        } catch (Exception e) {
            log.error("查询秒点异常", e);
        }
        return success(transmitTotalVo);
    }

    @PostMapping("points/aggregate")
    public ResponseMessage pointsAggregate(@RequestBody AggregateMeasurementParam aggregateMeasurementParam) {
        try {
            measurementService.aggregateMeasurement(aggregateMeasurementParam);
            return success();
        } catch (Exception e) {
            e.printStackTrace();
            return failed(e);
        }
    }

    @PostMapping("query/v2")
    public ResponseMessage queryV2(@RequestBody MeasurementQueryParam measurementQueryParam) {
        try {
            Object data = measurementService.getSamples(measurementQueryParam);
            return success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return failed(e);
        }
    }
}
