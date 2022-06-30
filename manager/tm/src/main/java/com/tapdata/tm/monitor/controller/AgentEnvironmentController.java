package com.tapdata.tm.monitor.controller;

import com.mongodb.bulk.BulkWriteResult;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.monitor.constant.TableNameEnum;
import com.tapdata.tm.monitor.service.MeasurementService;
import com.tapdata.tm.monitor.vo.GetStaticVo;
import io.tapdata.common.sample.request.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping(value = "/api/agentEnvironment")
@Slf4j
public class AgentEnvironmentController extends BaseController {

    @Autowired
    MeasurementService measurementService;

    @PostMapping("add")
    public ResponseMessage add(@RequestBody List<ProcessStaticParam> processStaticParam) {
        BulkWriteResult bulkWriteResult = null;
        try {
            measurementService.addStatic(processStaticParam, TableNameEnum.AgentEnvironment);
        } catch (Exception e) {
            log.error("添加秒点异常", e);
            return failed("添加秒点异常");
        }
        return success();
    }


    @GetMapping("get")
    public ResponseMessage get(@RequestBody ProcessStaticParam getMeasurementParam) {
        List<GetStaticVo> getStaticVoList = null;
        try {
            if (null == getMeasurementParam.getTags()) {
                return failed("tags must not empty");
            }
            getStaticVoList = measurementService.getStatic(getMeasurementParam,TableNameEnum.AgentEnvironment);
        } catch (Exception e) {
            log.error("添加秒点异常", e);
        }
        return success(getStaticVoList);
    }


    /**
     * 添加静态指标
     *
     * @param processStaticParamList
     * @return
     */
  /*  @PostMapping("addStatic")
    public ResponseMessage addStatic(@RequestBody List<ProcessStaticParam> processStaticParamList) {
        BulkWriteResult bulkWriteResult = null;
        try {
            measurementService.addStatic(processStaticParamList);
        } catch (Exception e) {
            log.error("添加秒点异常", e);
            return failed("添加秒点异常");
        }
        return success();
    }*/

    /**
     * 添加静态指标
     *
     * @return
     */
  /*  @GetMapping("getStatic")
    public ResponseMessage getStatic(@RequestBody ProcessStaticParam param) {
        List<GetStaticVo> getStaticVoList = new ArrayList<>();
        try {
            if (null == param.getTags()) {
                return failed("tags must not empty");
            }
            getStaticVoList = measurementService.getStatic(param);
        } catch (Exception e) {
            log.error("获取静态秒点异常", e);
            return failed("获取静态秒点异常");
        }
        return success(getStaticVoList);
    }*/

}
