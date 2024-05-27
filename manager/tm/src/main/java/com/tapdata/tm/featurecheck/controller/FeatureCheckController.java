package com.tapdata.tm.featurecheck.controller;


import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.featurecheck.dto.FeatureCheckDto;
import com.tapdata.tm.featurecheck.dto.FeatureCheckResult;
import com.tapdata.tm.featurecheck.service.FeatureCheckService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Tag(name = "featureChecks", description = "featureChecks api")
@RestController
@Slf4j
@RequestMapping("/api/feature/check")
public class FeatureCheckController extends BaseController {

    @Autowired
    private FeatureCheckService featureCheckService;
    @Operation(summary = "check feature min version")
    @PostMapping
    public ResponseMessage<FeatureCheckResult> queryFeatureCheck(@RequestBody List<FeatureCheckDto> featureCheckDtoList) {
        return success(featureCheckService.checkVersionDependencies(featureCheckDtoList, getLoginUser()));
    }
}
