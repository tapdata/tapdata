package com.tapdata.tm.connectorRecord.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.commons.metrics.ConnectorRecordDto;
import com.tapdata.tm.connectorRecord.entity.ConnectorRecordEntity;
import com.tapdata.tm.connectorRecord.service.ConnectorRecordService;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = "ConnectorRecord", description = "ConnectorRecord指标上报接口")
@RestController
@RequestMapping("/api/ConnectorRecord")
@Slf4j
public class ConnectorRecordController extends BaseController {
    @Autowired
    private ConnectorRecordService connectorRecordService;
    @PostMapping()
    public ResponseMessage<ConnectorRecordEntity> uploadConnectorRecord(@RequestBody ConnectorRecordDto connectorRecordDto){
        return success(connectorRecordService.uploadConnectorRecord(connectorRecordDto,getLoginUser()));
    }
}
