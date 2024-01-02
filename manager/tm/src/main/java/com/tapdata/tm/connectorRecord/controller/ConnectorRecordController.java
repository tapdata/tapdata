package com.tapdata.tm.connectorRecord.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.commons.metrics.ConnectorRecordDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.connectorRecord.entity.ConnectorRecordEntity;
import com.tapdata.tm.connectorRecord.service.ConnectorRecordService;
import com.tapdata.tm.ws.dto.MessageInfo;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

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

    @GetMapping()
    public ResponseMessage<ConnectorRecordEntity> getConnectorRecord(@RequestParam String connectionId){
        return success(connectorRecordService.queryByConnectionId(connectionId));
    }
    @PostMapping("/upsertWithWhere")
    public ResponseMessage<ConnectorRecordDto> upsertConnectorRecord(@RequestParam("where") String whereJson,
                                        @RequestBody ConnectorRecordDto connectorRecordDto){
        Where where = parseWhere(whereJson);
        return success(connectorRecordService.upsertByWhere(where,connectorRecordDto,getLoginUser()));
    }

    @PostMapping("/downloadConnector")
    public ResponseMessage<String> downloadConnector(@RequestBody MessageInfo messageInfo){
        connectorRecordService.sendMessage(messageInfo,getLoginUser());
        return success("ok");
    }
    @DeleteMapping("{connectionId}")
    public ResponseMessage<Void> delete(@PathVariable("connectionId") String connectionId) {
        connectorRecordService.deleteByConnectionId(connectionId);
        return success();
    }



}
