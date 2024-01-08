package com.tapdata.tm.connectorRecord.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.commons.metrics.ConnectorRecordDto;
import com.tapdata.tm.commons.metrics.ConnectorRecordFlag;
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
    public ResponseMessage<ConnectorRecordEntity> getConnectorRecord(@RequestParam String processId,@RequestParam String pdkHash){
        UserDetail userDetail = getLoginUser();
        return success(connectorRecordService.queryByConnectionId(processId,pdkHash,userDetail));
    }
    @PostMapping("/upsertWithWhere")
    public ResponseMessage<ConnectorRecordDto> upsertConnectorRecord(@RequestParam("where") String whereJson,
                                        @RequestBody ConnectorRecordDto connectorRecordDto){
        Where where = parseWhere(whereJson);
        return success(connectorRecordService.upsertByWhere(where,connectorRecordDto,getLoginUser()));
    }

    @PostMapping("/downloadConnector")
    public ResponseMessage<String> downloadConnector(@RequestBody MessageInfo messageInfo){
        String processId = connectorRecordService.sendMessage(messageInfo, getLoginUser());
        return success(processId);
    }
    @DeleteMapping("{connectionId}")
    public ResponseMessage<Void> delete(@PathVariable("connectionId") String connectionId) {
        connectorRecordService.deleteByConnectionId(connectionId);
        return success();
    }
    @PostMapping("/saveFlag")
    public ResponseMessage<Void> saveConnectorFlag(@RequestBody ConnectorRecordFlag connectorRecordFlag) {
        UserDetail loginUser = getLoginUser();
        connectorRecordService.saveFlag(connectorRecordFlag,loginUser);
        return success();
    }
    @GetMapping("/getFlag")
    public ResponseMessage<ConnectorRecordFlag> getConnectorFlag(@RequestParam String processId,@RequestParam Long version,@RequestParam String pdkHash){
        UserDetail loginUser = getLoginUser();
        return success(connectorRecordService.queryFlag(processId,pdkHash,version,loginUser));
    }


}
