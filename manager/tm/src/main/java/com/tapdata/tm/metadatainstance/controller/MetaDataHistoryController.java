package com.tapdata.tm.metadatainstance.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.metadatainstance.service.MetaDataHistoryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("api/metadata/history")
public class MetaDataHistoryController extends BaseController {

    @Autowired
    private MetaDataHistoryService metaDataHistoryService;
    @PostMapping
    public ResponseMessage<?> saveHistory(@RequestBody MetadataInstancesDto metadataInstancesDto) {
        metaDataHistoryService.saveHistory(metadataInstancesDto);
        return success();
    }

    /**
     *
     * @param qualifiedName 唯一名称
     * @param time 最近时间戳
     * @param order true 所传时间戳之前第一条数据   false 所传时间之后第一条数据
     * @return
     */
    @GetMapping
    public ResponseMessage<?> findByVersionTime(@RequestParam("qualifiedName") String qualifiedName, @RequestParam("time") long time,
                                                @RequestParam(value = "order", defaultValue = "true", required = false) Boolean order) {
        MetadataInstancesDto metadataInstancesDto = metaDataHistoryService.findByVersionTime(qualifiedName, time, order);
        return success(metadataInstancesDto);
    }
}
