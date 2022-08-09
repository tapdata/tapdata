package com.tapdata.tm.discovery.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.discovery.bean.DataDiscoveryDto;
import com.tapdata.tm.discovery.bean.DiscoveryQueryParam;
import com.tapdata.tm.discovery.bean.DiscoveryStorageOverviewDto;
import com.tapdata.tm.discovery.bean.DiscoveryStoragePreviewDto;
import com.tapdata.tm.discovery.service.DiscoveryService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;


/**
 * @Date: 2021/10/19
 * @Description:
 */
@Tag(name = "Data Discovery", description = "数据发现相关接口")
@RestController
@RequestMapping({"/api/discovery"})
@Setter(onMethod_ = {@Autowired})
public class DisCoveryController extends BaseController {

    private DiscoveryService discoveryService;


    @Operation(summary = "find discovery overview list")
    @GetMapping
    public ResponseMessage<Page<DataDiscoveryDto>> find(DiscoveryQueryParam param) {
        return success(discoveryService.find(param));
    }

    @Operation(summary = "find storage object overview")
    @GetMapping("storage/overview/{id}")
    public ResponseMessage<DiscoveryStorageOverviewDto> storageOverview(@PathVariable("id") String id) {
        return success(discoveryService.storageOverview(id));
    }

    @Operation(summary = "find storage object preview")
    @GetMapping("storage/preview/{id}")
    public ResponseMessage<DiscoveryStoragePreviewDto> storagePreview(@PathVariable("id") String id) {
        return success(discoveryService.storagePreview(id));
    }



}