package com.tapdata.tm.discovery.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.discovery.bean.*;
import com.tapdata.tm.discovery.service.DiscoveryService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;


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
        return success(discoveryService.find(param, getLoginUser()));
    }

    @Operation(summary = "find storage object overview")
    @GetMapping("storage/overview/{id}")
    public ResponseMessage<DiscoveryStorageOverviewDto> storageOverview(@PathVariable("id") String id) {
        return success(discoveryService.storageOverview(id, getLoginUser()));
    }

    @Operation(summary = "find task object overview")
    @GetMapping("task/overview/{id}")
    public ResponseMessage<DiscoveryTaskOverviewDto> taskOverview(@PathVariable("id") String id) {
        return success(discoveryService.taskOverview(id, getLoginUser()));
    }

    @Operation(summary = "find api object overview")
    @GetMapping("api/overview/{id}")
    public ResponseMessage<DiscoveryApiOverviewDto> apiOverview(@PathVariable("id") String id) {
        return success(discoveryService.apiOverview(id, getLoginUser()));
    }

    @Operation(summary = "find storage object preview")
    @GetMapping("storage/preview/{id}")
    public ResponseMessage<Page<Object>> storagePreview(@PathVariable("id") String id, @RequestParam(value = "skip", defaultValue = "0") Integer skip,
    @RequestParam(value = "size", defaultValue = "20") Integer size) {
        return success(discoveryService.storagePreview(id, getLoginUser()));
    }

    @Operation(summary = "find storage filter type list")
    @GetMapping("filterList")
    public ResponseMessage<Map<ObjectFilterEnum, List<String>>> filterList(@RequestParam("filterType") List<ObjectFilterEnum> filterTypes) {
        return success(discoveryService.filterList(filterTypes, getLoginUser()));
    }

    @Operation(summary = "find directory data list")
    @GetMapping("directory/data")
    public ResponseMessage<Page<DataDirectoryDto>> findDirectoryData(DirectoryQueryParam param) {
        return success(discoveryService.findDataDirectory(param, getLoginUser()));
    }


    @Operation(summary = "discovery object update tags")
    @PatchMapping("tags")
    public ResponseMessage<Void> updateListTags(@RequestBody TagBindingReq req) {
        discoveryService.addListTags(req.getTagBindingParams(), req.getTagIds(), req.getOldTagIds(), getLoginUser(), false);
        return success();
    }

    @Operation(summary = "discovery object add tags")
    @PostMapping("tags")
    public ResponseMessage<Void> addListTags(@RequestBody TagBindingReq req) {
        discoveryService.addListTags(req.getTagBindingParams(), req.getTagIds(), req.getOldTagIds(), getLoginUser(), true);
        return success();
    }
}