package com.tapdata.tm.metaData.controller;


import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.metaData.service.MetaDataService;
import com.tapdata.tm.metaData.vo.MetaDataVo;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.web.bind.annotation.*;

@Tag(name = "MetaData", description = "元数据")
@Slf4j
@RestController
@RequestMapping("/api/MetaData")
@Setter(onMethod_ = {@Autowired})
public class MetadataController extends BaseController {

    MetaDataService metaDataService;
    MetadataInstancesService metadataInstancesService;

    @GetMapping
    public ResponseMessage<Page<MetaDataVo>> find(@RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(metaDataService.find(filter, getLoginUser()));
    }

    @PatchMapping("/{id}")
    public ResponseMessage<Page<MetaDataVo>> update(@PathVariable("id") String id, @RequestParam("name") String name) {
        Update update = new Update().set("original_name", name);
        metadataInstancesService.updateById(id, update, getLoginUser());
        return success();
    }


    @DeleteMapping("/{id}")
    public ResponseMessage<Page<MetaDataVo>> delete(@PathVariable("id") String id ) {
        metadataInstancesService.deleteLogicsById(id);
        return success();
    }


}
