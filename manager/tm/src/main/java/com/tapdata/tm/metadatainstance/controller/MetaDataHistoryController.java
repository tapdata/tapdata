package com.tapdata.tm.metadatainstance.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.metadatainstance.service.MetaDataHistoryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Date;

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
     * @param filterJson 唯一名称
     * @return
     */
    @GetMapping()
    public ResponseMessage<?> findByVersionTime(@RequestParam("filter") String filterJson) {
        Filter filter = parseFilter(filterJson);
        Where where = filter.getWhere();
        if (where == null) {
            throw new BizException("SystemError");
        }
        String qualifiedName = (String) where.get("qualifiedName");
        long time = (long) where.get("time");

        MetadataInstancesDto metadataInstancesDto = metaDataHistoryService.findByVersionTime(qualifiedName, time);

        return success(metadataInstancesDto);
    }
}
