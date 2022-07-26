package com.tapdata.tm.databasetags.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.databasetags.dto.DatabaseTagsDto;
import com.tapdata.tm.databasetags.service.DatabaseTagsService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = "DatabaseTags", description = "DatabaseTags相关接口")
@RestController
@RequestMapping("/api/DatabaseTags")
public class DatabaseTagsController extends BaseController {

    @Autowired
    private DatabaseTagsService databaseTagsService;


    /**
     * Find a list of available Tags
     */
    @Operation(summary = "Find a list of available Tags")
    @GetMapping("/availableTags")
    public ResponseMessage<List<DatabaseTagsDto>> findAvailableTags() {
        return success(databaseTagsService.findAvailableTags());
    }

    @Operation(summary = "DatabaseTags cache evicted")
    @DeleteMapping("/cacheEvict")
    public ResponseMessage<Object> cacheEvict() {
        databaseTagsService.cacheEvict();
        return success();
    }

}