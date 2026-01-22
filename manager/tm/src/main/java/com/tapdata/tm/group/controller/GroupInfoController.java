package com.tapdata.tm.group.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.group.dto.GroupInfoDto;
import com.tapdata.tm.group.dto.GroupInfoRecordDto;
import com.tapdata.tm.group.service.GroupInfoService;
import com.tapdata.tm.group.service.GroupInfoRecordService;
import com.tapdata.tm.commons.task.dto.ImportModeEnum;
import com.tapdata.tm.group.vo.ExportGroupRequest;
import com.tapdata.tm.utils.MongoUtils;
import io.swagger.v3.oas.annotations.Operation;
import jakarta.servlet.http.HttpServletResponse;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.http.MediaType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/groupInfo")
public class GroupInfoController extends BaseController {
    @Autowired
    private GroupInfoService groupInfoService;
    @Autowired
    private GroupInfoRecordService groupInfoRecordService;

    @Operation(summary = "group导出")
    @PostMapping("/batch/load")
    public void batchLoadTasks(@RequestBody ExportGroupRequest exportRequest,
            HttpServletResponse response) {
		groupInfoService.exportGroupInfos(response, exportRequest, getLoginUser());
    }

	@Operation(summary = "group导出")
	@PostMapping("/batch/load/git")
	public ResponseMessage<Map<String, String>> batchLoadTasksGit(@RequestBody ExportGroupRequest exportRequest) {
		groupInfoService.exportGroupInfos(null, exportRequest, getLoginUser());
		return success();
	}

    @Operation(summary = "group异步导入，返回记录ID用于查询进度")
    @PostMapping(path = "/batch/import", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseMessage<Map<String, String>> batchImport(@RequestParam("file") MultipartFile file,
            @RequestParam(value = "importMode", required = false, defaultValue = "group_import") String importMode)
            throws IOException {
        ImportModeEnum importModeEnum = ImportModeEnum.fromValue(importMode);
        ObjectId recordId = groupInfoService.batchImportGroup(file, getLoginUser(), importModeEnum);
        Map<String, String> result = new HashMap<>();
        result.put("recordId", recordId.toHexString());
        return success(result);
    }

    @Operation(summary = "group导入导出记录列表")
    @GetMapping("/record/list")
    public ResponseMessage<Page<GroupInfoRecordDto>> recordList(
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        return success(groupInfoRecordService.find(filter, getLoginUser()));
    }

    @Operation(summary = "group列表")
    @GetMapping("/groupList")
    public ResponseMessage<Page<GroupInfoDto>> groupList(
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        return success(groupInfoService.groupList(filter, getLoginUser()));
    }

    @Operation(summary = "新增group")
    @PostMapping
    public ResponseMessage<GroupInfoDto> save(@RequestBody GroupInfoDto groupInfoDto) {
        groupInfoDto.setId(null);
        return success(groupInfoService.save(groupInfoDto, getLoginUser()));
    }

    /**
     * 修改 modules
     *
     * @return
     */
    @Operation(summary = "修改 发布  group")
    @PatchMapping()
    public ResponseMessage update(@RequestBody GroupInfoDto groupInfoDto) {
        return success(
                groupInfoService.update(Query.query(Criteria.where("_id").is(groupInfoDto.getId())), groupInfoDto));
    }

    /**
     * 逻辑删除
     *
     * @param id
     * @return
     */
    @Operation(summary = "Delete a model instance by {{id}} from the data source")
    @DeleteMapping("{id}")
    public ResponseMessage<Void> delete(@PathVariable("id") String id) {
        groupInfoService.deleteLogicsById(id);
        return success();
    }
    @GetMapping("/getGroupImportStatus/{id}")
    public ResponseMessage<GroupInfoRecordDto> getGroupImportStatusByRecordId(@PathVariable("id") String id) {
        GroupInfoRecordDto groupInfoRecordDto = groupInfoRecordService.findById(MongoUtils.toObjectId(id), getLoginUser());
        if(groupInfoRecordDto == null){
            throw new BizException("GroupInfo.Not.Found");
        }
        return success(groupInfoRecordDto);
    }

	@Operation(summary = "获取最新的tag")
	@GetMapping("/lastestGitTag/{id}")
	public ResponseMessage<String> lastestGitTag(@PathVariable String id) {
		return success(groupInfoService.lastestTagName(id));
	}
}
