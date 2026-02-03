package com.tapdata.tm.modules.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.discovery.bean.DiscoveryFieldDto;
import com.tapdata.tm.metadatadefinition.param.BatchUpdateParam;
import com.tapdata.tm.metadatadefinition.service.MetadataDefinitionService;
import com.tapdata.tm.modules.dto.ModulesPermissionsDto;
import com.tapdata.tm.modules.dto.ModulesTagsDto;
import com.tapdata.tm.module.dto.Param;
import com.tapdata.tm.modules.vo.ModulesDetailVo;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.modules.param.ApiDetailParam;
import com.tapdata.tm.modules.service.ModulesService;
import com.tapdata.tm.group.service.GroupInfoService;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.worker.dto.ApiServerWorkerInfo;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import jakarta.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Collections;


/**
 * @Date: 2021/10/14
 * @Description:
 */
@Tag(name = "Modules", description = "Modules相关接口")
@RestController
@RequestMapping({"/api/Modules","/api/modules"})
public class ModulesController extends BaseController {

  @Autowired
  private ModulesService modulesService;

  @Autowired
  private MetadataDefinitionService metadataDefinitionService;

  @Autowired
  private GroupInfoService groupInfoService;


  @Operation(summary = "新增module")
  @PostMapping
  public ResponseMessage<ModulesDto> save(@RequestBody ModulesDto modulesDto) {
    modulesDto.setId(null);
    return success(modulesService.save(modulesDto, getLoginUser()));
  }


  /**
   * 修改  modules
   *
   * @return
   */
  @Operation(summary = "修改 发布  modules")
  @PatchMapping()
  public ResponseMessage<ModulesDto> update(@RequestBody ModulesDto module) {
    return success(modulesService.updateModuleById(module, getLoginUser()));
  }

  @Operation(summary = "更新权限")
  @PatchMapping("/updatePermissions")
  public ResponseMessage<Void> updatePermissions(@RequestBody ModulesPermissionsDto permissions) {
    modulesService.updatePermissions(permissions, getLoginUser());
    return success();
  }

  @Operation(summary = "更新所属应用")
  @PatchMapping("/updateTags")
  public ResponseMessage<Void> updateTags(@RequestBody ModulesTagsDto modulesTagsDto) {
    modulesService.updateTags(modulesTagsDto, getLoginUser());
    return success();
  }


  /**
   * @author derin
   * @Description   生成 modules
   * @Date 2022/9/1
   */
  @Operation(summary = "生成  modules")
  @PatchMapping("generate")
  public ResponseMessage<ModulesDto> generate(@Validated @RequestBody ModulesDto module) {
    return success(modulesService.generate(module, getLoginUser()));
  }


  /**
   * 批量修改
   *
   * @return
   */
  /**
   *
   */
  @Operation(summary = " 批量修改所属类别")
  @PatchMapping("batchUpdateListtags")
  public ResponseMessage<List<String>> batchUpdateListtags(@RequestBody BatchUpdateParam batchUpdateParam) {
    return success(metadataDefinitionService.batchUpdateListTags("Modules",batchUpdateParam,getLoginUser()));
  }
  @Operation(summary = "批量修改 发布  modules")
  @PatchMapping("batchUpdate")
  public ResponseMessage<List<ModulesDto>> batchUpdate(@RequestBody List<ModulesDto> modules) {
    return success(modulesService.batchUpdateModuleByList(modules, getLoginUser()));
  }


  /**
   * 查询api列表,  分页返回
   *
   * @param filterJson
   * @return
   */
  @GetMapping
  public ResponseMessage find(@RequestParam(value = "filter", required = false) String filterJson) {
    Filter filter = parseFilter(filterJson);
    if (filter == null) {
      filter = new Filter();
    }
    return success(modulesService.findModules(filter, getLoginUser()));
  }



  @GetMapping("{id}")
  public ResponseMessage<ModulesDetailVo> findById(@PathVariable("id") String id) {
    return success(modulesService.findById(id));
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
    modulesService.deleteLogicsById(id);
    groupInfoService.removeResourceReferences(Collections.singletonList(id), getLoginUser());
    return success();
  }


  /**
   * 逻辑删除
   *
   * @param id
   * @return
   */
  @Operation(summary = "Delete a model instance by {{id}} from the data source")
  @PostMapping("{id}/copy")
  public ResponseMessage<Void> copy(@PathVariable("id") String id) {
    modulesService.copy(id, getLoginUser());
    return success();
  }

  @PostMapping("updateOutParameter/{id}")
  public ResponseMessage<Void> updateOutParameter(@PathVariable("id")String id,@RequestBody DiscoveryFieldDto discoveryFieldDto) {
    modulesService.updateOutParameter(id,discoveryFieldDto, getLoginUser());
    return success();
  }

  @PostMapping("updateIntParameter/{id}")
  public ResponseMessage<Void> updateIntParameter(@PathVariable("id")String id,@RequestBody Param param) {
    modulesService.updateIntParameter(id,param, getLoginUser());
    return success();
  }


  @Operation(summary = "Find first instance of the model matched by filter from the data source.")
  @GetMapping("findOne")
  public ResponseMessage<ModulesDto> findOne(@RequestParam(value = "filter", required = false) String filterJson) {
    Filter filter = parseFilter(filterJson);
    if (filter == null) {
      filter = new Filter();
    }
    return success(modulesService.findOne(filter, getLoginUser()));
  }


  @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
  @PostMapping("update")
  public ResponseMessage<Map<String, Long>> updateByWhere(@RequestParam("where") String whereJson, @RequestBody ModulesDto modules) {
    Where where = parseWhere(whereJson);
    long count = modulesService.updateByWhere(where, modules, getLoginUser());
    HashMap<String, Long> countValue = new HashMap<>();
    countValue.put("count", count);
    return success(countValue);
  }


  @Operation(summary = "Update an existing model instance or insert a new one into the data source based on the where criteria.")
  @PostMapping("upsertWithWhere")
  public ResponseMessage<ModulesDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody ModulesDto modules) {
    Where where = parseWhere(whereJson);
    return success(modulesService.upsertByWhere(where, modules, getLoginUser()));
  }

  /**
   * 查询api列表
   *
   * @return
   */
  @GetMapping("getSchema/{id}")
  public ResponseMessage getSchema(@PathVariable("id") String id) {
    return success(modulesService.getSchema(id, getLoginUser()));
  }

  /**
   * 查询api列表
   *
   * @return
   */
  @GetMapping("apiDefinition")
  public ResponseMessage apiDefinition() {
    return success(modulesService.apiDefinition(getLoginUser()));
  }

  @GetMapping("worker-info")
  public ResponseMessage<List<ApiServerWorkerInfo>> getApiWorkerInfo(@RequestParam(value = "workerCount", required = false) Integer workerCount,
                                                                     @RequestParam(value = "processId", required = true) String processId) {
    return success(modulesService.getApiWorkerInfo(processId, workerCount));
  }

  @GetMapping("api/getByCollectionName")
  public ResponseMessage<List<ModulesDto>> getByCollectionName(@RequestParam(value = "connection_id", required = false) String connection_id,
                                                               @RequestParam("collection_name") String collection_name) {
    return success(modulesService.getByCollectionName(connection_id, collection_name, getLoginUser()));
  }


  @GetMapping("getApiDocument/{id}")
  public ResponseMessage getApiDocument(@PathVariable("id") String id){
    return success(modulesService.getApiDocument(MongoUtils.toObjectId(id),getLoginUser()));
  }


  @GetMapping("preview")
  @Deprecated
  public ResponseMessage preview(){
    return success(modulesService.preview(getLoginUser()));
  }

  @GetMapping("rankLists")
  public ResponseMessage rankLists(@RequestParam(value = "filter", required = false) String filterJson){
    Filter filter=parseFilter(filterJson);
//    modulesService.executeRankList(getLoginUser());
    return success(modulesService.rankLists(filter,getLoginUser()));
  }



  @GetMapping("apiList")
  public ResponseMessage apiList(@RequestParam(value = "filter", required = false) String filterJson){
    Filter filter=parseFilter(filterJson);
    return success(modulesService.apiList(filter,getLoginUser()));
  }


  @PostMapping("apiDetail")
  public ResponseMessage apiDetail(@RequestBody ApiDetailParam apiDetailParam){
    return success(modulesService.apiDetail(apiDetailParam));
  }

	@Operation(summary = "api导出")
	@GetMapping("batch/load")
	public void batchLoadTasks(@RequestParam("id") List<String> id, HttpServletResponse response) {
		modulesService.batchLoadTask(response, id, getLoginUser());
	}

	@Operation(summary = "api导入")
	@PostMapping(path = "batch/import", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseMessage<Void> uploadWithMode(@RequestParam(value = "file") MultipartFile file,
                                                @RequestParam(value = "importMode", required = false, defaultValue = "import_as_copy") String importMode) {
      com.tapdata.tm.commons.task.dto.ImportModeEnum mode = com.tapdata.tm.commons.task.dto.ImportModeEnum.fromValue(importMode);
      modulesService.batchUpTask(file, getLoginUser(), mode);
      return success();
    }

  @Operation(summary = "api文档导出")
  @GetMapping("api/export")
  public void batchLoadApis(@RequestParam("id") List<String> id,@RequestParam("ip") String ip, HttpServletResponse response) {
    modulesService.saveWord(response, id,ip, getLoginUser());
  }

}
