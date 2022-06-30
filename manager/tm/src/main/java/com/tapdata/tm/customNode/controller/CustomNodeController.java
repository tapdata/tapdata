package com.tapdata.tm.customNode.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.customNode.dto.CustomNodeDto;
import com.tapdata.tm.customNode.service.CustomNodeService;
import com.tapdata.tm.utils.MongoUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.MediaType;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @Date: 2022/03/09
 * @Description:
 */
@Tag(name = "CustomNode", description = "CustomNode相关接口")
@RestController
@RequestMapping("/api/customNode")
public class CustomNodeController extends BaseController {

  @Autowired
  private CustomNodeService customNodeService;

  /**
   * Create a new instance of the model and persist it into the data source
   *
   * @param logs
   * @return
   */
  @Operation(summary = "Create a new instance of the model and persist it into the data source")
  @PostMapping
  public ResponseMessage<CustomNodeDto> save(@RequestBody CustomNodeDto logs) {
    logs.setId(null);
    return success(customNodeService.save(logs, getLoginUser()));
  }

  /**
   * Patch an existing model instance or insert a new one into the data source
   *
   * @param logs
   * @return
   */
  @Operation(summary = "Patch an existing model instance or insert a new one into the data source")
  @PatchMapping()
  public ResponseMessage<CustomNodeDto> update(@RequestBody CustomNodeDto logs) {
    return success(customNodeService.save(logs, getLoginUser()));
  }


  /**
   * Find all instances of the model matched by filter from the data source
   *
   * @param filterJson
   * @return
   */
  @Operation(summary = "Find all instances of the model matched by filter from the data source")
  @GetMapping
  public ResponseMessage<Page<CustomNodeDto>> find(
    @Parameter(in = ParameterIn.QUERY,
      description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"fields\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
    )
    @RequestParam(value = "filter", required = false) String filterJson) {
    Filter filter = parseFilter(filterJson);
    if (filter == null) {
      filter = new Filter();
    }
    return success(customNodeService.find(filter, getLoginUser()));
  }

  /**
   * Replace an existing model instance or insert a new one into the data source
   *
   * @param logs
   * @return
   */
  @Operation(summary = "Replace an existing model instance or insert a new one into the data source")
  @PutMapping
  public ResponseMessage<CustomNodeDto> put(@RequestBody CustomNodeDto logs) {
    return success(customNodeService.replaceOrInsert(logs, getLoginUser()));
  }


  /**
   * Check whether a model instance exists in the data source
   *
   * @return
   */
  @Operation(summary = "Check whether a model instance exists in the data source")
  @RequestMapping(value = "{id}", method = RequestMethod.HEAD)
  public ResponseMessage<HashMap<String, Boolean>> checkById(@PathVariable("id") String id) {
    long count = customNodeService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
    HashMap<String, Boolean> existsValue = new HashMap<>();
    existsValue.put("exists", count > 0);
    return success(existsValue);
  }

  /**
   * Patch attributes for a model instance and persist it into the data source
   *
   * @param logs
   * @return
   */
  @Operation(summary = "Patch attributes for a model instance and persist it into the data source")
  @PatchMapping("{id}")
  public ResponseMessage<CustomNodeDto> updateById(@PathVariable("id") String id, @RequestBody CustomNodeDto logs) {
    logs.setId(MongoUtils.toObjectId(id));
    return success(customNodeService.save(logs, getLoginUser()));
  }


  /**
   * Find a model instance by {{id}} from the data source
   *
   * @param fieldsJson
   * @return
   */
  @Operation(summary = "Find a model instance by {{id}} from the data source")
  @GetMapping("{id}")
  public ResponseMessage<CustomNodeDto> findById(@PathVariable("id") String id,
                                                 @RequestParam(value = "fields", required = false) String fieldsJson) {
    Field fields = parseField(fieldsJson);
    return success(customNodeService.findById(MongoUtils.toObjectId(id), fields, getLoginUser()));
  }

  /**
   * Replace attributes for a model instance and persist it into the data source.
   *
   * @param logs
   * @return
   */
  @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
  @PutMapping("{id}")
  public ResponseMessage<CustomNodeDto> replceById(@PathVariable("id") String id, @RequestBody CustomNodeDto logs) {
    return success(customNodeService.replaceById(MongoUtils.toObjectId(id), logs, getLoginUser()));
  }

  /**
   * Replace attributes for a model instance and persist it into the data source.
   *
   * @param logs
   * @return
   */
  @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
  @PostMapping("{id}/replace")
  public ResponseMessage<CustomNodeDto> replaceById2(@PathVariable("id") String id, @RequestBody CustomNodeDto logs) {
    return success(customNodeService.replaceById(MongoUtils.toObjectId(id), logs, getLoginUser()));
  }


  /**
   * Delete a model instance by {{id}} from the data source
   *
   * @param id
   * @return
   */
  @Operation(summary = "Delete a model instance by {{id}} from the data source")
  @DeleteMapping("{id}")
  public ResponseMessage<Void> delete(@PathVariable("id") String id) {
    customNodeService.deleteById(MongoUtils.toObjectId(id), getLoginUser());
    return success();
  }

  /**
   * Check whether a model instance exists in the data source
   *
   * @param id
   * @return
   */
  @Operation(summary = "Check whether a model instance exists in the data source")
  @GetMapping("{id}/exists")
  public ResponseMessage<HashMap<String, Boolean>> checkById1(@PathVariable("id") String id) {
    long count = customNodeService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
    HashMap<String, Boolean> existsValue = new HashMap<>();
    existsValue.put("exists", count > 0);
    return success(existsValue);
  }

  /**
   * Count instances of the model matched by where from the data source
   *
   * @param whereJson
   * @return
   */
  @Operation(summary = "Count instances of the model matched by where from the data source")
  @GetMapping("count")
  public ResponseMessage<HashMap<String, Long>> count(@RequestParam("where") String whereJson) {
    Where where = parseWhere(whereJson);
    if (where == null) {
      where = new Where();
    }
    long count = customNodeService.count(where, getLoginUser());
    HashMap<String, Long> countValue = new HashMap<>();
    countValue.put("count", count);
    return success(countValue);
  }

  /**
   * Find first instance of the model matched by filter from the data source.
   *
   * @param filterJson
   * @return
   */
  @Operation(summary = "Find first instance of the model matched by filter from the data source.")
  @GetMapping("findOne")
  public ResponseMessage<CustomNodeDto> findOne(
    @Parameter(in = ParameterIn.QUERY,
      description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
    )
    @RequestParam(value = "filter", required = false) String filterJson) {
    Filter filter = parseFilter(filterJson);
    if (filter == null) {
      filter = new Filter();
    }
    return success(customNodeService.findOne(filter, getLoginUser()));
  }

  /**
   * Update instances of the model matched by {{where}} from the data source.
   *
   * @param whereJson
   * @return
   */
  @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
  @PostMapping("update")
  public ResponseMessage<Map<String, Long>> updateByWhere(@RequestParam("where") String whereJson, @RequestBody CustomNodeDto logs) {
    Where where = parseWhere(whereJson);
    long count = customNodeService.updateByWhere(where, logs, getLoginUser());
    HashMap<String, Long> countValue = new HashMap<>();
    countValue.put("count", count);
    return success(countValue);
  }

  /**
   * Update an existing model instance or insert a new one into the data source based on the where criteria.
   *
   * @param whereJson
   * @return
   */
  @Operation(summary = "Update an existing model instance or insert a new one into the data source based on the where criteria.")
  @PostMapping("upsertWithWhere")
  public ResponseMessage<CustomNodeDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody CustomNodeDto logs) {
    Where where = parseWhere(whereJson);
    return success(customNodeService.upsertByWhere(where, logs, getLoginUser()));
  }

  @PostMapping(path = "/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  public ResponseMessage<String> upload(@RequestParam(value = "image") MultipartFile multipartFile) {
    String upload = customNodeService.upload(multipartFile, getLoginUser());

    return success(upload);
  }
  @GetMapping({"/jar/{id}", "/icon/{id}"})
  public ResponseMessage<Void> downloadJar(@PathVariable("id") String id, HttpServletResponse response) {
    customNodeService.uploadAndView(MongoUtils.toObjectId(id), getLoginUser(), response);
    return success();
  }

  @GetMapping("checkUsed/{id}")
  public ResponseMessage<List<TaskDto>> findTaskById(@PathVariable("id") String id) {
    return success(customNodeService.findTaskById(id, getLoginUser()));
  }
}
