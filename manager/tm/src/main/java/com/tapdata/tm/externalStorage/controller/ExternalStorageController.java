package com.tapdata.tm.externalStorage.controller;

import com.tapdata.tm.Permission.service.PermissionService;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.externalStorage.service.ExternalStorageService;
import com.tapdata.tm.permissions.constants.DataPermissionEnumsName;
import com.tapdata.tm.utils.MongoUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * @Date: 2022/09/07
 * @Description:
 */
@Tag(name = "External Storage", description = "External Storage API")
@RestController
@Slf4j
@RequestMapping("/api/ExternalStorage")
public class ExternalStorageController extends BaseController {

	@Autowired
	private ExternalStorageService externalStorageService;
	@Autowired
	private PermissionService permissionService;
	@Autowired
	private SettingsService settingsService;

	/**
	 * Create a new instance of the model and persist it into the data source
	 *
	 * @param externalStorage
	 * @return
	 */
	@Operation(summary = "Create a new instance of the model and persist it into the data source")
	@PostMapping
	public ResponseMessage<ExternalStorageDto> save(@RequestBody(required = false) ExternalStorageDto externalStorage) {
		if (settingsService.isCloud() || Boolean.TRUE.equals(permissionService.checkCurrentUserHasPermission(DataPermissionEnumsName.V2_EXTERNAL_STORAGE_MENU, getLoginUser().getUserId()))) {
			return success(externalStorageService.save(externalStorage, getLoginUser()));
		} else {
			throw new BizException("NotAuthorized");
		}
	}

	/**
	 * Patch an existing model instance or insert a new one into the data source
	 *
	 * @param externalStorage
	 * @return
	 */
	@Operation(summary = "Patch an existing model instance or insert a new one into the data source")
	@PatchMapping()
	public ResponseMessage<ExternalStorageDto> update(@RequestBody ExternalStorageDto externalStorage) {
		if (settingsService.isCloud() || Boolean.TRUE.equals(permissionService.checkCurrentUserHasPermission(DataPermissionEnumsName.V2_EXTERNAL_STORAGE_MENU, getLoginUser().getUserId()))) {
			return success(externalStorageService.update(externalStorage, getLoginUser()));
		} else {
			throw new BizException("NotAuthorized");
		}

	}


	/**
	 * Find all instances of the model matched by filter from the data source
	 *
	 * @param filterJson
	 * @return
	 */
	@Operation(summary = "Find all instances of the model matched by filter from the data source")
	@GetMapping
	public ResponseMessage<Page<ExternalStorageDto>> find(
			@Parameter(in = ParameterIn.QUERY,
					description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"fields\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
			)
			@RequestParam(value = "filter", required = false) String filterJson) {
		Filter filter = parseFilter(filterJson);
		if (filter == null) {
			filter = new Filter();
		}
		return success(externalStorageService.find(filter, getLoginUser()));
	}

	@Operation(summary = "Find all instances of the model matched by filter from the data source")
	@GetMapping("/list")
	public ResponseMessage<Page<ExternalStorageDto>> list(
			@Parameter(in = ParameterIn.QUERY,
					description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"fields\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
			)
			@RequestParam(value = "filter", required = false) String filterJson) {
		Filter filter = parseFilter(filterJson);
		if (filter == null) {
			filter = new Filter();
		}
		Page<ExternalStorageDto> data = externalStorageService.find(filter, getLoginUser());
		return success(data);
	}

	/**
	 * Check whether a model instance exists in the data source
	 *
	 * @return
	 */
	@Operation(summary = "Check whether a model instance exists in the data source")
	@RequestMapping(value = "{id}", method = RequestMethod.HEAD)
	public ResponseMessage<HashMap<String, Boolean>> checkById(@PathVariable("id") String id) {
		long count = externalStorageService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
		HashMap<String, Boolean> existsValue = new HashMap<>();
		existsValue.put("exists", count > 0);
		return success(existsValue);
	}

	/**
	 * Patch attributes for a model instance and persist it into the data source
	 *
	 * @param externalStorage
	 * @return
	 */
	@Operation(summary = "Patch attributes for a model instance and persist it into the data source")
	@PatchMapping("{id}")
	public ResponseMessage<ExternalStorageDto> updateById(@PathVariable("id") String id, @RequestBody ExternalStorageDto externalStorage) {
		if (settingsService.isCloud() || Boolean.TRUE.equals(permissionService.checkCurrentUserHasPermission(DataPermissionEnumsName.V2_EXTERNAL_STORAGE_MENU, getLoginUser().getUserId()))) {
			externalStorage.setId(MongoUtils.toObjectId(id));
			return success(externalStorageService.update(externalStorage, getLoginUser()));
		}else{
			throw new BizException("NotAuthorized");
		}

	}


	/**
	 * Find a model instance by {{id}} from the data source
	 *
	 * @param fieldsJson
	 * @return
	 */
	@Operation(summary = "Find a model instance by {{id}} from the data source")
	@GetMapping("{id}")
	public ResponseMessage<ExternalStorageDto> findById(@PathVariable("id") String id,
														@RequestParam(value = "fields", required = false) String fieldsJson) {
		Field fields = parseField(fieldsJson);
		ExternalStorageDto externalStorageDto = externalStorageService.findById(MongoUtils.toObjectId(id), fields, getLoginUser());
		if (null != externalStorageDto) {
			externalStorageDto.setUri(externalStorageDto.maskUriPassword());
		}
		return success(externalStorageDto);
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
		List<TaskDto> usingTasks = externalStorageService.findUsingTasks(id);
		if (CollectionUtils.isEmpty(usingTasks)) {
			externalStorageService.deleteById(MongoUtils.toObjectId(id), getLoginUser());
			return success();
		} else {
			String taskName = usingTasks.stream().map(TaskDto::getName).collect(Collectors.joining(","));
			throw new BizException("External.Storage.Cannot.Delete", usingTasks.size(), taskName);
		}
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
		long count = externalStorageService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
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
		long count = externalStorageService.count(where, getLoginUser());
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
	public ResponseMessage<ExternalStorageDto> findOne(
			@Parameter(in = ParameterIn.QUERY,
					description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
			)
			@RequestParam(value = "filter", required = false) String filterJson) {
		Filter filter = parseFilter(filterJson);
		if (filter == null) {
			filter = new Filter();
		}
		return success(externalStorageService.findOne(filter, getLoginUser()));
	}

	/**
	 * Update instances of the model matched by {{where}} from the data source.
	 *
	 * @param whereJson
	 * @return
	 */
	@Operation(summary = "Update instances of the model matched by {{where}} from the data source")
	@PostMapping("update")
	public ResponseMessage<Map<String, Long>> updateByWhere(@RequestParam("where") String whereJson, @RequestBody ExternalStorageDto externalStorage) {
		Where where = parseWhere(whereJson);
		long count = externalStorageService.updateByWhere(where, externalStorage, getLoginUser());
		HashMap<String, Long> countValue = new HashMap<>();
		countValue.put("count", count);
		return success(countValue);
	}

	@Operation(summary = "Update instances of the model matched by {{where}} from the data source")
	@PostMapping("set/update")
	public ResponseMessage<Map<String, Long>> updateByWhere(@RequestParam("where") String whereJson, @RequestBody String reqBody) {
		Where where = parseWhere(whereJson);
		UserDetail user = getLoginUser();
		long count;
		if (reqBody.indexOf("\"$set\"") > 0) {
			Document document = JsonUtil.parseJson(reqBody, Document.class);
			if (document.containsKey("$set")) {
				Object o = document.get("$set");
				if (o instanceof Map) {
					document.put("$set", JsonUtil.map2PojoUseJackson((Map<String, Object>) o, Document.class));
				}
				count = externalStorageService.updateByWhere(where, document, user);
				HashMap<String, Long> countValue = new HashMap<>();
				countValue.put("count", count);
				return success(countValue);
			}
		}
		return failed(new BizException(BizException.SYSTEM_ERROR, new IllegalArgumentException("Wrong update body, must start with $set: " + reqBody)));
	}

	/**
	 * Update an existing model instance or insert a new one into the data source based on the where criteria.
	 *
	 * @param whereJson
	 * @return
	 */
	@Operation(summary = "Update an existing model instance or insert a new one into the data source based on the where criteria.")
	@PostMapping("upsertWithWhere")
	public ResponseMessage<ExternalStorageDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody ExternalStorageDto externalStorage) {
		Where where = parseWhere(whereJson);
		return success(externalStorageService.upsertByWhere(where, externalStorage, getLoginUser()));
	}

	@Operation(summary = "Set default external storage")
	@PatchMapping("{id}/default")
	public ResponseMessage<Void> defaultExternalStorage(@PathVariable("id") String id) {
		externalStorageService.update(Query.query(Criteria.where("defaultStorage").is(true)), Update.update("defaultStorage", false), getLoginUser());
		externalStorageService.updateById(id, Update.update("defaultStorage", true), getLoginUser());
		return success();
	}

	@Operation(summary = "Get using tasks")
	@GetMapping("{id}/usingTask")
	public ResponseMessage<List<TaskDto>> getUsingTask(@PathVariable("id") String id) {
		return success(externalStorageService.findUsingTasks(id));
	}
}
